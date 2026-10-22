"""LSEG / Refinitiv API client.

Handles authentication, GraphQL metadata queries, and document retrieval.
Credentials are resolved in order:
  1. Environment variables REFINITIV_CLIENT_ID / _CLIENT_SECRET / _USERNAME /
     _PASSWORD, if all four are set.
  2. Databricks secrets scope (config.SECRETS_SCOPE), if running on Databricks
     and the scope exists.
  3. Parsed at runtime from Maria's refinitiv_api notebook in the MPU repo
     (read-only; that notebook is NOT modified). Locally this uses the
     Databricks CLI profile in ~/.databrickscfg.
"""

import base64
import os
import re
import random
import time
from datetime import datetime, timedelta

import requests

from config import (
    AUTH_URL,
    GRAPHQL_URL,
    RETRIEVAL_URL,
    RETRIEVAL_API_KEY,
    SECRETS_SCOPE,
)

# ---------------------------------------------------------------------------
# Credential resolution
# ---------------------------------------------------------------------------

_REFINITIV_API_NB_PATH = (
    "/Repos/rob5910@robeco.nl/MPU/notebooks/Users/"
    "rob5910@robeco.nl/ai/deepresearch/refinitiv_api"
)

_cached_creds: dict | None = None


def _creds_from_env() -> dict | None:
    """Read credentials from REFINITIV_* environment variables, if set.

    The client secret may be absent: the password grant only needs the app key.
    """
    names = ("client_id", "username", "password")
    values = {n: os.environ.get(f"REFINITIV_{n.upper()}") for n in names}
    if not all(values.values()):
        return None
    values["client_secret"] = os.environ.get("REFINITIV_CLIENT_SECRET", "")
    return values


def _creds_from_secrets(dbutils) -> dict | None:
    """Try reading credentials from the Databricks secrets scope.

    Returns None if the scope doesn't exist or any key is missing.
    """
    _keys = [
        ("client_id", "refinitiv-client-id"),
        ("client_secret", "refinitiv-client-secret"),
        ("username", "refinitiv-username"),
        ("password", "refinitiv-password"),
    ]
    try:
        # Check scope exists and has all required keys
        existing = {k.key for k in dbutils.secrets.list(SECRETS_SCOPE)}
        if not all(secret_key in existing for _, secret_key in _keys):
            return None
        return {
            name: dbutils.secrets.get(scope=SECRETS_SCOPE, key=secret_key)
            for name, secret_key in _keys
        }
    except BaseException:
        return None


def _creds_from_notebook() -> dict:
    """Parse credentials from Maria's refinitiv_api notebook (read-only)."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.workspace import ExportFormat

    w = WorkspaceClient()
    nb = w.workspace.export(path=_REFINITIV_API_NB_PATH, format=ExportFormat.SOURCE)
    source = base64.b64decode(nb.content).decode("utf-8")

    creds = {}
    var_to_key = {
        "client_id": "client_id",
        "client_secret": "client_secret",
        "username": "username",
        "password": "password",
    }
    for line in source.splitlines():
        stripped = line.strip()
        for var, key in var_to_key.items():
            m = re.match(rf'^{var}\s*=\s*["\'](.*)["\'](\s*#.*)?$', stripped)
            if m:
                creds[key] = m.group(1)
    if len(creds) != 4:
        raise RuntimeError(
            f"Could not parse all 4 credentials from {_REFINITIV_API_NB_PATH} "
            f"(found {list(creds.keys())})"
        )
    return creds


def _resolve_creds(dbutils=None) -> dict:
    """Return credentials, caching after first successful resolution."""
    global _cached_creds
    if _cached_creds is not None:
        return _cached_creds

    creds = _creds_from_env()
    if creds is None and dbutils is not None:
        creds = _creds_from_secrets(dbutils)
    if creds is None:
        # Fall back to reading Maria's notebook
        creds = _creds_from_notebook()
    _cached_creds = creds
    return creds


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def get_access_token(dbutils=None) -> tuple[str, datetime]:
    """Obtain an OAuth2 access token via the Refinitiv password grant.

    Returns (access_token, expires_at).
    """
    creds = _resolve_creds(dbutils)
    client_id = creds["client_id"]
    client_secret = creds["client_secret"]
    username = creds["username"]
    password = creds["password"]

    form_data = {
        "username": username,
        "password": password,
        "grant_type": "password",
        "scope": "trapi",
        "takeExclusiveSignOnControl": "true",
    }

    auth_value = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_value}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    response = requests.post(AUTH_URL, data=form_data, headers=headers)
    if response.status_code != 200:
        raise RuntimeError(f"Auth error {response.status_code}: {response.text[:500]}")

    token_info = response.json()
    token = token_info["access_token"]
    expires_at = datetime.now() + timedelta(seconds=int(token_info["expires_in"]) - 5)
    return token, expires_at


# ---------------------------------------------------------------------------
# Retry-aware HTTP helper
# ---------------------------------------------------------------------------

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def request_json_retry(
    method: str,
    url: str,
    *,
    headers: dict,
    max_attempts: int = 4,
    timeout: tuple = (10, 60),
    error_prefix: str = "API",
    **request_kwargs,
):
    """Make an HTTP request and return decoded JSON, with exponential backoff."""
    for attempt in range(max_attempts):
        try:
            response = requests.request(
                method, url, headers=headers, timeout=timeout, **request_kwargs
            )
        except (requests.Timeout, requests.ConnectionError):
            if attempt == max_attempts - 1:
                raise
            time.sleep(min(2**attempt + random.random(), 30))
            continue

        if response.status_code in RETRYABLE_STATUS_CODES:
            if attempt == max_attempts - 1:
                raise RuntimeError(
                    f"{error_prefix} error {response.status_code}: {response.text[:1000]}"
                )
            retry_after = response.headers.get("Retry-After")
            try:
                delay = float(retry_after)
            except (TypeError, ValueError):
                delay = min(2**attempt + random.random(), 30)
            time.sleep(delay)
            continue

        if response.status_code != 200:
            raise RuntimeError(
                f"{error_prefix} error {response.status_code}: {response.text[:1000]}"
            )

        try:
            return response.json()
        except requests.exceptions.JSONDecodeError as exc:
            raise RuntimeError(f"{error_prefix} returned invalid JSON") from exc

    raise RuntimeError(f"{error_prefix} request exhausted all retry attempts")


# ---------------------------------------------------------------------------
# GraphQL metadata queries
# ---------------------------------------------------------------------------

def _filing_query(
    organization_id: int,
    from_date,
    to_date,
    mid_level_category_id: int = 1,
    extra_filters: str = "",
) -> str:
    """Build a FinancialFiling GraphQL query."""
    start = from_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    end = to_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    return (
        '{\n'
        '    FinancialFiling(\n'
        '        filter:{\n'
        '        AND: [\n'
        f'            {{ FilingDocument: {{ Identifiers: {{ OrganizationId: {{ EQ: "{organization_id}" }} }} }} }},\n'
        f'            {{ FilingDocument: {{ DocumentSummary: {{ FilingDate: {{ BETWN: {{ FROM: "{start}", TO: "{end}" }} }} }} }} }},\n'
        '            { FilingDocument: { DocumentSummary: { HighLevelCategoryId: { EQ: "1" } } } },\n'
        f'            {{ FilingDocument: {{ DocumentSummary: {{ MidLevelCategoryId: {{ EQ: "{mid_level_category_id}" }} }} }} }}\n'
        f'            {extra_filters}\n'
        '        ]\n'
        '        },\n'
        '        sort: { FilingDocument: { DocumentSummary: { FilingDate: DESC } } },\n'
        '        limit: 200\n'
        '    ) {\n'
        '        FilingDocument {\n'
        '        DocId\n'
        '        DocumentSummary {\n'
        '            DocumentDate\n'
        '            DocumentTitle\n'
        '            FeedName\n'
        '            FilingDate\n'
        '            LanguageId\n'
        '            MidLevelCategoryId\n'
        '            OriginalFilename\n'
        '            PageCount\n'
        '            SizeInBytes\n'
        '            SubmissionTypeDescription{\n'
        '              Value\n'
        '            }\n'
        '        }\n'
        '        FilesMetaData{\n'
        '          MimeType\n'
        '        }\n'
        '      }\n'
        '    }\n'
        '}'
    )


def periodic_annual_reports_query(organization_id: int, from_date, to_date) -> str:
    """GraphQL query for annual reports (MidLevelCategoryId = 1)."""
    return _filing_query(organization_id, from_date, to_date, mid_level_category_id=1)


def periodic_esg_reports_query(organization_id: int, from_date, to_date) -> str:
    """GraphQL query for ESG disclosure reports (MidLevelCategoryId = 26)."""
    return _filing_query(
        organization_id,
        from_date,
        to_date,
        mid_level_category_id=26,
        extra_filters=',{ FilingDocument: { DocumentSummary: { SubmissionTypeDescription: { Value: { EQ: "ESG" } } } } }',
    )


QUERY_BUILDERS = {
    1: periodic_annual_reports_query,
    26: periodic_esg_reports_query,
}


def fetch_reports_info(access_token: str, query: str) -> dict:
    """Post a GraphQL query to the LSEG data-store and return the payload."""
    payload = request_json_retry(
        "POST",
        GRAPHQL_URL,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        json={"query": query},
        error_prefix="GraphQL API",
    )
    if not isinstance(payload, dict):
        raise RuntimeError("GraphQL API returned an invalid response")
    if payload.get("errors"):
        raise RuntimeError(f"GraphQL errors: {payload['errors']}")
    return payload


# ---------------------------------------------------------------------------
# Document retrieval
# ---------------------------------------------------------------------------

class NoPdfError(RuntimeError):
    """The document exists but LSEG offers no PDF rendition of it."""


def get_retrieval_url(access_token: str, doc_id: int, max_attempts: int = 4) -> str:
    """Resolve a document ID to a signed PDF download URL."""
    payload = request_json_retry(
        "GET",
        f"{RETRIEVAL_URL}/{doc_id}",
        headers={
            "Authorization": f"Bearer {access_token}",
            "ClientID": "API_Playground",
            "X-Api-Key": RETRIEVAL_API_KEY,
            "Accept": "*/*",
        },
        max_attempts=max_attempts,
        error_prefix="Retrieval API",
    )
    if not isinstance(payload, dict):
        raise RuntimeError("Retrieval API returned an invalid response")

    for document in payload.values():
        if not isinstance(document, dict):
            continue
        if document.get("mimeType") == "application/pdf":
            signed_url = document.get("signedUrl")
            if signed_url:
                return signed_url

    available = sorted(
        {d.get("mimeType") for d in payload.values() if isinstance(d, dict)} - {None}
    )
    raise NoPdfError(f"no PDF for document {doc_id}; available: {', '.join(available) or 'none'}")


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

_SUMMARY_FIELDS = [
    "DocumentDate",
    "DocumentTitle",
    "FeedName",
    "FilingDate",
    "LanguageId",
    "MidLevelCategoryId",
    "OriginalFilename",
    "PageCount",
    "SizeInBytes",
    ["SubmissionTypeDescription", "Value"],
]

_DATE_FIELDS = {"DocumentDate", "FilingDate"}


def extract_rows(organization_id: int, response_data: dict) -> list[dict]:
    """Parse the GraphQL response into a flat list of row dicts."""
    data = response_data.get("data")
    if not isinstance(data, dict):
        raise RuntimeError("Invalid API response: missing data object")

    filings = data.get("FinancialFiling")
    if not isinstance(filings, list):
        raise RuntimeError("Invalid API response: missing FinancialFiling list")

    result = []
    for filing in filings:
        document = filing.get("FilingDocument") or {}
        doc_id_str = document.get("DocId")
        if doc_id_str is None:
            continue

        metadata = document.get("FilesMetaData") or []
        mimes = sorted(item["MimeType"] for item in metadata if item.get("MimeType"))
        mime = next(
            (m for m in ("application/pdf", "text/plain") if m in mimes),
            str(mimes),
        )

        summary = document.get("DocumentSummary") or {}
        row = {
            "document_id": int(doc_id_str),
            "organization_id": organization_id,
            "mime_type": mime,
        }

        for field in _SUMMARY_FIELDS:
            if isinstance(field, str):
                key = field
                value = summary.get(field)
            else:
                key = field[0]
                value = summary
                for nested_field in field:
                    if not isinstance(value, dict):
                        value = None
                        break
                    value = value.get(nested_field)

            if key in _DATE_FIELDS and value:
                value = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").date()

            row[key.lower()] = value

        result.append(row)

    return result
