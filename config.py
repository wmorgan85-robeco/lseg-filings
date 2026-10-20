"""Configuration for the LSEG filings pipeline.

Credentials are read from Databricks secrets. Set them up once:
    databricks secrets put-secret lseg-filings refinitiv-client-id
    databricks secrets put-secret lseg-filings refinitiv-client-secret
    databricks secrets put-secret lseg-filings refinitiv-username
    databricks secrets put-secret lseg-filings refinitiv-password
"""

# ---------------------------------------------------------------------------
# Secrets scope (create via: databricks secrets create-scope lseg-filings)
# ---------------------------------------------------------------------------
SECRETS_SCOPE = "lseg-filings"

# ---------------------------------------------------------------------------
# LSEG / Refinitiv API endpoints
# ---------------------------------------------------------------------------
AUTH_URL = "https://api.refinitiv.com/auth/oauth2/v1/token"
GRAPHQL_URL = "https://api.refinitiv.com/data-store/v1/graphql"
RETRIEVAL_URL = "https://api.refinitiv.com/data/filings/v1/retrieval/search/docId"
RETRIEVAL_API_KEY = "155d9dbf-f0ac-46d9-8b77-f7f6dcd238f8"

# ---------------------------------------------------------------------------
# Azure Blob Storage
# ---------------------------------------------------------------------------
STORAGE_ACCOUNT_NAME = "rbqrrhippoprd"
STORAGE_CONTAINER = "aisearch"
REPORT_SUBFOLDER = "si-universe/reports/"


def storage_base_url(subfolder: str | None = None) -> str:
    """Return the abfss:// base URL for the given subfolder."""
    sf = subfolder or REPORT_SUBFOLDER
    return f"abfss://{STORAGE_CONTAINER}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{sf}"


# ---------------------------------------------------------------------------
# Publishing (see publisher.py)
# The pipeline writes to a working container (aisearch / datascientist).
# Downstream consumers can only read `sistrategist`, so finished PDFs are
# copied there. Same storage account unless --dest-account is given.
# ---------------------------------------------------------------------------
PUBLISH_CONTAINER = "sistrategist"
PUBLISH_SUBFOLDER = "si-universe/reports/"
PUBLISH_ALLOWED_SOURCES = ("aisearch", "datascientist")
PUBLISH_MAX_WORKERS = 8

# Optional service principal for publishing from Databricks, stored in
# SECRETS_SCOPE. Needs Storage Blob Data Reader on the source container and
# Storage Blob Data Contributor on `sistrategist`. If absent, publisher.py
# falls back to DefaultAzureCredential (e.g. `az login` locally).
AZURE_SP_SECRET_KEYS = {
    "tenant_id": "azure-tenant-id",
    "client_id": "azure-client-id",
    "client_secret": "azure-client-secret",
}

# ---------------------------------------------------------------------------
# Input: Doc IDs source file
# ---------------------------------------------------------------------------
DOC_IDS_EXCEL_PATH = (
    "abfss://datascientist@rbqrrhippoprd.dfs.core.windows.net/"
    "users/mw/lseg_filings/LSEG_company_download_list.xlsx"
)

# ---------------------------------------------------------------------------
# Local output (within the project repo, gitignored)
# ---------------------------------------------------------------------------
# Resolved at runtime relative to the notebook; see pipeline.py
LOCAL_OUTPUT_SUBFOLDER = "lseg_filings"

# ---------------------------------------------------------------------------
# Download concurrency
# ---------------------------------------------------------------------------
MAX_WORKERS = 4
REQUESTS_PER_SECOND = 4.0
ROW_BATCH = 2_500
SAVE_EVERY_SECONDS = 600
