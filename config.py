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
# Download concurrency
# ---------------------------------------------------------------------------
MAX_WORKERS = 4
REQUESTS_PER_SECOND = 4.0
ROW_BATCH = 2_500
SAVE_EVERY_SECONDS = 600
