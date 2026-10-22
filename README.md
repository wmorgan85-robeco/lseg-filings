# LSEG Filings Pipeline

Bulk download LSEG/Refinitiv financial filings (annual reports, ESG disclosures) as PDFs.

Adapted from Maria Putintseva's `download_filings` notebook in the MPU repo.

## Project Structure

```
lseg-filings/
├── README.md          # This file
├── config.py          # Environment config (secrets, storage, API endpoints)
├── lseg_api.py        # LSEG/Refinitiv API client (auth, GraphQL, retrieval)
├── downloader.py      # Rate-limited, multi-threaded downloader that streams PDFs into blob
├── ranking.py         # Document ranking and deduplication logic
├── publisher.py       # Copy PDFs to the consumer-facing `sistrategist` container
├── run_local.py       # Simple mode from a laptop (uv script)
├── .env.op            # 1Password references for the Refinitiv credentials (no secrets)
└── pipeline.ipynb     # Main orchestration notebook (Databricks)
```

PDFs stream from LSEG straight into blob storage in 4 MB chunks. They are
never held whole in memory or written to disk, so memory stays flat however
large a filing is. Never write filings into the repo folder: `*.pdf` is
git-ignored.

## Credentials

Resolved in order (see `lseg_api.py`):

1. `REFINITIV_CLIENT_ID` / `_USERNAME` / `_PASSWORD` (and optional
   `_CLIENT_SECRET`) environment variables. Locally these come from the
   1Password item `Private/refinitiv-api` via `op run --env-file .env.op`.
2. The `lseg-filings` Databricks secrets scope, on Databricks.
3. Parsed at runtime from Maria's `refinitiv_api` notebook (read-only).

Storage uses `az login` locally, or the service principal in the secrets scope
on Databricks (falling back to `DefaultAzureCredential`).

## Usage

### Locally (recommended)

```powershell
az login
op run --env-file .env.op -- uv run run_local.py                      # 1. dry run
op run --env-file .env.op -- uv run run_local.py --execute --limit 25 # 2. trial
op run --env-file .env.op -- uv run run_local.py --execute            # 3. everything
```

Defaults: doc IDs from `DOC_IDS_EXCEL_PATH`, target
`datascientist/users/mw/lseg_filings/reports/`. Existing blobs are skipped and never
overwritten, so re-runs resume. Each run writes `manifests/download_*.csv` with
one row per document: `copied`, `skipped_exists`, `no_pdf` (LSEG only has
txt/xhtml for it), `failed` (e.g. 404 from LSEG) or `not_attempted`.

### Simple Mode in Databricks (doc IDs → download)

If you already have a list of LSEG document IDs:

1. Open `pipeline` in Databricks
2. Set `mode = simple`
3. Set `doc_ids_path` to your CSV file (expects a `document_id` column)
4. Run with `download = dry_run`, then `execute`

### Full Mode (universe → metadata → rank → download)

For the complete pipeline starting from a Robeco universe file:

1. Open `pipeline` in Databricks
2. Set `mode = full`
3. Set `universe_csv_path` to your CSV (expects `Robeco Party ID` and `company_name` columns)
4. Run with `download = dry_run`, then `execute`

## Publishing to `sistrategist`

The pipeline writes to a working container (`datascientist` by default, or `aisearch`).
Downstream consumers can only read `sistrategist`, so finished PDFs are
copied there by `publisher.py`, preserving sub-folders and blob metadata.

**In Databricks:** set the `publish` widget in `pipeline` to `dry_run`
(default), check the manifest, then `execute` (optionally with
`publish_limit` for a trial). The manifest is saved to
`datascientist/_publish_manifests/`.

**Locally** (uses `az login`; dependencies declared inline for `uv`):

```powershell
az login
uv run publisher.py                                        # 1. dry run
uv run publisher.py --execute --limit 5                    # 2. small trial
uv run publisher.py --execute --dest-prefix si-universe-oct-2026/   # 3. full copy
# --source-container aisearch   to publish from aisearch instead
```

Manifests are written to `manifests/` (git-ignored).

| Concern | Handling |
| --- | --- |
| Accidental run | Dry run is the default; `execute` is required to write |
| Source data | Read-only. No delete, move, rename or overwrite against the source |
| Overwriting targets | `overwrite=False`; existing targets are skipped. Size mismatches are reported as `skipped_exists_differs`, never replaced |
| Container creation | Not done; a missing `sistrategist` is an error |
| Partial failures | Logged per blob; the run continues and re-runs resume |
| Auditability | CSV manifest per run (source, target, size, action, detail) |

The copy is server-side (Put Blob From URL, source authorised with a bearer
token), with a per-blob download-then-upload fallback that is flagged
`copied_stream`.

**Permissions:** `Storage Blob Data Reader` on the source container and
`Storage Blob Data Contributor` on `sistrategist`. On Databricks, put a
service principal with those roles in the `lseg-filings` scope
(`azure-tenant-id`, `azure-client-id`, `azure-client-secret`); otherwise
`DefaultAzureCredential` is used.

## API Reference

| Endpoint | Purpose |
| --- | --- |
| `api.refinitiv.com/auth/oauth2/v1/token` | OAuth2 token |
| `api.refinitiv.com/data-store/v1/graphql` | Filing metadata (GraphQL) |
| `api.refinitiv.com/data/filings/v1/retrieval/search/docId/{id}` | Document download URL |

## Filing Categories

- **MidLevelCategoryId = 1**: Annual reports (10-K, 20-F, etc.)
- **MidLevelCategoryId = 26**: ESG disclosure reports

## Notes

- Rate limited to 4 requests/second with 4 concurrent workers
- Token auto-refreshes before expiry
- Downloads are stored to Azure Blob Storage at `abfss://datascientist@rbqrrhippoprd.dfs.core.windows.net/users/mw/lseg_filings/reports/`, then published to `sistrategist` for consumers
- The ranking logic prioritises English documents, larger page counts, and more recent filings
