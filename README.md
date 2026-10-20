# LSEG Filings Pipeline

Bulk download LSEG/Refinitiv financial filings (annual reports, ESG disclosures) as PDFs.

Adapted from Maria Putintseva's `download_filings` notebook in the MPU repo.

## Project Structure

```
lseg-filings/
├── README.md          # This file
├── config.py          # Environment config (secrets, storage, API endpoints)
├── lseg_api.py        # LSEG/Refinitiv API client (auth, GraphQL, retrieval)
├── downloader.py      # Rate-limited, multi-threaded bulk PDF downloader
├── ranking.py         # Document ranking and deduplication logic
├── publisher.py       # Copy PDFs to the consumer-facing `sistrategist` container
└── pipeline.py        # Main orchestration notebook (Databricks)
```

## Setup

### 1. Configure Secrets

Create a Databricks secrets scope and add your Refinitiv credentials:

```bash
databricks secrets create-scope lseg-filings
databricks secrets put-secret lseg-filings refinitiv-client-id
databricks secrets put-secret lseg-filings refinitiv-client-secret
databricks secrets put-secret lseg-filings refinitiv-username
databricks secrets put-secret lseg-filings refinitiv-password
```

### 2. Dependencies

The pipeline uses:
- **PySpark** (included in Databricks Runtime)
- **requests** (included in Databricks Runtime)
- **dblib.api.storage.BlobStorageClient** (Robeco internal library)

## Usage

### Simple Mode (doc IDs → download)

If you already have a list of LSEG document IDs:

1. Open `pipeline.py` in Databricks
2. Set `mode = simple`
3. Set `doc_ids_path` to your CSV file (expects a `document_id` column)
4. Run all cells

### Full Mode (universe → metadata → rank → download)

For the complete pipeline starting from a Robeco universe file:

1. Open `pipeline.py` in Databricks
2. Set `mode = full`
3. Set `universe_csv_path` to your CSV (expects `Robeco Party ID` and `company_name` columns)
4. Run all cells

## Publishing to `sistrategist`

The pipeline writes to a working container (`aisearch`, or `datascientist`).
Downstream consumers can only read `sistrategist`, so finished PDFs are
copied there by `publisher.py`, preserving sub-folders and blob metadata.

**In Databricks:** set the `publish` widget in `pipeline` to `dry_run`
(default), check the manifest, then `execute` (optionally with
`publish_limit` for a trial). The manifest is saved to
`aisearch/_publish_manifests/`.

**Locally** (uses `az login`; dependencies declared inline for `uv`):

```powershell
az login
uv run publisher.py                                        # 1. dry run
uv run publisher.py --execute --limit 5                    # 2. small trial
uv run publisher.py --execute --dest-prefix si-universe-oct-2026/   # 3. full copy
# --source-container datascientist   to publish from datascientist instead
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
- Downloads are stored to Azure Blob Storage at `abfss://aisearch@rbqrrhippoprd.dfs.core.windows.net/`, then published to `sistrategist` for consumers
- The ranking logic prioritises English documents, larger page counts, and more recent filings
