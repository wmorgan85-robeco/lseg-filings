# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "requests>=2.31",
#     "azure-storage-blob>=12.19",
#     "azure-identity>=1.15",
#     "databricks-sdk>=0.30",
#     "pandas>=2.0",
#     "openpyxl>=3.1",
# ]
# ///
"""Run the simple-mode pipeline (doc IDs -> PDFs in blob) from a laptop.

PDFs stream from LSEG straight into blob storage; nothing is written locally
except the CSV manifest. Uses `az login` for storage and, unless REFINITIV_*
env vars are set, the Databricks CLI profile to read the Refinitiv
credentials from Maria's refinitiv_api notebook.

    az login
    uv run run_local.py                          # 1. dry run
    uv run run_local.py --execute --limit 5      # 2. small trial
    uv run run_local.py --execute                # 3. everything (re-runs resume)
"""

from __future__ import annotations

import argparse
import csv
import io
import re
import sys
from collections import Counter
from dataclasses import asdict, fields
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import DOC_IDS_EXCEL_PATH, REPORT_SUBFOLDER, STORAGE_CONTAINER
from downloader import BulkDownloader, FAILED, ManifestRow, container_client

ABFSS = re.compile(r"^abfss://(?P<container>[^@]+)@[^/]+/(?P<path>.+)$")


def load_doc_ids(source: str) -> list[int]:
    """Read doc IDs from an abfss:// blob path or a local CSV/Excel file."""
    match = ABFSS.match(source)
    if match:
        cc = container_client(match["container"])
        data = io.BytesIO(cc.download_blob(match["path"]).readall())
    else:
        data = source
    name = match["path"] if match else source
    df = pd.read_excel(data) if name.endswith((".xlsx", ".xls")) else pd.read_csv(data)

    # Same column detection as the notebook
    candidates = [c for c in df.columns if "doc" in str(c).lower() and "id" in str(c).lower()]
    column = candidates[0] if candidates else next(
        (c for c in df.columns if pd.api.types.is_integer_dtype(df[c])), df.columns[0]
    )
    ids = list(dict.fromkeys(df[column].dropna().astype(int)))  # dedupe, keep order
    print(f"{len(df)} rows in {name}; using column '{column}' -> {len(ids)} unique doc IDs")
    return ids


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--doc-ids", default=DOC_IDS_EXCEL_PATH,
                        help="abfss:// path or local CSV/Excel with a doc ID column")
    parser.add_argument("--container", default=STORAGE_CONTAINER)
    parser.add_argument("--prefix", default=REPORT_SUBFOLDER)
    parser.add_argument("--execute", action="store_true", help="actually download (default: dry run)")
    parser.add_argument("--limit", type=int, help="max new documents to attempt")
    args = parser.parse_args()

    doc_ids = load_doc_ids(args.doc_ids)
    downloader = BulkDownloader(container_client(args.container), args.prefix)
    rows = downloader.download(
        [{"document_id": d} for d in doc_ids], execute=args.execute, limit=args.limit
    )

    manifest = Path("manifests") / f"download_{datetime.now():%Y%m%d_%H%M%S}.csv"
    manifest.parent.mkdir(exist_ok=True)
    with manifest.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=[f.name for f in fields(ManifestRow)])
        writer.writeheader()
        writer.writerows(asdict(r) for r in rows)

    print(f"{dict(Counter(r.action for r in rows))}\nmanifest: {manifest}")
    return 1 if any(r.action == FAILED for r in rows) else 0


if __name__ == "__main__":
    sys.exit(main())
