"""Rate-limited, multi-threaded LSEG filing downloader that streams to blob.

PDFs are never held in memory or written to local disk: each document is
streamed from its signed LSEG URL into a block blob in fixed-size chunks, so
memory use is bounded by STREAM_CHUNK_BYTES per worker regardless of PDF size.

Usage:
    from downloader import BulkDownloader, container_client
    cc = container_client("datascientist", dbutils=dbutils)   # dbutils optional
    dl = BulkDownloader(cc, "si-universe/reports/", dbutils=dbutils)
    rows = dl.download(reports, execute=True)
"""

from __future__ import annotations

import base64
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import Lock
from time import monotonic, sleep

import requests
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, ContainerClient, ContentSettings

from config import MAX_WORKERS, REQUESTS_PER_SECOND, STORAGE_ACCOUNT_NAME
from lseg_api import NoPdfError, get_access_token, get_retrieval_url

PDF_CONTENT = ContentSettings(content_type="application/pdf")
STREAM_CHUNK_BYTES = 4 * 1024 * 1024
DOWNLOAD_HEADERS = {
    "Accept": "application/pdf",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    ),
}

# Manifest actions
WOULD_DOWNLOAD = "would_download"   # dry run
COPIED = "copied"                   # streamed into blob
SKIPPED_EXISTS = "skipped_exists"   # target already present, left untouched
NO_PDF = "no_pdf"                   # LSEG has only other formats (txt, xhtml)
FAILED = "failed"


@dataclass
class ManifestRow:
    document_id: int
    target: str
    action: str
    detail: str = ""


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

class RateLimiter:
    """Thread-safe token-bucket style rate limiter."""

    def __init__(self, requests_per_second: float):
        self._interval = 1.0 / requests_per_second
        self._next_request_at = 0.0
        self._lock = Lock()

    def wait(self):
        with self._lock:
            now = monotonic()
            request_at = max(now, self._next_request_at)
            self._next_request_at = request_at + self._interval
        delay = request_at - now
        if delay > 0:
            sleep(delay)


# ---------------------------------------------------------------------------
# Token provider (auto-refresh)
# ---------------------------------------------------------------------------

class TokenProvider:
    """Thread-safe token cache that refreshes before expiry."""

    def __init__(self, dbutils=None):
        self._lock = Lock()
        self._token = None
        self._expires_at = None
        self._dbutils = dbutils

    def get(self) -> str:
        with self._lock:
            now = (
                datetime.now(self._expires_at.tzinfo)
                if self._expires_at and self._expires_at.tzinfo
                else datetime.now()
            )
            if (
                self._token is None
                or now >= self._expires_at - timedelta(seconds=30)
            ):
                self._token, self._expires_at = get_access_token(self._dbutils)
            return self._token


# ---------------------------------------------------------------------------
# Blob helpers
# ---------------------------------------------------------------------------

def container_client(container: str, dbutils=None) -> ContainerClient:
    """Return a client for `container` on the pipeline storage account.

    Uses publisher.get_credential: the service principal in the secrets scope
    on Databricks if present, otherwise DefaultAzureCredential (`az login`).
    """
    from publisher import get_credential

    service = BlobServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
        credential=get_credential(dbutils),
    )
    cc = service.get_container_client(container)
    cc.get_container_properties()  # fail fast on a missing container / no access
    return cc


def report_metadata(report: dict) -> dict | None:
    """Blob metadata for a full-mode report row; None for a bare doc ID."""
    if "documentdate" not in report:
        return None
    doc_title = report.get("documenttitle") or ""
    return {
        "robeco_party": str(report.get("robeco_party") or ""),
        "company_name": report.get("company_name") or "",
        "document_id": str(report["document_id"]),
        "document_date": report["documentdate"].strftime("%Y-%m-%d"),
        "filing_date": report["filingdate"].strftime("%Y-%m-%d"),
        "feed_name": report.get("feedname") or "",
        "submission_type_description": report.get("submissiontypedescription") or "",
        "language_name": report.get("language_name") or "",
        # Titles may contain non-ASCII, which blob metadata does not allow
        "document_title": base64.urlsafe_b64encode(doc_title.encode("utf-8")).decode("ascii"),
        "original_file_name": report.get("originalfilename") or "",
        "mid_level_category_id": str(report.get("midlevelcategoryid") or ""),
    }


# ---------------------------------------------------------------------------
# Bulk downloader
# ---------------------------------------------------------------------------

class BulkDownloader:
    """Streams LSEG filing PDFs into a blob container with rate limiting."""

    def __init__(
        self,
        container: ContainerClient,
        prefix: str,
        dbutils=None,
        max_workers: int = MAX_WORKERS,
    ):
        self.container = container
        self.prefix = prefix.strip("/") + "/" if prefix.strip("/") else ""
        self.max_workers = max_workers
        self.rate_limiter = RateLimiter(REQUESTS_PER_SECOND)
        self.token_provider = TokenProvider(dbutils)

    def _stream_one(self, doc_id: int, blob_name: str, metadata: dict | None) -> tuple[str, str]:
        """Copy one document into `blob_name`. Returns (action, detail)."""
        self.rate_limiter.wait()
        signed_url = get_retrieval_url(self.token_provider.get(), doc_id)
        blob = self.container.get_blob_client(blob_name)

        # Server-side Put Blob From URL is not an option: the LSEG CDN refuses
        # Azure's fetch (CannotVerifyCopySource), so stream through in chunks.
        with requests.get(
            signed_url, headers=DOWNLOAD_HEADERS, stream=True, timeout=(10, 300)
        ) as response:
            if response.status_code != 200:
                raise RuntimeError(
                    f"Download error {response.status_code}: {response.text[:300]}"
                )
            blob.upload_blob(
                response.iter_content(chunk_size=STREAM_CHUNK_BYTES),
                overwrite=False,
                content_settings=PDF_CONTENT,
                metadata=metadata,
                max_concurrency=1,
            )
        return COPIED, f"{blob.get_blob_properties().size:,} bytes"

    def download(
        self,
        reports: list[dict],
        execute: bool = False,
        limit: int | None = None,
    ) -> list[ManifestRow]:
        """Stream reports into the container, skipping ones already there.

        Each report needs a `document_id`; full-mode rows carrying filing
        metadata (see report_metadata) also get it attached as blob metadata.
        Dry run unless `execute`. `limit` caps how many new documents are
        attempted (existing ones are skipped first, so re-runs resume).
        Returns one manifest row per report.
        """
        existing = {
            b.name for b in self.container.list_blobs(name_starts_with=self.prefix)
        }
        print(f"{len(existing)} blob(s) already under {self.container.container_name}/{self.prefix}")

        rows, todo = [], []
        for report in reports:
            doc_id = int(report["document_id"])
            row = ManifestRow(doc_id, f"{self.prefix}{doc_id}.pdf", "")
            rows.append(row)
            if row.target in existing:
                row.action = SKIPPED_EXISTS
            elif limit is not None and len(todo) >= limit:
                row.action, row.detail = "not_attempted", "beyond limit"
            elif not execute:
                row.action = WOULD_DOWNLOAD
                todo.append((row, report))
            else:
                todo.append((row, report))

        if not execute:
            print(f"Dry run: {dict(Counter(r.action for r in rows))}")
            return rows

        print(f"Downloading {len(todo)} document(s) with {self.max_workers} workers")
        started_at = monotonic()
        done = 0
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self._stream_one, row.document_id, row.target, report_metadata(report)
                ): row
                for row, report in todo
            }
            for future in as_completed(futures):
                row = futures[future]
                try:
                    row.action, row.detail = future.result()
                except ResourceExistsError:
                    row.action, row.detail = SKIPPED_EXISTS, "appeared concurrently"
                except NoPdfError as exc:
                    row.action, row.detail = NO_PDF, str(exc)
                except Exception as exc:  # one bad document must not stop the run
                    row.action, row.detail = FAILED, f"{type(exc).__name__}: {exc}"[:500]
                    print(f"  Failed {row.document_id}: {row.detail}")
                done += 1
                if done % 50 == 0:
                    print(f"  {done}/{len(todo)} in {monotonic() - started_at:.0f}s")

        counts = Counter(r.action for r in rows)
        print(f"Done in {monotonic() - started_at:.0f}s: {dict(counts)}")
        return rows
