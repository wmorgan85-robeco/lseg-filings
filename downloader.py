"""Rate-limited, multi-threaded bulk document downloader.

Usage from a Databricks notebook:
    from downloader import BulkDownloader
    dl = BulkDownloader(dbutils)
    failed = dl.download_reports(reports, subfolder="si-universe/reports/")
"""

import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from threading import Lock
from time import monotonic, sleep

import requests

from config import MAX_WORKERS, REQUESTS_PER_SECOND, storage_base_url
from lseg_api import get_access_token, get_retrieval_url


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

    def __init__(self, dbutils):
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
# Bulk downloader
# ---------------------------------------------------------------------------

class BulkDownloader:
    """Downloads LSEG filing PDFs with rate limiting and concurrency."""

    def __init__(self, dbutils, max_workers: int = MAX_WORKERS):
        self.dbutils = dbutils
        self.max_workers = max_workers
        self.rate_limiter = RateLimiter(REQUESTS_PER_SECOND)
        self.token_provider = TokenProvider(dbutils)

    def _download_one(self, doc_id: int) -> bytes:
        """Download a single document by ID. Returns raw PDF bytes."""
        self.rate_limiter.wait()
        token = self.token_provider.get()
        doc_url = get_retrieval_url(token, doc_id)

        headers = {
            "Accept": "application/pdf",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            ),
        }
        response = requests.get(doc_url, headers=headers)
        if response.status_code != 200:
            raise RuntimeError(
                f"Download error {response.status_code} for doc {doc_id}: "
                f"{response.text[:500]}"
            )
        return response.content

    def download_reports(
        self,
        reports: list[dict],
        subfolder: str = "si-universe/reports/",
        storage_client_cls=None,
    ) -> list[int]:
        """Download a list of reports to blob storage.

        Each report dict should contain at minimum:
            document_id, robeco_party, company_name, documenttitle,
            documentdate, filingdate, feedname, submissiontypedescription,
            language_name, originalfilename, midlevelcategoryid

        Returns list of failed document IDs.
        """
        # Lazy import to avoid hard dependency at module level
        if storage_client_cls is None:
            from dblib.api.storage import BlobStorageClient
            storage_client_cls = BlobStorageClient

        storage_loc = storage_base_url(subfolder)
        storage = storage_client_cls(storage_loc, dbutils=self.dbutils)

        failed_docs = []
        n_processed = 0
        started_at = monotonic()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._download_one, report["document_id"]): report
                for report in reports
            }

            for future in as_completed(futures):
                report = futures[future]
                doc_id = report.get("document_id")

                try:
                    doc_content = future.result()
                    filename = f"{doc_id}.pdf"

                    # Encode title safely for metadata
                    doc_title = report.get("documenttitle") or ""
                    doc_title_b64 = (
                        base64.urlsafe_b64encode(doc_title.encode("utf-8")).decode("ascii")
                        if doc_title
                        else None
                    )

                    metadata = {
                        "robeco_party": str(report.get("robeco_party", "")),
                        "company_name": report.get("company_name", ""),
                        "document_id": str(doc_id),
                        "document_date": report["documentdate"].strftime("%Y-%m-%d"),
                        "filing_date": report["filingdate"].strftime("%Y-%m-%d"),
                        "feed_name": report.get("feedname", ""),
                        "submission_type_description": report.get(
                            "submissiontypedescription", ""
                        ),
                        "language_name": report.get("language_name", ""),
                        "document_title": doc_title_b64,
                        "original_file_name": report.get("originalfilename", ""),
                        "mid_level_category_id": str(
                            report.get("midlevelcategoryid", "")
                        ),
                    }

                    storage.store(filename, doc_content, overwrite=True, metadata=metadata)
                    n_processed += 1

                    if n_processed % 50 == 0:
                        elapsed = monotonic() - started_at
                        print(
                            f"Processed {n_processed}/{len(reports)} "
                            f"in {elapsed:.1f}s"
                        )

                except Exception as exc:
                    failed_docs.append(doc_id)
                    print(f"Failed {doc_id}: {type(exc).__name__}: {exc}")

        elapsed = monotonic() - started_at
        print(
            f"Done: {n_processed} downloaded, {len(failed_docs)} failed "
            f"in {elapsed:.1f}s"
        )
        return failed_docs


    def download_by_ids(
        self,
        doc_ids: list[int],
        subfolder: str = "si-universe/reports/",
        storage_client_cls=None,
    ) -> list[int]:
        """Simplified download: just a list of doc IDs, no metadata.

        Use this when you have a file with doc IDs and just want the PDFs.
        Returns list of failed document IDs.
        """
        if storage_client_cls is None:
            from dblib.api.storage import BlobStorageClient
            storage_client_cls = BlobStorageClient

        storage_loc = storage_base_url(subfolder)
        storage = storage_client_cls(storage_loc, dbutils=self.dbutils)

        failed_docs = []
        n_processed = 0
        started_at = monotonic()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._download_one, doc_id): doc_id
                for doc_id in doc_ids
            }

            for future in as_completed(futures):
                doc_id = futures[future]
                try:
                    doc_content = future.result()
                    storage.store(
                        f"{doc_id}.pdf", doc_content, overwrite=True
                    )
                    n_processed += 1

                    if n_processed % 50 == 0:
                        elapsed = monotonic() - started_at
                        print(
                            f"Processed {n_processed}/{len(doc_ids)} "
                            f"in {elapsed:.1f}s"
                        )
                except Exception as exc:
                    failed_docs.append(doc_id)
                    print(f"Failed {doc_id}: {type(exc).__name__}: {exc}")

        elapsed = monotonic() - started_at
        print(
            f"Done: {n_processed} downloaded, {len(failed_docs)} failed "
            f"in {elapsed:.1f}s"
        )
        return failed_docs
