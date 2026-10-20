# /// script
# requires-python = ">=3.10"
# dependencies = ["azure-storage-blob>=12.19", "azure-identity>=1.15"]
# ///
"""Publish downloaded PDFs to the consumer-facing container.

The pipeline writes PDFs to a working container we can access (``aisearch``
or ``datascientist``). Downstream consumers can only read ``sistrategist``,
so this module copies finished PDFs across, preserving sub-folders and blob
metadata (company, document date, category, ...).

Safety properties (same as the original ai-search-doc-copy script):

* Dry run by default; ``execute=True`` / ``--execute`` is required to write.
* Source is read-only: no delete, move, rename or overwrite against it.
* Targets are never overwritten (``overwrite=False``); existing targets are
  skipped and reported, including when their size differs from the source.
* The destination container is never created; a missing one is an error.
* One blob's failure is recorded and the run continues; re-runs resume.
* Every run returns a manifest (source, target, size, action, detail).

The copy is server-side (Put Blob From URL), so bytes do not pass through the
caller. If the service refuses it for a blob, that blob falls back to a
download-then-upload stream (equally non-destructive) and is flagged.

Local usage (uses ``az login`` credentials)::

    uv run publisher.py                                  # dry run
    uv run publisher.py --execute --limit 5              # small trial
    uv run publisher.py --execute --dest-prefix si-universe-oct-2026/

Databricks usage: see the "Publish" cells in the ``pipeline`` notebook.
"""

from __future__ import annotations

import argparse
import csv
import io
import os
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
from time import monotonic

from azure.core.exceptions import (
    HttpResponseError,
    ResourceExistsError,
    ResourceNotFoundError,
)
from azure.storage.blob import BlobServiceClient, ContentSettings

from config import (
    AZURE_SP_SECRET_KEYS,
    PUBLISH_ALLOWED_SOURCES,
    PUBLISH_CONTAINER,
    PUBLISH_MAX_WORKERS,
    PUBLISH_SUBFOLDER,
    REPORT_SUBFOLDER,
    SECRETS_SCOPE,
    STORAGE_ACCOUNT_NAME,
    STORAGE_CONTAINER,
)

STORAGE_SCOPE = "https://storage.azure.com/.default"

# Manifest actions
WOULD_COPY = "would_copy"                  # dry run
COPIED = "copied"                          # server-side copy
COPIED_STREAM = "copied_stream"            # fallback: download + upload
SKIPPED_EXISTS = "skipped_exists"          # target exists, same size
SKIPPED_DIFFERS = "skipped_exists_differs" # target exists, different size
NOT_ATTEMPTED = "not_attempted_limit"      # beyond --limit
FAILED = "failed"


@dataclass
class ManifestRow:
    source: str
    target: str
    size: int
    action: str
    detail: str = ""


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

def get_credential(dbutils=None):
    """Return an Azure credential.

    On Databricks (``dbutils`` given), use the service principal stored in the
    ``SECRETS_SCOPE`` secrets scope if present. Otherwise fall back to
    ``DefaultAzureCredential`` (``az login`` locally, managed identity, env).
    """
    from azure.identity import ClientSecretCredential, DefaultAzureCredential

    if dbutils is not None:
        try:
            values = {
                arg: dbutils.secrets.get(SECRETS_SCOPE, key)
                for arg, key in AZURE_SP_SECRET_KEYS.items()
            }
            return ClientSecretCredential(**values)
        except Exception as exc:  # secret missing / scope missing
            print(
                f"Service principal secrets not found in scope '{SECRETS_SCOPE}' "
                f"({type(exc).__name__}); falling back to DefaultAzureCredential"
            )
    return DefaultAzureCredential(exclude_interactive_browser_credential=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _account_url(account: str) -> str:
    return f"https://{account}.blob.core.windows.net"


def _norm_prefix(prefix: str | None) -> str:
    prefix = (prefix or "").strip("/")
    return f"{prefix}/" if prefix else ""


def _plan(src_cc, dst_cc, src_prefix, dst_prefix, extensions):
    """List source blobs and pair each with its target and existing target size."""
    existing = {b.name: b.size for b in dst_cc.list_blobs(name_starts_with=dst_prefix)}
    plan = []
    for blob in src_cc.list_blobs(name_starts_with=src_prefix, include=["metadata"]):
        if not blob.name.lower().endswith(extensions):
            continue
        target = dst_prefix + blob.name[len(src_prefix):]
        plan.append((blob, target, existing.get(target)))
    plan.sort(key=lambda item: item[0].name)
    return plan


def _copy_one(src_bc, dst_bc, blob, bearer_token):
    """Copy one blob; never overwrites. Returns (action, detail)."""
    metadata = dict(blob.metadata or {})
    try:
        dst_bc.upload_blob_from_url(
            src_bc.url,
            overwrite=False,
            metadata=metadata,
            source_authorization=f"Bearer {bearer_token()}",
        )
        return COPIED, ""
    except ResourceExistsError:
        return SKIPPED_EXISTS, "target appeared during run"
    except HttpResponseError as exc:
        reason = f"server-side refused ({exc.status_code} {exc.error_code})"

    # Fallback: stream through the caller. Still overwrite=False.
    cs = blob.content_settings
    downloader = src_bc.download_blob()
    try:
        dst_bc.upload_blob(
            downloader.chunks(),
            length=blob.size,
            overwrite=False,
            metadata=metadata,
            content_settings=ContentSettings(
                content_type=cs.content_type,
                content_encoding=cs.content_encoding,
                content_language=cs.content_language,
                content_disposition=cs.content_disposition,
                cache_control=cs.cache_control,
            ),
        )
    except ResourceExistsError:
        return SKIPPED_EXISTS, "target appeared during run"
    return COPIED_STREAM, reason


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def publish_pdfs(
    *,
    source_container: str = STORAGE_CONTAINER,
    source_prefix: str = REPORT_SUBFOLDER,
    dest_prefix: str = PUBLISH_SUBFOLDER,
    dest_container: str = PUBLISH_CONTAINER,
    source_account: str = STORAGE_ACCOUNT_NAME,
    dest_account: str | None = None,
    execute: bool = False,
    limit: int | None = None,
    credential=None,
    dbutils=None,
    max_workers: int = PUBLISH_MAX_WORKERS,
    extensions: tuple[str, ...] = (".pdf",),
) -> list[ManifestRow]:
    """Copy PDFs from ``source_container/source_prefix`` to
    ``dest_container/dest_prefix``. Dry run unless ``execute=True``.

    Returns the manifest rows for every source blob considered.
    """
    if source_container not in PUBLISH_ALLOWED_SOURCES:
        raise ValueError(
            f"source_container must be one of {PUBLISH_ALLOWED_SOURCES}, "
            f"got '{source_container}'"
        )
    dest_account = dest_account or source_account
    if (source_account, source_container) == (dest_account, dest_container):
        raise ValueError("Source and destination container are the same")

    src_prefix = _norm_prefix(source_prefix)
    dst_prefix = _norm_prefix(dest_prefix)
    credential = credential or get_credential(dbutils)

    def bearer_token() -> str:
        # azure-identity caches and refreshes tokens internally
        return credential.get_token(STORAGE_SCOPE).token

    src_svc = BlobServiceClient(_account_url(source_account), credential=credential)
    dst_svc = (
        src_svc
        if dest_account == source_account
        else BlobServiceClient(_account_url(dest_account), credential=credential)
    )
    src_cc = src_svc.get_container_client(source_container)
    dst_cc = dst_svc.get_container_client(dest_container)

    try:
        dst_cc.get_container_properties()
    except ResourceNotFoundError:
        raise RuntimeError(
            f"Destination container '{dest_account}/{dest_container}' does not exist; "
            "it will not be created"
        ) from None

    mode = "EXECUTE" if execute else "DRY RUN"
    print(
        f"[{mode}] {source_account}/{source_container}/{src_prefix} -> "
        f"{dest_account}/{dest_container}/{dst_prefix}"
    )

    rows: list[ManifestRow] = []
    todo = []
    for blob, target, existing_size in _plan(src_cc, dst_cc, src_prefix, dst_prefix, extensions):
        src_uri = f"{source_container}/{blob.name}"
        dst_uri = f"{dest_container}/{target}"
        if existing_size is None:
            todo.append((blob, target, src_uri, dst_uri))
        elif existing_size == blob.size:
            rows.append(ManifestRow(src_uri, dst_uri, blob.size, SKIPPED_EXISTS))
        else:
            rows.append(ManifestRow(
                src_uri, dst_uri, blob.size, SKIPPED_DIFFERS,
                f"target size {existing_size}",
            ))

    if limit is not None:
        for blob, _, src_uri, dst_uri in todo[limit:]:
            rows.append(ManifestRow(src_uri, dst_uri, blob.size, NOT_ATTEMPTED))
        todo = todo[:limit]

    if not execute:
        rows.extend(ManifestRow(s, d, b.size, WOULD_COPY) for b, _, s, d in todo)
        print_summary(rows)
        return rows

    started = monotonic()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _copy_one,
                src_cc.get_blob_client(blob.name),
                dst_cc.get_blob_client(target),
                blob,
                bearer_token,
            ): (blob, src_uri, dst_uri)
            for blob, target, src_uri, dst_uri in todo
        }
        for n, future in enumerate(as_completed(futures), start=1):
            blob, src_uri, dst_uri = futures[future]
            try:
                action, detail = future.result()
            except Exception as exc:
                action, detail = FAILED, f"{type(exc).__name__}: {exc}"[:500]
                print(f"Failed {src_uri}: {detail}")
            rows.append(ManifestRow(src_uri, dst_uri, blob.size, action, detail))
            if n % 100 == 0:
                print(f"Processed {n}/{len(todo)} in {monotonic() - started:.1f}s")

    print(f"Finished in {monotonic() - started:.1f}s")
    print_summary(rows)
    return rows


def print_summary(rows: list[ManifestRow]) -> None:
    counts = Counter(r.action for r in rows)
    total_mb = sum(r.size for r in rows if r.action in (COPIED, COPIED_STREAM, WOULD_COPY)) / 1024**2
    breakdown = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
    print(f"Summary: {breakdown or 'no matching source blobs found'}")
    print(f"Bytes (copied / would copy): {total_mb:,.1f} MB")


def manifest_csv(rows: list[ManifestRow]) -> str:
    """Render manifest rows as CSV text."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=[f.name for f in fields(ManifestRow)])
    writer.writeheader()
    writer.writerows(asdict(r) for r in rows)
    return buf.getvalue()


def manifest_filename() -> str:
    return f"copy_manifest_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.csv"


# ---------------------------------------------------------------------------
# CLI (local use)
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--execute", action="store_true", help="actually copy (default: dry run)")
    p.add_argument("--limit", type=int, default=None, help="copy at most N new blobs")
    p.add_argument("--source-container", default=STORAGE_CONTAINER, choices=PUBLISH_ALLOWED_SOURCES)
    p.add_argument("--source-prefix", default=REPORT_SUBFOLDER)
    p.add_argument("--dest-container", default=PUBLISH_CONTAINER)
    p.add_argument("--dest-prefix", default=PUBLISH_SUBFOLDER)
    p.add_argument("--source-account", default=STORAGE_ACCOUNT_NAME)
    p.add_argument("--dest-account", default=None, help="defaults to --source-account")
    p.add_argument("--workers", type=int, default=PUBLISH_MAX_WORKERS)
    p.add_argument("--manifest-dir", default="manifests")
    args = p.parse_args(argv)

    rows = publish_pdfs(
        source_container=args.source_container,
        source_prefix=args.source_prefix,
        dest_container=args.dest_container,
        dest_prefix=args.dest_prefix,
        source_account=args.source_account,
        dest_account=args.dest_account,
        execute=args.execute,
        limit=args.limit,
        max_workers=args.workers,
    )

    os.makedirs(args.manifest_dir, exist_ok=True)
    path = os.path.join(args.manifest_dir, manifest_filename())
    with open(path, "w", newline="", encoding="utf-8") as f:
        f.write(manifest_csv(rows))
    print(f"Manifest: {path}")
    return 1 if any(r.action == FAILED for r in rows) else 0


if __name__ == "__main__":
    raise SystemExit(main())
