#!/usr/bin/env python3
"""Download MIRACL Chinese corpus in parallel (Python standard library only).

Run from the directory where the dataset should be saved:
    python scripts/download_miracl_zh.py --workers 5
Rerun the same command to resume interrupted downloads.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
from pathlib import Path
import re
import threading
import time
from urllib.request import Request, urlopen


REPO = "miracl/miracl-corpus"
FOLDER = "miracl-corpus-v1.0-zh"
BASE = "https://huggingface.co"
PRINT_LOCK = threading.Lock()


def log(message):
    with PRINT_LOCK:
        print(message, flush=True)


def request(url, headers=None):
    return urlopen(Request(url, headers={
        "User-Agent": "miracl-zh-downloader/1.0",
        "Accept-Encoding": "identity",
        **(headers or {}),
    }), timeout=60)


def get_json(url, retries):
    for attempt in range(retries):
        try:
            with request(url) as response:
                return json.load(response)
        except Exception:
            if attempt + 1 == retries:
                raise
            time.sleep(min(2 ** attempt, 30))


def valid(path, size, sha256):
    if not path.is_file() or path.stat().st_size != size:
        return False
    if not sha256:
        raise ValueError("Missing SHA-256 in dataset metadata")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest() == sha256


def download(entry, revision, output, retries):
    name = Path(entry["path"]).name
    target = output / name
    partial = output / (name + ".part")
    size = entry["size"]
    sha256 = entry.get("lfs", {}).get("oid")
    if not sha256 or not re.fullmatch(r"[0-9a-f]{64}", sha256):
        raise ValueError(f"{name}: missing or invalid SHA-256 metadata")
    if valid(target, size, sha256):
        log(f"[skip] {name}: already verified")
        return
    if target.exists():
        raise FileExistsError(f"{target} exists but fails verification; move it aside first")
    url = f"{BASE}/datasets/{REPO}/resolve/{revision}/{entry['path']}"
    for attempt in range(retries):
        try:
            offset = partial.stat().st_size if partial.exists() else 0
            if offset >= size:
                if valid(partial, size, sha256):
                    partial.replace(target)
                    log(f"[done] {name}: SHA-256 verified")
                    return
                with partial.open("wb"):
                    pass
                offset = 0
            headers = {"Range": f"bytes={offset}-"} if offset else {}
            log(f"[start] {name}: {offset / 1e6:.1f}/{size / 1e6:.1f} MB")
            with request(url, headers) as response:
                if response.status == 206:
                    content_range = response.headers.get("Content-Range", "")
                    match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", content_range)
                    if not match or int(match[1]) != offset or int(match[3]) != size:
                        raise ValueError(f"Unexpected Content-Range: {content_range}")
                elif response.status == 200:
                    offset = 0  # Server ignored Range: safely restart this file.
                else:
                    raise ValueError(f"Unexpected HTTP status: {response.status}")
                last_report = time.monotonic()
                speed_start = last_report
                speed_offset = offset
                with partial.open("ab" if offset else "wb") as stream:
                    while True:
                        block = response.read1(1024 * 1024)
                        if not block:
                            break
                        stream.write(block)
                        offset += len(block)
                        if offset > size:
                            raise ValueError("Response exceeded expected file size")
                        if time.monotonic() - last_report >= 10:
                            log(f"[progress] {name}: {offset / size:.0%}")
                            last_report = time.monotonic()
                        elapsed = time.monotonic() - speed_start
                        if elapsed >= 30:
                            if (offset - speed_offset) / elapsed < 64 * 1024:
                                raise TimeoutError("Connection below 64 KiB/s for 30s; reconnecting")
                            speed_start = time.monotonic()
                            speed_offset = offset
            if not valid(partial, size, sha256):
                if partial.stat().st_size >= size:
                    with partial.open("wb"):
                        pass
                raise ValueError("Size or SHA-256 mismatch")
            partial.replace(target)
            log(f"[done] {name}: SHA-256 verified")
            return
        except Exception as error:
            if attempt + 1 == retries:
                raise
            log(f"[retry {attempt + 1}/{retries}] {name}: {error}")
            time.sleep(min(2 ** attempt, 30))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=5, help="Parallel downloads (default: 5)")
    parser.add_argument("--retries", type=int, default=6, help="Attempts per request (default: 6)")
    parser.add_argument("--output", type=Path, default=Path.cwd() / FOLDER)
    parser.add_argument("--revision", default="main", help="Dataset revision (default: main)")
    args = parser.parse_args()
    if args.workers < 1 or args.retries < 1:
        parser.error("--workers and --retries must be positive")
    # Pin all downloads to one immutable revision.
    metadata = get_json(f"{BASE}/api/datasets/{REPO}/revision/{args.revision}", args.retries)
    revision = metadata["sha"]
    entries = get_json(f"{BASE}/api/datasets/{REPO}/tree/{revision}/{FOLDER}", args.retries)
    files = [entry for entry in entries if entry["type"] == "file"
             and re.fullmatch(r"docs-\d+\.jsonl\.gz", Path(entry["path"]).name)]
    if len(files) != 10:
        raise ValueError(f"Expected 10 corpus shards, found {len(files)}")
    args.output.mkdir(parents=True, exist_ok=True)
    log(f"Revision: {revision}")
    log(f"Downloading {len(files)} files, {sum(f['size'] for f in files) / 1e6:.1f} MB, "
        f"{args.workers} workers -> {args.output.resolve()}")
    failures = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        jobs = {executor.submit(download, entry, revision, args.output, args.retries): entry
                for entry in files}
        for job in as_completed(jobs):
            try:
                job.result()
            except Exception as error:
                failures.append(jobs[job]["path"])
                log(f"[failed] {jobs[job]['path']}: {error}")
    if failures:
        log(f"{len(failures)} file(s) failed. Rerun this command to resume.")
        return 1
    log("All 10 files downloaded and SHA-256 verified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
