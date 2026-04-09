#!/usr/bin/env python3
import argparse
import base64
import concurrent.futures
import hashlib
import html
import json
import mimetypes
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone


DEFAULT_EXTENSIONS = {
    ".pdf",
    ".docx",
    ".txt",
    ".md",
    ".xlsx",
    ".pptx",
    ".csv",
    ".json",
    ".xml",
    ".html",
    ".htm",
    ".rtf",
    ".png",
    ".jpg",
    ".jpeg",
    ".tif",
    ".tiff",
    ".gif",
    ".bmp",
}


class RequestPacer:
    def __init__(self, min_gap_ms):
        self.min_gap_s = max(0, int(min_gap_ms or 0)) / 1000.0
        self.lock = threading.Lock()
        self.next_allowed_at = 0.0

    def wait_turn(self):
        if self.min_gap_s <= 0:
            return
        while True:
            with self.lock:
                now = time.monotonic()
                if now >= self.next_allowed_at:
                    self.next_allowed_at = now + self.min_gap_s
                    return
                sleep_for = self.next_allowed_at - now
            time.sleep(min(sleep_for, 0.25))


def parse_retry_after_ms(error_body):
    if not error_body:
        return None
    try:
        parsed = json.loads(error_body)
        retry_ms = parsed.get("retryAfterMs")
        if retry_ms is None:
            return None
        retry_ms = int(retry_ms)
        return retry_ms if retry_ms > 0 else None
    except Exception:
        return None


def classify_failure(error_text):
    text = str(error_text or "").lower()
    if "no extractable text" in text:
        return "needs-ocr-or-manual-review"
    if "http 429" in text or "rate limit exceeded" in text:
        return "rate-limited"
    if "broken pipe" in text:
        return "connection-dropped"
    if "timed out" in text or "timeout" in text:
        return "timeout"
    return "other"


def to_iso(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def read_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def write_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def sanitize_relative_name(rel_path):
    safe = rel_path.replace(os.sep, "__")
    safe = safe.replace("/", "__")
    return safe


def list_files(root_dir, allowed_exts):
    files = []
    for current, _dirs, names in os.walk(root_dir):
        for name in names:
            abs_path = os.path.join(current, name)
            ext = os.path.splitext(name)[1].lower()
            if allowed_exts and ext not in allowed_exts:
                continue
            files.append(abs_path)
    files.sort()
    return files


def compute_sha256(file_path, chunk_size=1024 * 1024):
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def normalize_text(text):
    s = str(text or "")
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def xml_text(xml_bytes):
    raw = xml_bytes.decode("utf-8", errors="ignore")
    raw = re.sub(r"<[^>]+>", " ", raw)
    raw = html.unescape(raw)
    return normalize_text(raw)


def extract_text_local(abs_path, ext):
    ext = (ext or "").lower()

    if ext in {".txt", ".md", ".csv", ".json", ".xml", ".html", ".htm"}:
        with open(abs_path, "r", encoding="utf-8", errors="ignore") as f:
            return normalize_text(f.read()), "local-text"

    if ext == ".docx":
        with zipfile.ZipFile(abs_path, "r") as z:
            names = [n for n in z.namelist() if n.startswith("word/") and n.endswith(".xml")]
            chunks = []
            for name in sorted(names):
                if name.startswith("word/document") or name.startswith("word/header") or name.startswith("word/footer"):
                    chunks.append(xml_text(z.read(name)))
            return normalize_text("\n\n".join(chunks)), "local-docx"

    if ext == ".xlsx":
        with zipfile.ZipFile(abs_path, "r") as z:
            chunks = []
            for name in sorted(z.namelist()):
                if name == "xl/sharedStrings.xml" or (name.startswith("xl/worksheets/") and name.endswith(".xml")):
                    chunks.append(xml_text(z.read(name)))
            return normalize_text("\n\n".join(chunks)), "local-xlsx"

    if ext == ".pptx":
        with zipfile.ZipFile(abs_path, "r") as z:
            chunks = []
            for name in sorted(z.namelist()):
                if name.startswith("ppt/slides/") and name.endswith(".xml"):
                    chunks.append(xml_text(z.read(name)))
            return normalize_text("\n\n".join(chunks)), "local-pptx"

    if ext == ".pdf":
        try:
            import pypdf
            reader = pypdf.PdfReader(abs_path)
            text = "\n\n".join((p.extract_text() or "") for p in reader.pages)
            return normalize_text(text), "pypdf"
        except Exception:
            pass
        try:
            proc = subprocess.run(["pdftotext", abs_path, "-"], check=False, capture_output=True, text=True)
            text = normalize_text(proc.stdout)
            if text:
                return text, "pdftotext"
        except Exception:
            pass
        if sys.platform == "darwin":
            swift_ocr = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pdf_ocr.swift")
            if os.path.exists(swift_ocr):
                try:
                    proc = subprocess.run(
                        ["swift", swift_ocr, abs_path],
                        check=False,
                        capture_output=True,
                        text=True,
                        timeout=240,
                    )
                    text = normalize_text(proc.stdout)
                    if text:
                        return text, "swift-vision-ocr"
                except Exception:
                    pass
        return "", "none"

    if ext in {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".gif"}:
        try:
            proc = subprocess.run(["tesseract", abs_path, "stdout"], check=False, capture_output=True, text=True)
            return normalize_text(proc.stdout), "tesseract"
        except Exception:
            return "", "none"

    if ext in {".rtf", ".doc"}:
        try:
            proc = subprocess.run(["textutil", "-convert", "txt", "-stdout", abs_path], check=False, capture_output=True, text=True)
            return normalize_text(proc.stdout), "textutil"
        except Exception:
            return "", "none"

    return "", "none"


def build_request(bridge_base, token, payload):
    url = bridge_base.rstrip("/") + "/docs/upload"
    if token:
        url += "?token=" + urllib.parse.quote(token)

    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Content-Length": str(len(body)),
    }
    if token:
        headers["x-upload-token"] = token

    return urllib.request.Request(url, data=body, headers=headers, method="POST")


def upload_one(abs_path, root_dir, args):
    rel_path = os.path.relpath(abs_path, root_dir)
    upload_name = sanitize_relative_name(rel_path)
    ext = os.path.splitext(abs_path)[1].lower()
    mime_type = mimetypes.guess_type(abs_path)[0] or "application/octet-stream"

    with open(abs_path, "rb") as f:
        data = f.read()

    extracted_text, extractor = extract_text_local(abs_path, ext)
    extracted_chars = len(extracted_text)
    if args.require_text and extracted_chars == 0:
        return {
            "ok": False,
            "relPath": rel_path,
            "bytes": len(data),
            "ext": ext,
            "error": f"No extractable text (extractor={extractor})",
            "sha256": hashlib.sha256(data).hexdigest(),
            "extractor": extractor,
            "extractedChars": extracted_chars,
        }

    payload = {
        "filename": upload_name,
        "content": base64.b64encode(data).decode("ascii"),
        "mimeType": mime_type,
        "lastModified": to_iso(os.path.getmtime(abs_path)),
        "group": args.group,
        "site": args.site,
        "webUrl": rel_path,
        "text": extracted_text,
        "requireText": bool(args.require_text),
    }

    last_error = None
    for attempt in range(1, args.retries + 2):
        try:
            args.request_pacer.wait_turn()
            req = build_request(args.bridge, args.token, payload)
            with urllib.request.urlopen(req, timeout=args.timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                parsed = json.loads(raw) if raw.strip() else {}
                if resp.status < 200 or resp.status >= 300:
                    raise RuntimeError(f"HTTP {resp.status}: {raw[:300]}")
                return {
                    "ok": True,
                    "relPath": rel_path,
                    "bytes": len(data),
                    "ext": ext,
                    "chunkCount": parsed.get("chunkCount"),
                    "wordCount": parsed.get("wordCount"),
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "extractor": extractor,
                    "extractedChars": extracted_chars,
                }
        except urllib.error.HTTPError as e:
            body = ''
            try:
                body = e.read().decode('utf-8', errors='replace').strip()
            except Exception:
                body = ''
            last_error = f"HTTP {e.code}: {body or str(e)}"
            if attempt <= args.retries:
                retry_after_ms = parse_retry_after_ms(body)
                if e.code == 429 and retry_after_ms:
                    time.sleep((retry_after_ms / 1000.0) + 1.0)
                else:
                    time.sleep(min(20, 2 * attempt))
                continue
            return {
                "ok": False,
                "relPath": rel_path,
                "bytes": len(data),
                "ext": ext,
                "error": last_error,
                "sha256": hashlib.sha256(data).hexdigest(),
                "extractor": extractor,
                "extractedChars": extracted_chars,
            }
        except (urllib.error.URLError, TimeoutError, RuntimeError, json.JSONDecodeError) as e:
            last_error = str(e)
            if attempt <= args.retries:
                time.sleep(min(20, 2 * attempt))
                continue
            return {
                "ok": False,
                "relPath": rel_path,
                "bytes": len(data),
                "ext": ext,
                "error": last_error,
                "sha256": hashlib.sha256(data).hexdigest(),
                "extractor": extractor,
                "extractedChars": extracted_chars,
            }


def parse_args():
    p = argparse.ArgumentParser(description="Bulk upload documents to WMS·IQ /docs/upload")
    p.add_argument("--dir", required=True, help="Local folder to scan recursively")
    p.add_argument("--group", required=True, help="Group id (e.g., manhattan-main, wmshub-prod, cigwms-prod)")
    p.add_argument("--bridge", default="http://localhost:3333", help="Bridge base URL")
    p.add_argument("--token", default="", help="Upload token if configured")
    p.add_argument("--site", default="local-bulk-upload", help="Site/source label")
    p.add_argument("--concurrency", type=int, default=4, help="Parallel uploads (default: 4)")
    p.add_argument("--timeout", type=int, default=120, help="HTTP timeout seconds per file")
    p.add_argument("--retries", type=int, default=2, help="Retries per file (default: 2)")
    p.add_argument(
        "--min-request-gap-ms",
        type=int,
        default=700,
        help="Minimum gap between upload requests across all workers (default: 700ms)",
    )
    p.add_argument("--state-file", default=".bulk-upload-state.json", help="Resume state file")
    p.add_argument("--report-file", default="bulk-upload-report.json", help="Final report output")
    p.add_argument(
        "--extensions",
        default=",".join(sorted(DEFAULT_EXTENSIONS)),
        help="Comma-separated extensions to include (default includes office/pdf/text/images)",
    )
    p.add_argument(
        "--no-hash-dedupe",
        action="store_true",
        help="Disable SHA-256 duplicate skipping and force upload even for identical-content files",
    )
    p.add_argument("--dry-run", action="store_true", help="List files without uploading")
    p.add_argument(
        "--require-text",
        action="store_true",
        help="Fail a file if no text can be extracted locally (recommended for strict knowledge quality)",
    )
    return p.parse_args()


def main():
    args = parse_args()
    args.request_pacer = RequestPacer(args.min_request_gap_ms)
    root_dir = os.path.abspath(args.dir)
    if not os.path.isdir(root_dir):
        print(f"ERROR: folder not found: {root_dir}", file=sys.stderr)
        return 2

    allowed_exts = {
        e.strip().lower() if e.strip().startswith(".") else "." + e.strip().lower()
        for e in args.extensions.split(",")
        if e.strip()
    }

    all_files = list_files(root_dir, allowed_exts)
    if not all_files:
        print("No files found for selected extensions.")
        return 0

    state = read_json(args.state_file, {"uploaded": {}, "uploadedByHash": {}, "failed": {}})
    uploaded_map = state.get("uploaded", {}) if isinstance(state, dict) else {}
    uploaded_by_hash = state.get("uploadedByHash", {}) if isinstance(state, dict) else {}
    if not isinstance(uploaded_by_hash, dict):
        uploaded_by_hash = {}

    if args.no_hash_dedupe:
        print("Computing SHA-256 hashes (dedupe disabled by --no-hash-dedupe)...")
    else:
        print("Computing SHA-256 hashes for dedupe...")
    file_meta = {}
    for path in all_files:
        rel = os.path.relpath(path, root_dir)
        mtime = int(os.path.getmtime(path))
        size = os.path.getsize(path)
        key = f"{rel}::{mtime}::{size}"
        sha = compute_sha256(path)
        file_meta[path] = {
            "rel": rel,
            "key": key,
            "mtime": mtime,
            "size": size,
            "sha256": sha,
        }

    pending = []
    duplicate_hash_skips = 0
    for path in all_files:
        meta = file_meta[path]
        rel = meta["rel"]
        key = meta["key"]
        sha = meta["sha256"]
        if key in uploaded_map:
            continue
        if (not args.no_hash_dedupe) and (sha in uploaded_by_hash):
            duplicate_hash_skips += 1
            uploaded_map[key] = {
                "relPath": rel,
                "uploadedAt": to_iso(time.time()),
                "bytes": meta["size"],
                "chunkCount": None,
                "wordCount": None,
                "sha256": sha,
                "duplicateOf": uploaded_by_hash.get(sha, {}).get("relPath", ""),
                "dedupeSkipped": True,
            }
            continue
        pending.append((path, key))

    print(f"Root: {root_dir}")
    print(f"Bridge: {args.bridge}")
    print(f"Group: {args.group}")
    print(f"Found: {len(all_files)} files")
    print(f"Already uploaded (resume): {len(all_files) - len(pending)}")
    if duplicate_hash_skips:
        print(f"Skipped as exact duplicates by SHA-256: {duplicate_hash_skips}")
    print(f"Pending: {len(pending)}")
    if args.require_text:
        print("Strict mode: --require-text enabled (files without extracted text will fail)")
    print(f"Request pacing: {args.min_request_gap_ms} ms minimum gap")

    if args.dry_run:
        for p, _k in pending[:100]:
            print(" -", os.path.relpath(p, root_dir))
        if len(pending) > 100:
            print(f"... plus {len(pending) - 100} more")
        return 0

    if not pending:
        print("Nothing to upload.")
        return 0

    results = []
    failed = {}
    done = 0
    total = len(pending)
    bytes_uploaded = 0
    extracted_chars_uploaded = 0

    def worker(item):
        path, key = item
        result = upload_one(path, root_dir, args)
        return key, result

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as ex:
        future_map = {ex.submit(worker, item): item for item in pending}
        for fut in concurrent.futures.as_completed(future_map):
            key, result = fut.result()
            done += 1
            rel = result.get("relPath", "?")
            if result.get("ok"):
                sha = result.get("sha256")
                uploaded_map[key] = {
                    "relPath": rel,
                    "uploadedAt": to_iso(time.time()),
                    "bytes": result.get("bytes", 0),
                    "chunkCount": result.get("chunkCount"),
                    "wordCount": result.get("wordCount"),
                    "sha256": sha,
                }
                if sha:
                    uploaded_by_hash[sha] = {
                        "relPath": rel,
                        "uploadedAt": to_iso(time.time()),
                    }
                bytes_uploaded += int(result.get("bytes", 0) or 0)
                extracted_chars_uploaded += int(result.get("extractedChars", 0) or 0)
                print(f"[{done}/{total}] OK   {rel}")
            else:
                failed[key] = {
                    "relPath": rel,
                    "error": result.get("error", "unknown"),
                    "at": to_iso(time.time()),
                }
                print(f"[{done}/{total}] FAIL {rel} :: {result.get('error', 'unknown')}")

            state_out = {
                "uploaded": uploaded_map,
                "uploadedByHash": uploaded_by_hash,
                "failed": failed,
            }
            write_json(args.state_file, state_out)
            results.append(result)

    ok_count = sum(1 for r in results if r.get("ok"))
    fail_count = len(results) - ok_count
    failure_buckets = {}
    ocr_candidates = []
    for result in results:
        if result.get("ok"):
            continue
        bucket = classify_failure(result.get("error"))
        failure_buckets[bucket] = failure_buckets.get(bucket, 0) + 1
        if bucket == "needs-ocr-or-manual-review":
            ocr_candidates.append(result.get("relPath"))

    report = {
        "root": root_dir,
        "group": args.group,
        "bridge": args.bridge,
        "totalScanned": len(all_files),
        "uploadedThisRun": ok_count,
        "failedThisRun": fail_count,
        "bytesUploadedThisRun": bytes_uploaded,
        "extractedCharsThisRun": extracted_chars_uploaded,
        "requireText": bool(args.require_text),
        "minRequestGapMs": args.min_request_gap_ms,
        "failureBuckets": failure_buckets,
        "ocrCandidates": ocr_candidates,
        "timestamp": to_iso(time.time()),
        "results": results,
    }
    write_json(args.report_file, report)

    print("\n--- Summary ---")
    print(f"Uploaded: {ok_count}")
    print(f"Failed:   {fail_count}")
    print(f"State:    {args.state_file}")
    print(f"Report:   {args.report_file}")
    if failure_buckets:
        print(f"Failure buckets: {json.dumps(failure_buckets, sort_keys=True)}")
    if ocr_candidates:
        print(f"OCR/manual review needed: {len(ocr_candidates)} files")
    if fail_count > 0:
        print("Re-run the same command to retry only failed/pending files.")

    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
