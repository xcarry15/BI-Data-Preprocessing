import gzip
import os
import threading
import uuid
import urllib.parse
import urllib.request

BACKUP_COMPRESS_THRESHOLD_BYTES = 10 * 1024 * 1024


def _env_bool(name, default=False):
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _build_upload_url(api_base, project_id):
    base = (api_base or "https://api.tstwg.cn/api").rstrip("/")
    if not base.endswith("/api"):
        base = f"{base}/api"
    encoded_project = urllib.parse.quote(project_id, safe="-_")
    return f"{base}/upload/{encoded_project}"


def _build_multipart_payload(filename, file_bytes, note=""):
    boundary = f"----BIDataBoundary{uuid.uuid4().hex}"
    line = b"\r\n"
    body = bytearray()

    def _write_text(name, value):
        body.extend(f"--{boundary}".encode("utf-8"))
        body.extend(line)
        body.extend(
            f'Content-Disposition: form-data; name="{name}"'.encode("utf-8")
        )
        body.extend(line)
        body.extend(line)
        body.extend(str(value).encode("utf-8"))
        body.extend(line)

    _write_text("keep_name", "true")
    if note:
        _write_text("note", note)

    safe_name = (filename or "upload.xlsx").replace('"', "_")
    body.extend(f"--{boundary}".encode("utf-8"))
    body.extend(line)
    body.extend(
        f'Content-Disposition: form-data; name="file"; filename="{safe_name}"'.encode(
            "utf-8"
        )
    )
    body.extend(line)
    body.extend(b"Content-Type: application/octet-stream")
    body.extend(line)
    body.extend(line)
    body.extend(file_bytes)
    body.extend(line)
    body.extend(f"--{boundary}--".encode("utf-8"))
    body.extend(line)

    content_type = f"multipart/form-data; boundary={boundary}"
    return bytes(body), content_type


def _prepare_backup_file(filename, file_bytes):
    safe_name = filename or "upload.xlsx"
    if len(file_bytes) <= BACKUP_COMPRESS_THRESHOLD_BYTES:
        return safe_name, file_bytes
    compressed_name = safe_name if safe_name.endswith(".gz") else f"{safe_name}.gz"
    return compressed_name, gzip.compress(file_bytes)


def backup_uploaded_file(filename, file_bytes, note=""):
    if not _env_bool("FILE_BACKUP_ENABLED", default=True):
        return False

    api_key = (os.getenv("FILE_STORAGE_API_KEY") or "").strip()
    project_id = (os.getenv("FILE_BACKUP_PROJECT_ID") or "bi-data").strip()
    api_base = (os.getenv("FILE_API_BASE") or "https://api.tstwg.cn/api").strip()
    timeout = float(os.getenv("FILE_BACKUP_TIMEOUT_SEC", "10"))

    if not api_key or not project_id or not file_bytes:
        return False

    backup_filename, backup_file_bytes = _prepare_backup_file(filename, file_bytes)
    body, content_type = _build_multipart_payload(backup_filename, backup_file_bytes, note=note)
    url = _build_upload_url(api_base, project_id)

    req = urllib.request.Request(
        url=url,
        data=body,
        method="POST",
        headers={
            "X-API-Key": api_key,
            "Content-Type": content_type,
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", None)
            if not isinstance(status, int):
                status = resp.getcode()
            return 200 <= int(status) < 300
    except Exception:
        return False


def backup_uploaded_file_async(filename, file_bytes, note=""):
    thread = threading.Thread(
        target=backup_uploaded_file,
        args=(filename, file_bytes, note),
        daemon=True,
    )
    thread.start()
    return thread
