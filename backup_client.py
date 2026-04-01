import gzip
import os
import queue
import threading
import time
import uuid
import urllib.parse
import urllib.request

BACKUP_COMPRESS_THRESHOLD_BYTES = 10 * 1024 * 1024
ASYNC_QUEUE_MAX_SIZE = 200
_backup_queue = queue.Queue(maxsize=ASYNC_QUEUE_MAX_SIZE)
_worker_lock = threading.Lock()
_worker_thread = None
_active_uploads = 0
_active_lock = threading.Lock()


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


def _backup_worker_loop():
    global _active_uploads
    while True:
        filename, file_bytes, note = _backup_queue.get()
        with _active_lock:
            _active_uploads += 1
        try:
            backup_uploaded_file(filename, file_bytes, note=note)
        finally:
            with _active_lock:
                _active_uploads -= 1
            _backup_queue.task_done()


def _ensure_backup_worker():
    global _worker_thread
    with _worker_lock:
        if _worker_thread is not None and _worker_thread.is_alive():
            return _worker_thread
        _worker_thread = threading.Thread(target=_backup_worker_loop, daemon=True)
        _worker_thread.start()
        return _worker_thread


def wait_for_backup_queue_idle(timeout=5.0):
    deadline = time.monotonic() + max(timeout, 0)
    while time.monotonic() <= deadline:
        with _active_lock:
            active = _active_uploads
        if _backup_queue.unfinished_tasks == 0 and active == 0:
            return True
        time.sleep(0.01)
    return False


def backup_uploaded_file_async(filename, file_bytes, note=""):
    worker = _ensure_backup_worker()
    try:
        _backup_queue.put_nowait((filename, file_bytes, note))
    except queue.Full:
        return None
    return worker
