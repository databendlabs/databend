import importlib
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from databend_udf import UDFServer, udf as udf_decorator

IMPORTS_DIR = "/app/imports"
sys._xoptions["databend_import_directory"] = IMPORTS_DIR
STATUS_ADDR = os.getenv("RESOURCE_STATUS_ADDR", "0.0.0.0:8080")
_request_lock = threading.Lock()
_request_count = 0
_first_request_time = None
_last_request_time = None

def _record_request():
    global _request_count, _first_request_time, _last_request_time
    now = time.time()
    with _request_lock:
        if _first_request_time is None:
            _first_request_time = now
        _last_request_time = now
        _request_count += 1

def _format_time(value):
    if value is None:
        return None
    return datetime.fromtimestamp(value, timezone.utc).isoformat()

class StatusHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            data = b"1"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(data)
            return
        if self.path != "/stats":
            self.send_response(404)
            self.end_headers()
            return
        payload = {
            "request_count": _request_count,
            "first_request_time": _format_time(_first_request_time),
            "last_request_time": _format_time(_last_request_time),
        }
        data = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
    def log_message(self, format, *args):
        return

def _serve_status():
    host, port = STATUS_ADDR.rsplit(":", 1)
    server = HTTPServer((host, int(port)), StatusHandler)
    server.serve_forever()

def _add_imports():
    if not os.path.isdir(IMPORTS_DIR):
        return
    sys.path.insert(0, IMPORTS_DIR)
    for name in os.listdir(IMPORTS_DIR):
        path = os.path.join(IMPORTS_DIR, name)
        if os.path.isfile(path):
            ext = os.path.splitext(name)[1].lower()
            if ext in (".zip", ".whl", ".egg"):
                sys.path.insert(0, path)

def _load_udf():
    return importlib.import_module("udf")

def _wrap_udf(func):
    return udf_decorator(name="{{HANDLER_LITERAL}}", input_types={{INPUT_TYPES_LITERAL}}, result_type="{{RESULT_TYPE_LITERAL}}")(func)

def _track_udf(udf):
    if hasattr(udf, "eval_batch"):
        original = udf.eval_batch
        def wrapped(*args, **kwargs):
            _record_request()
            return original(*args, **kwargs)
        udf.eval_batch = wrapped
        return udf
    def wrapper(*args, **kwargs):
        _record_request()
        return udf(*args, **kwargs)
    return wrapper

def main():
    status_thread = threading.Thread(target=_serve_status, daemon=True)
    status_thread.start()
    _add_imports()
    udf = _load_udf()
    address = os.getenv("UDF_SERVER_ADDR", "0.0.0.0:8815")
    server = UDFServer(location=address)
    func = getattr(udf, "{{HANDLER_LITERAL}}")
    if not hasattr(func, "_name"):
        func = _wrap_udf(func)
    func = _track_udf(func)
    server.add_function(func)
    server.serve()

if __name__ == "__main__":
    main()
