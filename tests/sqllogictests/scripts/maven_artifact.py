"""Download Maven Central artifacts with retries for HTTP 429/5xx."""

from __future__ import annotations

import os
import random
import time
import urllib.error
import urllib.request
from pathlib import Path

MAVEN_CENTRAL = "https://repo.maven.apache.org/maven2"
USER_AGENT = "databend-ci-maven-artifact"
DEFAULT_ATTEMPTS = 8
RETRY_STATUS = {408, 425, 429, 500, 502, 503, 504}


def parse_maven_coord(coord: str) -> tuple[str, str, str]:
    parts = coord.split(":")
    if len(parts) != 3 or not all(parts):
        raise ValueError(f"expected group:artifact:version, got {coord!r}")
    return parts[0], parts[1], parts[2]


def maven_jar_url(group_id: str, artifact_id: str, version: str) -> str:
    group_path = group_id.replace(".", "/")
    return (
        f"{MAVEN_CENTRAL}/{group_path}/{artifact_id}/{version}/"
        f"{artifact_id}-{version}.jar"
    )


def download_maven_jars(coords: list[str], dest_dir: Path | None = None) -> list[str]:
    return [
        download_maven_jar(*parse_maven_coord(coord), dest_dir=dest_dir)
        for coord in coords
    ]


def retry_call(fn, *, attempts: int = DEFAULT_ATTEMPTS, what: str = "operation"):
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - CI bootstrap retries transient IO
            last_error = exc
            if attempt == attempts:
                break
            delay = min(60.0, (2 ** (attempt - 1)) + random.uniform(0, 1))
            print(
                f"{what} failed on attempt {attempt}/{attempts}: {exc}; "
                f"retrying in {delay:.1f}s",
                flush=True,
            )
            time.sleep(delay)
    raise RuntimeError(f"{what} failed after {attempts} attempts") from last_error


def download_maven_jar(
    group_id: str,
    artifact_id: str,
    version: str,
    dest_dir: Path | None = None,
) -> str:
    dest_dir = dest_dir or Path(
        os.environ.get(
            "MAVEN_ARTIFACT_CACHE",
            Path.home() / ".cache" / "databend-maven",
        )
    )
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / f"{artifact_id}-{version}.jar"
    if target.exists() and target.stat().st_size > 0:
        return str(target)

    download_url(maven_jar_url(group_id, artifact_id, version), target)
    return str(target)


def download_url(url: str, target: Path, attempts: int = DEFAULT_ATTEMPTS) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        tmp_path = target.with_suffix(f"{target.suffix}.tmp.{os.getpid()}.{attempt}")
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                with tmp_path.open("wb") as tmp_file:
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        tmp_file.write(chunk)
            if tmp_path.stat().st_size <= 0:
                raise RuntimeError(f"downloaded empty file from {url}")
            tmp_path.replace(target)
            return
        except urllib.error.HTTPError as exc:
            last_error = exc
            retryable = exc.code in RETRY_STATUS
        except (urllib.error.URLError, TimeoutError, OSError, RuntimeError) as exc:
            last_error = exc
            retryable = True
        finally:
            if tmp_path.exists():
                tmp_path.unlink()

        if not retryable or attempt == attempts:
            break

        delay = min(60.0, (2 ** (attempt - 1)) + random.uniform(0, 1))
        print(
            f"download {url} failed on attempt {attempt}/{attempts}: {last_error}; "
            f"retrying in {delay:.1f}s",
            flush=True,
        )
        time.sleep(delay)

    raise RuntimeError(
        f"failed to download {url} after {attempts} attempts"
    ) from last_error


if __name__ == "__main__":
    import http.server
    import socketserver
    import threading

    hits = {"n": 0}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            hits["n"] += 1
            if hits["n"] < 3:
                self.send_error(429, "Too Many Requests")
                return
            body = b"ok-artifact"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            return

    with socketserver.TCPServer(("127.0.0.1", 0), Handler) as httpd:
        host, port = httpd.server_address
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        dest = Path("/tmp") / f"databend-maven-artifact-test-{os.getpid()}.jar"
        dest.unlink(missing_ok=True)
        try:
            download_url(f"http://{host}:{port}/artifact.jar", dest, attempts=5)
            assert dest.read_bytes() == b"ok-artifact"
            assert hits["n"] == 3
            assert parse_maven_coord("org.apache.paimon:paimon-s3:1.4.1") == (
                "org.apache.paimon",
                "paimon-s3",
                "1.4.1",
            )
            print("maven_artifact local 429 retry ok")
        finally:
            dest.unlink(missing_ok=True)
            httpd.shutdown()
