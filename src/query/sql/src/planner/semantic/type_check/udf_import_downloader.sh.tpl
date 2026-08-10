python - <<'PY'
import json
import os
import urllib.parse
import urllib.request

def _rewrite_url(url):
    override = os.getenv("RESOURCE_IMPORTS_HOST")
    if not override:
        return url
    parsed = urllib.parse.urlsplit(url)
    if parsed.hostname not in ("127.0.0.1", "localhost"):
        return url
    netloc = override
    if parsed.port:
        netloc = f"{override}:{parsed.port}"
    return urllib.parse.urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))

def _import_basename(location, index):
    name = location.strip().rstrip("/").split("/")[-1]
    if not name:
        return f"import_{index}"
    return name

with open("/app/imports.json", "r") as f:
    imports = json.load(f)

for index, item in enumerate(imports):
    location = item.get("location", "")
    url = item.get("url", "")
    headers = item.get("headers") or {}
    if not url:
        continue
    url = _rewrite_url(url)
    dest = os.path.join("/app/imports", _import_basename(location, index))
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp, open(dest, "wb") as out:
        out.write(resp.read())
PY
