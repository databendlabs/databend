import io
import json
import urllib.error
from unittest.mock import Mock

import pytest

import noxfile


@pytest.mark.parametrize("version", ["0.34.2", "0.34.3"])
def test_latest_python_driver_uses_pypi(monkeypatch, version):
    response = io.BytesIO(json.dumps({"info": {"version": version}}).encode())
    urlopen = Mock(return_value=response)
    monkeypatch.setattr(noxfile.urllib.request, "urlopen", urlopen)
    github_latest = Mock(side_effect=AssertionError("must not use GitHub releases"))
    monkeypatch.setattr(noxfile, "resolve_latest_release_tag", github_latest)

    assert noxfile.resolve_python_driver_version("latest") == version
    request = urlopen.call_args.args[0]
    assert request.full_url == "https://pypi.org/pypi/databend-driver/json"
    assert urlopen.call_args.kwargs == {"timeout": 30}
    github_latest.assert_not_called()


def test_pinned_python_driver_does_not_access_network(monkeypatch):
    urlopen = Mock(
        side_effect=AssertionError("pinned versions must not access the network")
    )
    monkeypatch.setattr(noxfile.urllib.request, "urlopen", urlopen)

    assert noxfile.resolve_python_driver_version("0.33.6") == "0.33.6"
    urlopen.assert_not_called()


def test_latest_python_driver_propagates_pypi_failure(monkeypatch):
    urlopen = Mock(side_effect=urllib.error.URLError("PyPI unavailable"))
    monkeypatch.setattr(noxfile.urllib.request, "urlopen", urlopen)

    with pytest.raises(urllib.error.URLError, match="PyPI unavailable"):
        noxfile.resolve_python_driver_version("latest")
