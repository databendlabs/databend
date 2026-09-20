"""Databend Test Helper

A Python library for starting and stopping Databend processes during testing.
Provides utilities for managing databend-meta and databend-query instances.
"""

from importlib import import_module
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("databend-test-helper")
except PackageNotFoundError:
    # Imported straight from the source tree without `pip install`.
    __version__ = "0.0.0+uninstalled"

_EXPORTS = {
    "DatabendMeta": ".meta",
    "DatabendQuery": ".query",
    "ProgressReporter": ".progress",
    "MetaCluster": ".meta_cluster",
    "QueryCluster": ".query_cluster",
    "DatabendCluster": ".cluster",
    "LocalMetaCluster": ".local_meta_cluster",
    "LocalMetaNode": ".local_meta_cluster",
    "MetaNodePorts": ".local_meta_cluster",
    "MetaGrpcCredential": ".meta_config",
    "MetaSecurityProfile": ".meta_config",
    "MetaClientProfile": ".meta_config",
    "write_password_file": ".meta_config",
    "render_meta_config": ".meta_config",
    "MetaArgs": ".args",
    "QueryArgs": ".args",
}

__all__ = list(_EXPORTS)


def __getattr__(name):
    """Load public helpers only when callers request them."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(_EXPORTS[name], __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
