"""Databend Test Helper

A Python library for starting and stopping Databend processes during testing.
Provides utilities for managing databend-meta and databend-query instances.
"""

from importlib.metadata import version

__version__ = version("databend-test-helper")

from .meta import DatabendMeta
from .query import DatabendQuery
from .progress import ProgressReporter
from .meta_cluster import MetaCluster
from .query_cluster import QueryCluster
from .cluster import DatabendCluster
from .args import MetaArgs, QueryArgs

__all__ = [
    "DatabendMeta",
    "DatabendQuery",
    "ProgressReporter",
    "MetaCluster",
    "QueryCluster",
    "DatabendCluster",
    "MetaArgs",
    "QueryArgs",
]
