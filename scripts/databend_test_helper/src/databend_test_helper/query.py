"""Databend Query service management utilities."""

import time
import requests
from typing import Optional

from .progress import ProgressReporter
from .args import QueryArgs
from .utils import (
    BinaryFinder,
    ConfigManager,
    ProcessManager,
    CommandBuilder,
    LogConfigHelper,
)
from .utils import PortDetector


class DatabendQuery:
    """Manager for databend-query process."""

    def __init__(
        self,
        binary_path: Optional[str] = None,
        config_path: Optional[str] = None,
        profile: Optional[str] = None,
        args: Optional[QueryArgs] = None,
        node_id: Optional[str] = None,
    ):
        self.binary_path = binary_path or BinaryFinder.find_binary("query", profile)
        self.config_path = config_path or ConfigManager.get_default_config_path("query")
        self.args = args
        self.process = None
        self.node_id = node_id or "1"

    def _parse_config(self) -> dict:
        """Parse config file to extract settings, applying CLI overrides."""
        overrides = self.args.to_config_overrides() if self.args else None
        return ConfigManager.parse_config(self.config_path, overrides)

    def _print_start_info(self) -> None:
        """Print startup information."""
        config = self._parse_config()
        ProgressReporter.print_message(f"🚀 Starting databend-query...")
        ProgressReporter.print_message(f"   Binary: {self.binary_path}")
        ProgressReporter.print_message(f"   Config: {self.config_path}")

        query_config = config.get("query", {})

        # Handler ports
        http_host = query_config.get("http_handler_host", "0.0.0.0")
        http_port = query_config.get("http_handler_port", "unknown")
        mysql_host = query_config.get("mysql_handler_host", "0.0.0.0")
        mysql_port = query_config.get("mysql_handler_port", "unknown")
        admin_addr = query_config.get("admin_api_address", "unknown")

        ProgressReporter.print_message(f"   HTTP API: {http_host}:{http_port}")
        ProgressReporter.print_message(f"   MySQL: {mysql_host}:{mysql_port}")
        ProgressReporter.print_message(f"   Admin API: {admin_addr}")

        # Cluster info
        tenant_id = query_config.get("tenant_id", "unknown")
        cluster_id = query_config.get("cluster_id", "unknown")
        ProgressReporter.print_message(
            f"   Cluster: tenant={tenant_id}, cluster={cluster_id}"
        )

        # Meta endpoints
        meta_config = config.get("meta", {})
        endpoints = meta_config.get("endpoints", [])
        if endpoints:
            ProgressReporter.print_message(f"   Meta endpoints: {', '.join(endpoints)}")

        # Storage config
        storage_config = config.get("storage", {})
        storage_type = storage_config.get("type", "unknown")
        if storage_type == "fs":
            fs_config = storage_config.get("fs", {})
            data_path = fs_config.get("data_path", "unknown")
            ProgressReporter.print_message(f"   Storage: {storage_type} ({data_path})")
        else:
            ProgressReporter.print_message(f"   Storage: {storage_type}")

        LogConfigHelper.print_log_config(config)
        ProgressReporter.print_message("")  # Empty line for separation

    def start(self, wait_for_ready: bool = True, dry_run: bool = False) -> None:
        """Start databend-query process."""
        if ProcessManager.is_process_running(self.process):
            raise RuntimeError("Query service is already running")

        self._print_start_info()

        # Build command
        cmd = CommandBuilder.build_command(
            self.binary_path, self.config_path, self.args
        )

        if dry_run:
            ProgressReporter.print_message("🔍 Dry run mode - would execute:")
            ProgressReporter.print_message(f"   Command: {' '.join(cmd)}")
            return

        config = self._parse_config()
        log_dir = config["log"]["file"]["dir"]
        self.process = ProcessManager.start_process(cmd, "query", log_dir)

        if wait_for_ready:
            config = self._parse_config()
            port = config["query"]["mysql_handler_port"]
            node_id = self.node_id
            PortDetector.ping_tcp(f"query-{node_id}", port)
            ProgressReporter.print_ready_info("databend-query", port)

    def stop(self) -> None:
        """Stop databend-query process."""
        ProcessManager.stop_process(self.process, "query")
        self.process = None

    def is_running(self) -> bool:
        """Check if query service is running."""
        return ProcessManager.is_process_running(self.process)
