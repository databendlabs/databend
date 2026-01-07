"""Databend Meta service management utilities."""

import socket
import time
from typing import Optional

from .progress import ProgressReporter
from .args import MetaArgs
from .utils import (
    BinaryFinder,
    ConfigManager,
    ProcessManager,
    CommandBuilder,
    LogConfigHelper,
)
from .utils import PortDetector


class DatabendMeta:
    """Manager for databend-meta process."""

    def __init__(
        self,
        binary_path: Optional[str] = None,
        config_path: Optional[str] = None,
        profile: Optional[str] = None,
        args: Optional[MetaArgs] = None,
    ):
        self.binary_path = binary_path or BinaryFinder.find_binary("meta", profile)
        self.config_path = config_path or ConfigManager.get_default_config_path("meta")
        self.args = args
        self.process = None

    def _parse_config(self) -> dict:
        """Parse config file to extract settings, applying CLI overrides."""
        overrides = self.args.to_config_overrides() if self.args else None
        return ConfigManager.parse_config(self.config_path, overrides)

    def _get_grpc_port(self) -> int:
        """Extract gRPC port from config."""
        config = self._parse_config()
        grpc_address = config.get("grpc_api_address", "0.0.0.0:9191")
        return int(grpc_address.split(":")[-1])

    def _print_start_info(self) -> None:
        """Print startup information."""
        config = self._parse_config()
        ProgressReporter.print_message(f"🚀 Starting databend-meta...")
        ProgressReporter.print_message(f"   Binary: {self.binary_path}")
        ProgressReporter.print_message(f"   Config: {self.config_path}")

        # Print config file content for debugging
        ProgressReporter.print_message("🔍 Config file content:")
        try:
            with open(self.config_path, "r") as f:
                for i, line in enumerate(f, 1):
                    ProgressReporter.print_message(f"     {i:2}: {line.rstrip()}")
        except Exception as e:
            ProgressReporter.print_message(f"   ❌ Failed to read config file: {e}")

        # Print CLI args if any
        if self.args:
            cli_args = self.args.to_cli_args()
            if cli_args:
                ProgressReporter.print_message(f"🔧 CLI Args: {' '.join(cli_args)}")

            # Print config overrides
            overrides = self.args.to_config_overrides()
            if overrides:
                ProgressReporter.print_message("🔄 Config Overrides:")
                import json

                ProgressReporter.print_message(
                    f"     {json.dumps(overrides, indent=4)}"
                )

        grpc_addr = config.get("grpc_api_address", "unknown")
        admin_addr = config.get("admin_api_address", "unknown")

        ProgressReporter.print_message(f"   gRPC API: {grpc_addr}")
        ProgressReporter.print_message(f"   Admin API: {admin_addr}")

        # Raft config
        raft_config = self.get_raft_config()
        node_id = raft_config.get("id", "unknown")
        raft_dir = raft_config.get("raft_dir", "unknown")
        raft_port = raft_config.get("raft_api_port", "unknown")
        ProgressReporter.print_message(
            f"   Raft: node_id={node_id}, port={raft_port}, dir={raft_dir}"
        )

        LogConfigHelper.print_log_config(config)
        ProgressReporter.print_message("")  # Empty line for separation

    def start(self, wait_for_ready: bool = True, dry_run: bool = False) -> None:
        """Start databend-meta process."""
        if ProcessManager.is_process_running(self.process):
            raise RuntimeError("Meta service is already running")

        self._print_start_info()

        # Build command
        cmd = CommandBuilder.build_command(
            self.binary_path, self.config_path, self.args
        )

        if dry_run:
            ProgressReporter.print_message("🔍 Dry run mode - would execute:")
            ProgressReporter.print_message(f"   Command: {' '.join(cmd)}")
            return

        # Extract node ID for unique process identification
        raft_config = self.get_raft_config()
        node_id = raft_config.get("id", "unknown")

        config = self._parse_config()
        log_dir = config["log"]["file"]["dir"]
        self.process = ProcessManager.start_process(cmd, "meta", log_dir)

        if wait_for_ready:
            port = self._get_grpc_port()
            PortDetector.ping_tcp(f"meta-{node_id}", port)
            ProgressReporter.print_ready_info("databend-meta", port)

    def stop(self) -> None:
        """Stop databend-meta process."""
        ProcessManager.stop_process(self.process, "meta")
        self.process = None

    def is_running(self) -> bool:
        """Check if meta service is running."""
        return ProcessManager.is_process_running(self.process)

    def get_raft_config(self) -> dict:
        """Get raft configuration from parsed config."""
        config = self._parse_config()
        raft_config = config.get("raft_config")
        if raft_config is None:
            raise ValueError(
                f"Config file {self.config_path} missing [raft_config] section"
            )
        return raft_config
