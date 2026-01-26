"""CLI arguments classes for Databend processes."""

from dataclasses import dataclass
from typing import Optional, List
import copy

from .utils import LogConfigHelper


class ArgsUtils:
    """Utility functions for Args classes."""

    @staticmethod
    def clone_args(args_instance):
        """Create a deep copy of an Args instance."""
        return copy.deepcopy(args_instance)

    @staticmethod
    def merge_args(target_args, source_args):
        """Merge source Args into target Args, replacing None values in-place."""
        for field_name, field_value in source_args.__dict__.items():
            if field_value is not None:
                setattr(target_args, field_name, field_value)
        return target_args


@dataclass
class MetaArgs:
    """CLI arguments for databend-meta process."""

    # Network configuration
    admin_api_address: Optional[str] = None
    grpc_api_address: Optional[str] = None
    grpc_api_advertise_host: Optional[str] = None

    # Raft configuration
    raft_dir: Optional[str] = None
    raft_api_port: Optional[int] = None
    raft_listen_host: Optional[str] = None
    raft_advertise_host: Optional[str] = None

    # Cluster configuration
    id: Optional[int] = None
    single: Optional[bool] = None
    join: Optional[List[str]] = None

    # Log configuration
    log_level: Optional[str] = None
    log_dir: Optional[str] = None
    log_format: Optional[str] = None

    def to_config_overrides(self) -> dict:
        """Convert CLI args to config file overrides."""
        overrides = {}

        # Top-level fields
        if self.admin_api_address is not None:
            overrides["admin_api_address"] = self.admin_api_address
        if self.grpc_api_address is not None:
            overrides["grpc_api_address"] = self.grpc_api_address
        if self.grpc_api_advertise_host is not None:
            overrides["grpc_api_advertise_host"] = self.grpc_api_advertise_host
        if self.single is not None:
            overrides["single"] = self.single
        if self.join is not None:
            overrides["join"] = self.join

        # Raft config section
        raft_config = {}
        if self.id is not None:
            raft_config["id"] = self.id
        if self.raft_dir is not None:
            raft_config["raft_dir"] = self.raft_dir
        if self.raft_api_port is not None:
            raft_config["raft_api_port"] = self.raft_api_port
        if self.raft_listen_host is not None:
            raft_config["raft_listen_host"] = self.raft_listen_host
        if self.raft_advertise_host is not None:
            raft_config["raft_advertise_host"] = self.raft_advertise_host

        if raft_config:
            overrides["raft_config"] = raft_config

        # Log config section
        log_config = LogConfigHelper.build_log_config_overrides(
            self.log_level, self.log_dir, self.log_format
        )
        if log_config:
            overrides["log"] = log_config

        return overrides

    def to_cli_args(self) -> List[str]:
        """Convert CLI args to command line arguments for databend-meta."""
        cli_args = []

        if self.admin_api_address is not None:
            cli_args.extend(["--admin-api-address", self.admin_api_address])
        if self.grpc_api_address is not None:
            cli_args.extend(["--grpc-api-address", self.grpc_api_address])
        if self.grpc_api_advertise_host is not None:
            cli_args.extend(["--grpc-api-advertise-host", self.grpc_api_advertise_host])

        if self.id is not None:
            cli_args.extend(["--id", str(self.id)])
        if self.raft_dir is not None:
            cli_args.extend(["--raft-dir", self.raft_dir])
        if self.raft_api_port is not None:
            cli_args.extend(["--raft-api-port", str(self.raft_api_port)])
        if self.raft_listen_host is not None:
            cli_args.extend(["--raft-listen-host", self.raft_listen_host])
        if self.raft_advertise_host is not None:
            cli_args.extend(["--raft-advertise-host", self.raft_advertise_host])

        if self.single is not None:
            if self.single:
                cli_args.append("--single")
        if self.join is not None:
            for endpoint in self.join:
                cli_args.extend(["--join", endpoint])

        if self.log_level is not None:
            cli_args.extend(["--log-level", self.log_level])
        if self.log_dir is not None:
            cli_args.extend(["--log-dir", self.log_dir])
        if self.log_format is not None:
            cli_args.extend(["--log-format", self.log_format])

        return cli_args

    def clone(self) -> "MetaArgs":
        """Create a deep copy of this MetaArgs instance."""
        return ArgsUtils.clone_args(self)

    def merge(self, other: "MetaArgs") -> "MetaArgs":
        """Merge another MetaArgs into this one, replacing None values in-place."""
        return ArgsUtils.merge_args(self, other)


@dataclass
class QueryArgs:
    """CLI arguments for databend-query process."""

    # API configuration
    admin_api_address: Optional[str] = None
    metric_api_address: Optional[str] = None
    http_handler_host: Optional[str] = None
    http_handler_port: Optional[int] = None
    mysql_handler_host: Optional[str] = None
    mysql_handler_port: Optional[int] = None
    clickhouse_http_handler_host: Optional[str] = None
    clickhouse_http_handler_port: Optional[int] = None
    flight_sql_handler_host: Optional[str] = None
    flight_sql_handler_port: Optional[int] = None

    # Cluster configuration
    tenant_id: Optional[str] = None
    cluster_id: Optional[str] = None

    # Query configuration
    max_active_sessions: Optional[int] = None
    table_engine_memory_enabled: Optional[bool] = None

    # Meta configuration
    meta_endpoints: Optional[List[str]] = None
    meta_username: Optional[str] = None
    meta_password: Optional[str] = None

    # Storage configuration
    storage_type: Optional[str] = None
    storage_fs_data_path: Optional[str] = None

    # Log configuration
    log_level: Optional[str] = None
    log_dir: Optional[str] = None
    log_format: Optional[str] = None
    log_query_enabled: Optional[bool] = None

    def to_config_overrides(self) -> dict:
        """Convert CLI args to config file overrides."""
        overrides = {}

        # Query config section
        query_config = {}
        if self.admin_api_address is not None:
            query_config["admin_api_address"] = self.admin_api_address
        if self.metric_api_address is not None:
            query_config["metric_api_address"] = self.metric_api_address
        if self.http_handler_host is not None:
            query_config["http_handler_host"] = self.http_handler_host
        if self.http_handler_port is not None:
            query_config["http_handler_port"] = self.http_handler_port
        if self.mysql_handler_host is not None:
            query_config["mysql_handler_host"] = self.mysql_handler_host
        if self.mysql_handler_port is not None:
            query_config["mysql_handler_port"] = self.mysql_handler_port
        if self.clickhouse_http_handler_host is not None:
            query_config["clickhouse_http_handler_host"] = (
                self.clickhouse_http_handler_host
            )
        if self.clickhouse_http_handler_port is not None:
            query_config["clickhouse_http_handler_port"] = (
                self.clickhouse_http_handler_port
            )
        if self.flight_sql_handler_host is not None:
            query_config["flight_sql_handler_host"] = self.flight_sql_handler_host
        if self.flight_sql_handler_port is not None:
            query_config["flight_sql_handler_port"] = self.flight_sql_handler_port
        if self.tenant_id is not None:
            query_config["tenant_id"] = self.tenant_id
        if self.cluster_id is not None:
            query_config["cluster_id"] = self.cluster_id
        if self.max_active_sessions is not None:
            query_config["max_active_sessions"] = self.max_active_sessions
        if self.table_engine_memory_enabled is not None:
            query_config["table_engine_memory_enabled"] = (
                self.table_engine_memory_enabled
            )

        if query_config:
            overrides["query"] = query_config

        # Meta config section
        meta_config = {}
        if self.meta_endpoints is not None:
            meta_config["endpoints"] = self.meta_endpoints
        if self.meta_username is not None:
            meta_config["username"] = self.meta_username
        if self.meta_password is not None:
            meta_config["password"] = self.meta_password

        if meta_config:
            overrides["meta"] = meta_config

        # Storage config section
        storage_config = {}
        if self.storage_type is not None:
            storage_config["type"] = self.storage_type

        if self.storage_fs_data_path is not None:
            fs_config = {"data_path": self.storage_fs_data_path}
            storage_config["fs"] = fs_config

        if storage_config:
            overrides["storage"] = storage_config

        # Log config section
        log_config = LogConfigHelper.build_log_config_overrides(
            self.log_level, self.log_dir, self.log_format
        )
        if self.log_query_enabled is not None:
            log_config["query"] = {"on": self.log_query_enabled}

        if log_config:
            overrides["log"] = log_config

        return overrides

    def to_cli_args(self) -> List[str]:
        """Convert CLI args to command line arguments for databend-query."""
        cli_args = []

        if self.admin_api_address is not None:
            cli_args.extend(["--admin-api-address", self.admin_api_address])
        if self.metric_api_address is not None:
            cli_args.extend(["--metric-api-address", self.metric_api_address])

        if self.http_handler_host is not None:
            cli_args.extend(["--http-handler-host", self.http_handler_host])
        if self.http_handler_port is not None:
            cli_args.extend(["--http-handler-port", str(self.http_handler_port)])
        if self.mysql_handler_host is not None:
            cli_args.extend(["--mysql-handler-host", self.mysql_handler_host])
        if self.mysql_handler_port is not None:
            cli_args.extend(["--mysql-handler-port", str(self.mysql_handler_port)])
        if self.clickhouse_http_handler_host is not None:
            cli_args.extend(
                ["--clickhouse-http-handler-host", self.clickhouse_http_handler_host]
            )
        if self.clickhouse_http_handler_port is not None:
            cli_args.extend(
                [
                    "--clickhouse-http-handler-port",
                    str(self.clickhouse_http_handler_port),
                ]
            )
        if self.flight_sql_handler_host is not None:
            cli_args.extend(["--flight-sql-handler-host", self.flight_sql_handler_host])
        if self.flight_sql_handler_port is not None:
            cli_args.extend(
                ["--flight-sql-handler-port", str(self.flight_sql_handler_port)]
            )

        if self.tenant_id is not None:
            cli_args.extend(["--tenant-id", self.tenant_id])
        if self.cluster_id is not None:
            cli_args.extend(["--cluster-id", self.cluster_id])
        if self.max_active_sessions is not None:
            cli_args.extend(["--max-active-sessions", str(self.max_active_sessions)])
        if self.table_engine_memory_enabled is not None:
            cli_args.extend(
                [
                    "--table-engine-memory-enabled",
                    str(self.table_engine_memory_enabled).lower(),
                ]
            )

        if self.meta_endpoints is not None:
            for endpoint in self.meta_endpoints:
                cli_args.extend(["--meta-endpoint", endpoint])
        if self.meta_username is not None:
            cli_args.extend(["--meta-username", self.meta_username])
        if self.meta_password is not None:
            cli_args.extend(["--meta-password", self.meta_password])

        if self.storage_type is not None:
            cli_args.extend(["--storage-type", self.storage_type])
        if self.storage_fs_data_path is not None:
            cli_args.extend(["--storage-fs-data-path", self.storage_fs_data_path])

        if self.log_level is not None:
            cli_args.extend(["--log-level", self.log_level])
        if self.log_dir is not None:
            cli_args.extend(["--log-dir", self.log_dir])
        if self.log_format is not None:
            cli_args.extend(["--log-format", self.log_format])
        if self.log_query_enabled is not None:
            cli_args.extend(
                ["--log-query-enabled", str(self.log_query_enabled).lower()]
            )

        return cli_args

    def clone(self) -> "QueryArgs":
        """Create a deep copy of this QueryArgs instance."""
        return ArgsUtils.clone_args(self)

    def merge(self, other: "QueryArgs") -> "QueryArgs":
        """Merge another QueryArgs into this one, replacing None values in-place."""
        return ArgsUtils.merge_args(self, other)
