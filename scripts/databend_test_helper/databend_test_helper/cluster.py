"""Complete Databend cluster management utilities."""

import time
from typing import List, Optional, Dict, Any

from .meta_cluster import MetaCluster
from .query_cluster import QueryCluster
from .progress import ProgressReporter


class DatabendCluster:
    """Manager for a complete Databend cluster with meta and query nodes."""

    def __init__(
        self,
        meta_configs: List[str],
        query_configs: List[str],
        meta_binary: Optional[str] = None,
        query_binary: Optional[str] = None,
        profile: Optional[str] = None,
    ):
        """
        Initialize complete Databend cluster.

        Args:
            meta_configs: List of meta config file paths
            query_configs: List of query config file paths
            meta_binary: Optional path to databend-meta binary
            query_binary: Optional path to databend-query binary
            profile: Optional build profile (debug/release)
        """
        self.meta_cluster = MetaCluster(meta_configs, meta_binary, profile)
        self.query_cluster = QueryCluster(query_configs, query_binary, profile)

    def start(
        self, meta_delay: float = 2.0, cluster_delay: float = 5.0, dry_run: bool = False
    ) -> None:
        """
        Start the complete cluster.

        Args:
            meta_delay: Delay between starting meta nodes
            cluster_delay: Delay between starting meta cluster and query cluster
            dry_run: If True, only print startup information without starting
        """
        # Start meta cluster first
        self.meta_cluster.start(delay_between_nodes=meta_delay, dry_run=dry_run)

        if dry_run:
            ProgressReporter.print_message(
                f"🔄 Would wait {cluster_delay}s between meta and query clusters"
            )
        else:
            # Wait for meta cluster to be ready
            time.sleep(cluster_delay)

        # Start query cluster
        self.query_cluster.start(dry_run=dry_run)

    def stop(self) -> None:
        """Stop the complete cluster (query nodes first, then meta nodes)."""
        self.query_cluster.stop()
        self.meta_cluster.stop()

    def is_running(self) -> bool:
        """Check if the complete cluster is running."""
        return self.meta_cluster.is_running() and self.query_cluster.is_running()

    def status(self) -> Dict[str, Any]:
        """Get complete cluster status."""
        return {
            "meta": self.meta_cluster.status(),
            "query": self.query_cluster.status(),
            "all_running": self.is_running(),
        }

    def print_status(self) -> None:
        """Print cluster status to stderr."""
        meta_status = self.meta_cluster.status()
        query_status = self.query_cluster.status()

        ProgressReporter.print_message("📊 Cluster Status:")
        ProgressReporter.print_message(
            f"Meta nodes: {meta_status['running']}/{meta_status['total']} running"
        )
        ProgressReporter.print_message(
            f"Query nodes: {query_status['running']}/{query_status['total']} running"
        )

        # Show connection info for query nodes
        connections = self.query_cluster.get_connection_info()
        if connections:
            ProgressReporter.print_message("📝 Connection info:")
            for conn in connections:
                ProgressReporter.print_message(
                    f"   Node {conn['node']}: HTTP={conn['http_url']}, MySQL={conn['mysql_url']}"
                )
