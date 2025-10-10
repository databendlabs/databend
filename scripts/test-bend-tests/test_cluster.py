#!/usr/bin/env python3
"""
Test script to start a Databend cluster with 3 meta nodes and 1 query node.
"""

import os
import sys
import time
import signal
from pathlib import Path

from databend_test_helper import MetaCluster, QueryCluster, ProgressReporter


def main():
    """Main test function."""
    script_dir = Path(__file__).parent
    config_dir = script_dir / "configs"

    # Check if configs exist
    if not config_dir.exists():
        ProgressReporter.print_message(f"❌ Config directory not found: {config_dir}")
        sys.exit(1)

    # Discover config files from subdirectories
    meta_config_dir = config_dir / "meta"
    query_config_dir = config_dir / "query"

    # Check if config subdirectories exist
    if not meta_config_dir.exists():
        ProgressReporter.print_message(
            f"❌ Meta config directory not found: {meta_config_dir}"
        )
        sys.exit(1)

    if not query_config_dir.exists():
        ProgressReporter.print_message(
            f"❌ Query config directory not found: {query_config_dir}"
        )
        sys.exit(1)

    # Find all .toml files in meta and query directories
    meta_configs = sorted([str(p) for p in meta_config_dir.glob("*.toml")])
    query_configs = sorted([str(p) for p in query_config_dir.glob("*.toml")])

    if not meta_configs:
        ProgressReporter.print_message(
            f"❌ No meta config files found in: {meta_config_dir}"
        )
        sys.exit(1)

    if not query_configs:
        ProgressReporter.print_message(
            f"❌ No query config files found in: {query_config_dir}"
        )
        sys.exit(1)

    ProgressReporter.print_message(f"📁 Found {len(meta_configs)} meta configs:")
    for config in meta_configs:
        ProgressReporter.print_message(f"   {Path(config).name}")

    ProgressReporter.print_message(f"📁 Found {len(query_configs)} query configs:")
    for config in query_configs:
        ProgressReporter.print_message(f"   {Path(config).name}")

    # Create clusters
    meta_cluster = MetaCluster(meta_configs, profile="debug")
    query_cluster = QueryCluster(query_configs, profile="debug")

    def signal_handler(signum, frame):
        ProgressReporter.print_message(
            f"\n🔄 Received signal {signum}, shutting down..."
        )
        query_cluster.stop()
        meta_cluster.stop()
        sys.exit(0)

    # Handle Ctrl+C gracefully
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Check if dry-run argument is passed
        dry_run = len(sys.argv) > 1 and sys.argv[1] == "--dry-run"

        # Start meta cluster first
        meta_cluster.start(dry_run=dry_run)

        if not dry_run:
            # Wait for meta cluster to be ready before starting query
            time.sleep(5.0)

        # Start query cluster
        query_cluster.start(dry_run=dry_run)

        if dry_run:
            ProgressReporter.print_message("🔍 Dry run completed!")
            return

        # Show status and connection info
        meta_status = meta_cluster.status()
        query_status = query_cluster.status()

        # Count running nodes - handle different status formats
        meta_running = sum(
            1 for node_info in meta_status.values() if node_info.get("running", False)
        )
        meta_total = len(meta_status)

        # QueryCluster.status() returns a different format with 'running' and 'total' keys
        query_running = query_status.get("running", 0)
        query_total = query_status.get("total", 0)

        ProgressReporter.print_message("📊 Cluster Status:")
        ProgressReporter.print_message(
            f"Meta nodes: {meta_running}/{meta_total} running"
        )
        ProgressReporter.print_message(
            f"Query nodes: {query_running}/{query_total} running"
        )

        # Show connection info for query nodes
        connections = query_cluster.get_connection_info()
        if connections:
            ProgressReporter.print_message("📝 Connection info:")
            for conn in connections:
                ProgressReporter.print_message(
                    f"   Node {conn['node']}: HTTP={conn['http_url']}, MySQL={conn['mysql_url']}"
                )

        ProgressReporter.print_message("\n🎉 Databend cluster is running!")
        ProgressReporter.print_message("📜 Logs are in: _databend_data/logs/")
        ProgressReporter.print_message("🔄 Press Ctrl+C to stop the cluster")

        # Keep the cluster running with periodic meta node restarts
        restart_counter = 0
        restart_interval = 20  # seconds
        node_ids = list(meta_cluster.nodes.keys())
        current_node_index = 0

        while True:
            time.sleep(1)
            restart_counter += 1

            # Restart a meta node every 5 seconds
            if restart_counter >= restart_interval:
                if node_ids:
                    node_id = node_ids[current_node_index]
                    meta_cluster.restart_node(node_id)
                    current_node_index = (current_node_index + 1) % len(node_ids)
                restart_counter = 0

            # Check if cluster is still healthy (every 10 seconds)
            if restart_counter % 10 == 0:
                meta_running = meta_cluster.is_running()
                query_running = query_cluster.is_running()
                if not (meta_running and query_running):
                    ProgressReporter.print_message(
                        "❌ Some processes have stopped unexpectedly!"
                    )
                    ProgressReporter.print_message(
                        f"Meta cluster running: {meta_running}"
                    )
                    ProgressReporter.print_message(
                        f"Query cluster running: {query_running}"
                    )
                    break

    finally:
        query_cluster.stop()
        meta_cluster.stop()


if __name__ == "__main__":
    main()
