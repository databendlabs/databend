"""Query cluster management utilities for Databend processes."""

import time
from typing import List, Optional, Dict, Any

from .query import DatabendQuery
from .progress import ProgressReporter


class QueryCluster:
    """Manager for a cluster of databend-query processes."""
    
    def __init__(self, config_paths: List[str], binary_path: Optional[str] = None, profile: Optional[str] = None):
        """
        Initialize query cluster.
        
        Args:
            config_paths: List of config file paths for each query node
            binary_path: Optional path to databend-query binary
            profile: Optional build profile (debug/release)
        """
        if not config_paths:
            raise ValueError("At least one config path must be provided")
            
        self.nodes = []
        for (i, config_path) in enumerate(config_paths):
            node_id = i + 1
            node = DatabendQuery(binary_path=binary_path, config_path=config_path, profile=profile, node_id=f"{node_id}")
            self.nodes.append(node)
    
    def start(self, sequential: bool = True, delay_between_nodes: float = 2.0, dry_run: bool = False) -> None:
        """
        Start all query nodes.
        
        Args:
            sequential: If True, start nodes one by one. If False, start all at once.
            delay_between_nodes: Delay in seconds between starting nodes (only for sequential)
            dry_run: If True, only print startup information without starting
        """
        if dry_run:
            ProgressReporter.print_message("🔍 Query Cluster Startup Plan:")
            ProgressReporter.print_message(f"   Total nodes: {len(self.nodes)}")
            ProgressReporter.print_message(f"   Sequential: {sequential}")
            if sequential:
                ProgressReporter.print_message(f"   Delay between nodes: {delay_between_nodes}s")
            ProgressReporter.print_message("")
        else:
            ProgressReporter.print_message("🚀 Starting Query Cluster...")
        
        if sequential:
            for i, node in enumerate(self.nodes):
                if not dry_run:
                    ProgressReporter.print_message(f"Starting query node {i+1}...")
                else:
                    ProgressReporter.print_message(f"Node {i+1}:")
                    
                node.start(dry_run=dry_run)
                if i < len(self.nodes) - 1:  # Don't delay after last node
                    if not dry_run:
                        time.sleep(delay_between_nodes)
        else:
            for i, node in enumerate(self.nodes):
                if dry_run:
                    ProgressReporter.print_message(f"Node {i+1}:")
                node.start(dry_run=dry_run)
                
        if not dry_run:
            ProgressReporter.print_message("✅ Query cluster started successfully!")
    
    def stop(self) -> None:
        """Stop all query nodes."""
        ProgressReporter.print_message("🛑 Stopping Query Cluster...")
        
        for node in self.nodes:
            if node.is_running():
                node.stop()
                
        ProgressReporter.print_message("✅ Query cluster stopped")
    
    def is_running(self) -> bool:
        """Check if all query nodes are running."""
        return all(node.is_running() for node in self.nodes)
    
    def running_count(self) -> int:
        """Get count of running query nodes."""
        return sum(1 for node in self.nodes if node.is_running())
    
    def total_count(self) -> int:
        """Get total count of query nodes."""
        return len(self.nodes)
    
    def status(self) -> Dict[str, Any]:
        """Get cluster status information."""
        return {
            "running": self.running_count(),
            "total": self.total_count(),
            "all_running": self.is_running(),
            "nodes": [
                {
                    "index": i,
                    "config": node.config_path,
                    "running": node.is_running()
                }
                for i, node in enumerate(self.nodes)
            ]
        }
    
    def get_connection_info(self) -> List[Dict[str, Any]]:
        """Get connection information for all running query nodes."""
        connections = []
        for i, node in enumerate(self.nodes):
            if node.is_running():
                try:
                    config = node._parse_config()
                    query_config = config.get("query", {})
                    
                    http_host = query_config.get("http_handler_host", "0.0.0.0")
                    http_port = query_config.get("http_handler_port", 8000)
                    mysql_host = query_config.get("mysql_handler_host", "0.0.0.0") 
                    mysql_port = query_config.get("mysql_handler_port", 3307)
                    
                    connections.append({
                        "node": i + 1,
                        "http_url": f"http://{http_host}:{http_port}",
                        "mysql_url": f"mysql://root@{mysql_host}:{mysql_port}",
                        "http_port": http_port,
                        "mysql_port": mysql_port
                    })
                except Exception:
                    # Skip nodes that can't be parsed
                    continue
                    
        return connections
