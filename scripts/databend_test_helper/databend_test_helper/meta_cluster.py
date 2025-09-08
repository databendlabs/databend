"""Meta cluster management utilities for Databend processes."""

import time
from typing import List, Optional, Dict, Any

from .meta import DatabendMeta
from .progress import ProgressReporter
from .args import MetaArgs


class MetaCluster:
    """Manager for a cluster of databend-meta processes."""
    
    def __init__(self, config_paths: List[str], binary_path: Optional[str] = None, profile: Optional[str] = None):
        """
        Initialize meta cluster.
        
        Args:
            config_paths: List of config file paths for each meta node
            binary_path: Optional path to databend-meta binary
            profile: Optional build profile (debug/release)
        """
        if not config_paths:
            raise ValueError("At least one config path must be provided")
            
        # Create nodes first as a dict indexed by node_id
        self.nodes = {}
        for config_path in config_paths:
            node = DatabendMeta(binary_path=binary_path, config_path=config_path, profile=profile)
            raft_config = node.get_raft_config()
            node_id = raft_config.get("id")
            if node_id is None:
                raise ValueError(f"Node config {config_path} missing raft_config.id")
            if node_id in self.nodes:
                raise ValueError(f"Duplicate node ID {node_id} in config {config_path}")
            self.nodes[node_id] = node
        
        # Extract join endpoints from all node configs
        join_endpoints = self._extract_join_endpoints()
        
        # Configure cluster-specific args for each node
        sorted_node_ids = sorted(self.nodes.keys())
        for i, node_id in enumerate(sorted_node_ids):
            node = self.nodes[node_id]
            if i == 0:
                # First node: start as single node
                bootstrap_args = MetaArgs(single=True)
                if node.args is None:
                    node.args = bootstrap_args
                else:
                    node.args.merge(bootstrap_args)
            else:
                # Subsequent nodes: join the existing cluster
                join_args = MetaArgs(join=join_endpoints)
                if node.args is None:
                    node.args = join_args
                else:
                    node.args.merge(join_args)
    
    def start(self, delay_between_nodes: float = 0.5, dry_run: bool = False) -> None:
        """
        Start all meta nodes sequentially.
        
        The first node starts as a single node (--single), and subsequent nodes
        join the existing cluster (--join).
        
        Args:
            delay_between_nodes: Delay in seconds between starting nodes
            dry_run: If True, only print cluster startup information without starting
        """
        if dry_run:
            ProgressReporter.print_message("🔍 Meta Cluster Startup Plan:")
            ProgressReporter.print_message(f"   Total nodes: {len(self.nodes)}")
            ProgressReporter.print_message(f"   Delay between nodes: {delay_between_nodes}s")
            ProgressReporter.print_message("")
        else:
            ProgressReporter.print_message("🚀 Starting Meta Cluster...")
        
        # Start all nodes with their pre-configured args
        sorted_node_ids = sorted(self.nodes.keys())
        for i, node_id in enumerate(sorted_node_ids):
            node = self.nodes[node_id]
            if not dry_run:
                ProgressReporter.print_message(f"Starting meta node {node_id}...")
            else:
                ProgressReporter.print_message(f"Node {node_id}:")
                
            node.start(dry_run=dry_run)
            
            if i < len(sorted_node_ids) - 1:  # Don't delay after last node
                if not dry_run:
                    time.sleep(delay_between_nodes)
                    
        if not dry_run:
            ProgressReporter.print_message("✅ Meta cluster started successfully!")
    
    def stop(self) -> None:
        """Stop all meta nodes."""
        ProgressReporter.print_message("🛑 Stopping Meta Cluster...")
        
        for node in self.nodes.values():
            if node.is_running():
                node.stop()
                
        ProgressReporter.print_message("✅ Meta cluster stopped")
    
    def status(self) -> Dict[int, Dict[str, Any]]:
        """Get cluster status information indexed by node ID."""
        status_dict = {}
        
        for node_id, node in self.nodes.items():
            node_config = node._parse_config()
            raft_config = node.get_raft_config()
            log_config = node_config.get("log", {})
            file_log_config = log_config.get("file", {})
                
            status_dict[node_id] = {
                "running": node.is_running(),
                "config_path": node.config_path,
                "grpc_api_address": node_config.get("grpc_api_address"),
                "admin_api_address": node_config.get("admin_api_address"),
                "raft_api_port": raft_config.get("raft_api_port"),
                "raft_dir": raft_config.get("raft_dir"),
                "log_dir": file_log_config.get("dir") if file_log_config.get("on") else None,
                "log_level": file_log_config.get("level") if file_log_config.get("on") else None
            }
        
        return status_dict
    
    def is_running(self) -> bool:
        """Check if all meta nodes are running."""
        return all(node.is_running() for node in self.nodes.values())
    
    def restart_node(self, node_id: int) -> None:
        """
        Restart a specific meta node by ID.
        
        Args:
            node_id: ID of the node to restart
        """
        if node_id not in self.nodes:
            raise ValueError(f"Node with ID {node_id} not found")
            
        node = self.nodes[node_id]
        if node.is_running():
            ProgressReporter.print_message(f"🔄 Stopping meta node {node_id}...")
            node.stop()
            
        ProgressReporter.print_message(f"🚀 Starting meta node {node_id}...")
        node.start()
        
        ProgressReporter.print_message(f"✅ Meta node {node_id} restarted successfully")
    
    def _extract_join_endpoints(self) -> List[str]:
        """Build join endpoint from the first node (bootstrap node)."""
        if not self.nodes:
            return []
            
        # Only use the first node (lowest ID) as the join endpoint
        first_node_id = min(self.nodes.keys())
        first_node = self.nodes[first_node_id]
        raft_config = first_node.get_raft_config()
        
        host = raft_config.get("raft_advertise_host")
        if not host:
            raise ValueError("First node config missing raft_advertise_host")
            
        port = raft_config.get("raft_api_port")
        if not port:
            raise ValueError("First node config missing raft_api_port")
        
        return [f"{host}:{port}"]
    
    
