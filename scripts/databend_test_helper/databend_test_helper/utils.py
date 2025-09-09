"""Common utilities for Databend services."""

import os
import subprocess
import socket
import time
import toml
from typing import Optional, Dict, Any


class PortDetector:

    @staticmethod
    def ping_tcp(service_name: str, port: int, timeout: int = 10) -> None:
        """Wait for a port to become available"""
        now = time.time()

        while time.time() - now < timeout:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect(("0.0.0.0", port))
                    print("OK :{} is listening".format(port))
                    return
            except Exception as e:
                print(e)
                print("... connecting to :{}".format(port))
                time.sleep(0.3)
            
        raise TimeoutError(f"{service_name} did not start on port {port} within {timeout} seconds")
    
        

class BinaryFinder:
    """Utility for finding Databend binaries."""
    
    @staticmethod
    def find_binary(service_name: str, profile: Optional[str] = None) -> str:
        """Find databend binary in common locations."""
        if profile and profile not in ["debug", "release"]:
            raise ValueError("profile must be 'debug' or 'release'")
            
        base_dirs = [".", "..", "../.."]
        profiles = [profile] if profile else ["debug", "release"]
        
        possible_paths = []
        for base_dir in base_dirs:
            for prof in profiles:
                possible_paths.append(f"{base_dir}/target/{prof}/databend-{service_name}")
        
        for path in possible_paths:
            if os.path.isfile(path):
                return path
                
        profile_msg = f" (profile: {profile})" if profile else ""
        raise FileNotFoundError(f"databend-{service_name} binary not found{profile_msg}")


class ConfigManager:
    """Utility for managing configuration files."""
    
    @staticmethod
    def get_default_config_path(service_name: str) -> str:
        """Get default config path for a service."""
        return os.path.join(os.path.dirname(__file__), "configs", f"databend-{service_name}.toml")
    
    @staticmethod
    def parse_config(config_path: str, args_overrides: Optional[Dict[str, Any]] = None) -> dict:
        """Parse config file to extract settings, applying CLI overrides."""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        config = toml.load(config_path)
        
        # Apply CLI argument overrides
        if args_overrides:
            config = ConfigManager._merge_config(config, args_overrides)
            
        return config
    
    @staticmethod
    def _merge_config(base_config: dict, overrides: dict) -> dict:
        """Deep merge CLI overrides into base config."""
        result = base_config.copy()
        
        for key, value in overrides.items():
            if isinstance(value, dict) and key in result and isinstance(result[key], dict):
                result[key] = ConfigManager._merge_config(result[key], value)
            else:
                result[key] = value
                
        return result


class ProcessManager:
    """Utility for managing subprocess lifecycle."""
    
    @staticmethod
    def start_process(cmd: list, service_name: str, log_dir: str = None) -> subprocess.Popen:
        """Start a subprocess with common configuration."""
        import os
        from .progress import ProgressReporter
        
        # Print the command being executed
        ProgressReporter.print_message(f"🔧 Executing: {' '.join(cmd)}")
        
        os.makedirs(log_dir, exist_ok=True)
            
        # Open log files for stdout and stderr
        stdout_file = open(f"{log_dir}/{service_name}_stdout.log", "w")
        stderr_file = open(f"{log_dir}/{service_name}_stderr.log", "w")
        
        ProgressReporter.print_message(f"📝 Logs: stdout={log_dir}/stdout.log, stderr={log_dir}/stderr.log")
        
        return subprocess.Popen(
            cmd,
            stdout=stdout_file,
            stderr=stderr_file,
            text=True
        )
    
    @staticmethod
    def stop_process(process: subprocess.Popen, service_name: str) -> None:
        """Stop a subprocess gracefully."""
        if process is None:
            return
            
        from .progress import ProgressReporter
        ProgressReporter.print_stop_info(f"databend-{service_name}")
        
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    
    @staticmethod
    def is_process_running(process: Optional[subprocess.Popen]) -> bool:
        """Check if process is running."""
        return process is not None and process.poll() is None


class CommandBuilder:
    """Utility for building command lines."""
    
    @staticmethod
    def build_command(binary_path: str, config_path: str, args=None) -> list:
        """Build command line for starting a service."""
        cmd = [binary_path, "--config-file", config_path]
        if args:
            cmd.extend(args.to_cli_args())
        return cmd


class LogConfigHelper:
    """Helper for common log configuration processing."""
    
    @staticmethod
    def print_log_config(config: dict) -> None:
        """Print log configuration information."""
        from .progress import ProgressReporter
        
        log_config = config.get("log", {})
        file_config = log_config.get("file", {})
        if file_config and file_config.get("on"):
            log_dir = file_config.get("dir", "unknown")
            log_level = file_config.get("level", "INFO")
            ProgressReporter.print_message(f"   Logs: {log_dir} ({log_level})")
    
    @staticmethod
    def build_log_config_overrides(log_level: Optional[str] = None, 
                                 log_dir: Optional[str] = None,
                                 log_format: Optional[str] = None) -> Dict[str, Any]:
        """Build log config section from parameters."""
        log_config = {}
        if log_level or log_dir or log_format:
            file_config = {}
            if log_level is not None:
                file_config["level"] = log_level
            if log_dir is not None:
                file_config["dir"] = log_dir
            if log_format is not None:
                file_config["format"] = log_format
            if file_config:
                file_config["on"] = True
                log_config["file"] = file_config
        return log_config
