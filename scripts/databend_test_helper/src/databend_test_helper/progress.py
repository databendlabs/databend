"""Progress reporting utilities for Databend processes."""

import sys


class ProgressReporter:
    """Utility class for reporting process startup/shutdown progress."""

    _enabled = True  # Global flag to enable/disable progress reporting

    @classmethod
    def set_enabled(cls, enabled: bool) -> None:
        """Enable or disable progress reporting globally."""
        cls._enabled = enabled

    @classmethod
    def is_enabled(cls) -> bool:
        """Check if progress reporting is enabled."""
        return cls._enabled

    @staticmethod
    def print_message(message: str) -> None:
        """Print a message to stderr if reporting is enabled."""
        if ProgressReporter._enabled:
            print(message, file=sys.stderr)

    @staticmethod
    def print_stop_info(service_type: str) -> None:
        """Print shutdown information to stderr."""
        ProgressReporter.print_message(f"🛑 Stopping {service_type}...")

    @staticmethod
    def print_ready_info(service_type: str, port: int) -> None:
        """Print ready information to stderr."""
        ProgressReporter.print_message(f"✅ {service_type} ready on port {port}")

    @staticmethod
    def print_error_info(service_type: str, error: str) -> None:
        """Print error information to stderr."""
        ProgressReporter.print_message(f"❌ {service_type} error: {error}")
