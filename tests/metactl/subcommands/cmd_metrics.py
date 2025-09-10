#!/usr/bin/env python3

import re
import shutil
from metactl_utils import metactl_bin
from utils import run_command, kill_databend_meta, start_meta_node, print_title


def verify_metrics_format(result):
    """Verify the metrics output format (Prometheus format)."""
    lines = result.strip().split("\n")

    # Prometheus metrics should have specific patterns
    metric_patterns = [
        r"# HELP .+ .+",  # Help comments
        r"# TYPE .+ (counter|gauge|histogram)",  # Type comments
        r"metasrv_server_.+ \d+(\.\d+)?",  # Server metrics
        r"metasrv_raft_.+ \d+(\.\d+)?",  # Raft metrics
    ]

    help_lines = [line for line in lines if line.startswith("# HELP")]
    type_lines = [line for line in lines if line.startswith("# TYPE")]
    metric_lines = [line for line in lines if not line.startswith("#") and line.strip()]

    print(f"Found {len(help_lines)} help lines")
    print(f"Found {len(type_lines)} type lines")
    print(f"Found {len(metric_lines)} metric lines")

    # Should have at least some metrics
    assert len(metric_lines) > 0, "Should have at least some metric lines"

    # Check for some expected metrics
    expected_metrics = [
        "metasrv_server_current_leader_id",
        "metasrv_server_is_leader",
        "metasrv_server_node_is_health",
    ]

    metrics_text = result
    found_metrics = []
    for metric in expected_metrics:
        if metric in metrics_text:
            found_metrics.append(metric)
            print(f"✓ Found expected metric: {metric}")

    assert len(found_metrics) > 0, (
        f"Should find at least some expected metrics. Found: {found_metrics}"
    )

    # Verify at least some lines match prometheus format (metric_name value)
    prometheus_pattern = r"^[a-zA-Z_:][a-zA-Z0-9_:]* \d+(\.\d+)?$"
    matching_lines = [
        line for line in metric_lines if re.match(prometheus_pattern, line)
    ]

    print(f"Found {len(matching_lines)} lines matching Prometheus format")
    if matching_lines:
        print(f"Example metric line: {matching_lines[0]}")

    assert len(matching_lines) > 0, "Should have lines matching Prometheus format"

    print("✓ Metrics format verification passed")


def test_metrics_subcommand():
    """Test metrics subcommand functionality."""
    print_title("Test metrics subcommand")
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)
    start_meta_node(1, False)

    admin_addr = "127.0.0.1:28101"

    # Test metrics command
    result = run_command([metactl_bin, "metrics", "--admin-api-address", admin_addr])

    # Verify metrics output contains data
    assert result, "Metrics should return data"
    assert len(result.strip()) > 0, "Metrics should produce output"

    print("Raw metrics output (first 500 chars):")
    print(result[:500])
    print("...")

    # Format verification
    verify_metrics_format(result)

    print("✓ Metrics subcommand test passed")

    # Clean up only on success
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)


def main():
    """Main function to run all metrics tests."""
    test_metrics_subcommand()


if __name__ == "__main__":
    main()
