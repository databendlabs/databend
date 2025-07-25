#!/usr/bin/env python3
"""
Comprehensive test runner for all metactl subcommands.

This script runs all subcommand tests in a logical sequence, ensuring proper
cleanup between tests and providing clear reporting for CI environments.

Usage:
    python tests/metactl/test_all_subcommands.py           # Run all tests
"""

import sys
import time
from utils import print_title, kill_databend_meta
import shutil

# Direct imports of all test modules
from subcommands import cmd_status
from subcommands import cmd_upsert
from subcommands import cmd_get
from subcommands import cmd_watch
from subcommands import cmd_trigger_snapshot
from subcommands import cmd_export_from_grpc
from subcommands import cmd_export_from_raft_dir
from subcommands import cmd_import
from subcommands import cmd_transfer_leader


def cleanup_environment():
    """Clean up test environment completely."""
    print("🧹 Cleaning up test environment...")
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)
    time.sleep(3)  # Allow complete cleanup


def main():
    """Main test execution function."""

    print_title("Metactl Subcommand Test Suite")

    passed_tests = 0
    failed_tests = []

    print("📋 Executing 9 test suites sequentially...")
    print("=" * 60)

    # Run each test directly
    test_functions = [
        ("status", cmd_status.main),
        ("upsert", cmd_upsert.main),
        ("get", cmd_get.main),
        ("watch", cmd_watch.main),
        ("trigger_snapshot", cmd_trigger_snapshot.main),
        ("export_from_grpc", cmd_export_from_grpc.main),
        ("export_from_raft_dir", cmd_export_from_raft_dir.main),
        ("import", cmd_import.main),
        ("transfer_leader", cmd_transfer_leader.main),
    ]

    for name, test_func in test_functions:
        cleanup_environment()

        try:
            print(f"🔸 Running {name}")
            test_func()
            print(f"✅ {name} - PASSED")
            passed_tests += 1
        except Exception as e:
            print(f"❌ {name} - FAILED: {str(e)}")
            failed_tests.append(name)

        print("-" * 60)

    # Final cleanup
    cleanup_environment()

    # Print summary
    print_title("Test Suite Summary")
    print(f"📊 Total test suites: 9")
    print(f"✅ Passed: {passed_tests}")
    print(f"❌ Failed: {len(failed_tests)}")
    print(f"📈 Success rate: {(passed_tests/9*100):.1f}%")

    if failed_tests:
        print(f"\n🚨 Failed test suites:")
        for failed_test in failed_tests:
            print(f"   • {failed_test}")
        return 1
    else:
        print(f"\n🎉 All {passed_tests} test suites passed successfully!")
        return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print(f"\n⏹️  Test execution interrupted by user")
        cleanup_environment()
        sys.exit(130)
    except Exception as e:
        print(f"\n💥 Unexpected error: {str(e)}")
        cleanup_environment()
        sys.exit(1)
