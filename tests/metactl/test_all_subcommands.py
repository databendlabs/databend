#!/usr/bin/env python3
"""
Comprehensive test runner for all metactl subcommands.

This script runs all subcommand tests in a logical sequence, ensuring proper
cleanup between tests and providing clear reporting for CI environments.

Usage:
    python tests/metactl/test_all_subcommands.py           # Run all tests
    python tests/metactl/test_all_subcommands.py lua       # Run only lua test
    python tests/metactl/test_all_subcommands.py status get # Run status and get tests
"""

import sys
import time
import argparse
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
from subcommands import cmd_lua


def cleanup_environment():
    """Clean up test environment completely."""
    print("🧹 Cleaning up test environment...")
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)
    time.sleep(3)  # Allow complete cleanup


def main():
    """Main test execution function."""
    parser = argparse.ArgumentParser(description="Run metactl subcommand tests")
    parser.add_argument("tests", nargs="*", help="Specific test names to run (default: all)")
    args = parser.parse_args()

    print_title("Metactl Subcommand Test Suite")

    passed_tests = 0
    failed_tests = []

    # All available test functions
    all_test_functions = [
        ("status", cmd_status.main),
        ("upsert", cmd_upsert.main),
        ("get", cmd_get.main),
        ("watch", cmd_watch.main),
        ("trigger_snapshot", cmd_trigger_snapshot.main),
        ("export_from_grpc", cmd_export_from_grpc.main),
        ("export_from_raft_dir", cmd_export_from_raft_dir.main),
        ("import", cmd_import.main),
        ("transfer_leader", cmd_transfer_leader.main),
        ("lua", cmd_lua.main),
    ]

    # Filter tests based on command line arguments
    if args.tests:
        # Create a mapping for easier lookup
        test_map = {name: func for name, func in all_test_functions}
        test_functions = []

        for test_name in args.tests:
            if test_name in test_map:
                test_functions.append((test_name, test_map[test_name]))
            else:
                available_tests = ", ".join(test_map.keys())
                print(f"❌ Unknown test '{test_name}'. Available tests: {available_tests}")
                return 1
    else:
        test_functions = all_test_functions

    total_tests = len(test_functions)
    print(f"📋 Executing {total_tests} test suite{'s' if total_tests != 1 else ''} sequentially...")
    print("=" * 60)

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
    print(f"📊 Total test suites: {total_tests}")
    print(f"✅ Passed: {passed_tests}")
    print(f"❌ Failed: {len(failed_tests)}")
    print(f"📈 Success rate: {(passed_tests/total_tests*100):.1f}%")

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
