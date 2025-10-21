#!/usr/bin/env python2
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
import os
import importlib
import glob
from utils import print_title, kill_databend_meta
import shutil


def cleanup_environment():
    """Clean up test environment completely."""
    print("🧹 Cleaning up test environment...")
    kill_databend_meta()
    shutil.rmtree(".databend", ignore_errors=True)
    time.sleep(0.5)  # Allow complete cleanup


def discover_test_modules():
    """Dynamically discover all test modules in subcommands directory."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    subcommands_dir = os.path.join(script_dir, "subcommands")

    # Find all cmd_*.py files
    cmd_files = glob.glob(os.path.join(subcommands_dir, "cmd_*.py"))

    test_modules = []
    for cmd_file in sorted(cmd_files):
        module_name = os.path.basename(cmd_file)[:-3]  # Remove .py extension
        test_name = module_name[4:]  # Remove 'cmd_' prefix

        # Import module dynamically
        module = importlib.import_module(f"subcommands.{module_name}")
        test_modules.append((test_name, module.main))

    return test_modules


def main():
    """Main test execution function."""
    parser = argparse.ArgumentParser(description="Run metactl subcommand tests")
    parser.add_argument(
        "tests", nargs="*", help="Specific test names to run (default: all)"
    )
    args = parser.parse_args()

    print_title("Metactl Subcommand Test Suite")

    passed_tests = 0
    failed_tests = []

    # Dynamically discover all test functions
    all_test_functions = discover_test_modules()

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
                print(
                    f"❌ Unknown test '{test_name}'. Available tests: {available_tests}"
                )
                return 1
    else:
        test_functions = all_test_functions

    total_tests = len(test_functions)
    print(
        f"📋 Executing {total_tests} test suite{'s' if total_tests != 1 else ''} sequentially..."
    )
    print("=" * 60)

    for name, test_func in test_functions:
        try:
            print(f"🔸 Running {name}")
            test_func()
            print(f"✅ {name} - PASSED")
            passed_tests += 1
        except Exception as e:
            print(f"❌ {name} - FAILED: {str(e)}")
            failed_tests.append(name)

        print("-" * 60)

    # Print summary
    print_title("Test Suite Summary")
    print(f"📊 Total test suites: {total_tests}")
    print(f"✅ Passed: {passed_tests}")
    print(f"❌ Failed: {len(failed_tests)}")
    print(f"📈 Success rate: {(passed_tests / total_tests * 100):.1f}%")

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
        sys.exit(130)
    except Exception as e:
        print(f"\n💥 Unexpected error: {str(e)}")
        sys.exit(1)
