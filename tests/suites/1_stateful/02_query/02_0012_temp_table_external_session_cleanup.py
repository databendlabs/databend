#!/usr/bin/env python3

import os
import sys
import mysql.connector
import time
import shutil
import glob

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "../../../helpers"))

from native_client import NativeClient
from native_client import prompt


def cleanup_test_directory(test_dir):
    """Clean up test directory"""
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)


def count_files_in_directory(test_dir, pattern="*"):
    """Count files matching pattern in directory"""
    if not os.path.exists(test_dir):
        return 0, []
    files = glob.glob(os.path.join(test_dir, "**", pattern), recursive=True)
    # Filter out directories and verification files
    files = [
        f
        for f in files
        if os.path.isfile(f)
        and not os.path.basename(f).startswith("_v_d77aa11285c22e0e1d4593a035c98c0d")
    ]
    return len(files), files


def test_external_temp_table_cleanup():
    """Test cleanup of external location temp tables when session ends"""

    # Test directory
    test_dir = "/tmp/databend_test_session_cleanup"

    print("====== Testing External Temp Table Session Cleanup ======")

    try:
        # Clean up any existing test directory
        cleanup_test_directory(test_dir)

        # Create test directory
        os.makedirs(test_dir, exist_ok=True)

        # Create temp table in session and verify cleanup
        print("-- Create external temp table")

        # Session: Create temp table with external location
        session_db = mysql.connector.connect(
            host="127.0.0.1", user="root", passwd="root", port="3307"
        )
        session_cursor = session_db.cursor()

        # Create temp table with external location
        session_cursor.execute(
            f"CREATE OR REPLACE TEMP TABLE temp_external (id INT, data STRING) 'fs://{test_dir}/';"
        )
        session_cursor.fetchall()

        # Insert some data
        session_cursor.execute(
            "INSERT INTO temp_external VALUES (1, 'session'), (2, 'test');"
        )
        session_cursor.fetchall()

        # Verify data exists
        session_cursor.execute("SELECT COUNT(*) FROM temp_external;")
        result = session_cursor.fetchone()
        print(f"Records in temp_external: {result[0]}")

        # Check that files were created in external location
        files_before_count, files_before_list = count_files_in_directory(test_dir)
        print(f"Files created in external location: {files_before_count}")

        # Close session to trigger cleanup
        session_cursor.close()
        session_db.close()

        # Wait for potential cleanup
        time.sleep(2)

        # Check if files were cleaned up
        files_after_count, files_after_list = count_files_in_directory(test_dir)
        print(f"Files after session ended: {files_after_count}")

        # If files still exist after cleanup, print their names
        if files_after_count > 0:
            print("Files remaining after cleanup:")
            for file_path in files_after_list:
                print(f"  {file_path}")

        # Test result
        test_passed = files_before_count > 0 and files_after_count == 0

        if test_passed:
            print("✓ External temp table cleanup test PASSED")
            return True
        else:
            print("✗ External temp table cleanup test FAILED")
            return False

    except Exception as e:
        print(f"✗ Test failed with exception: {e}")
        return False

    finally:
        # Clean up test directory
        cleanup_test_directory(test_dir)


if __name__ == "__main__":
    success = test_external_temp_table_cleanup()
    sys.exit(0 if success else 1)
