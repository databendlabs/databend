#!/usr/bin/env python3

import sys
import os
import tarfile
import argparse
import subprocess
from datetime import datetime
from pathlib import Path


def ensure_tomli_available():
    """Ensure tomli is available, install if needed."""
    print("=== Checking dependencies ===")
    print(f"Python executable: {sys.executable}")
    print(f"Python version: {sys.version}")

    try:
        import tomli

        print("✓ tomli already available")
        return tomli
    except ImportError:
        print("✗ tomli not found, installing...")
        try:
            print(f"Installing tomli using: {sys.executable} -m pip install tomli")
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "tomli"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            print("✓ tomli installed successfully")
            import tomli

            print("✓ tomli imported successfully")
            return tomli
        except subprocess.CalledProcessError as e:
            print(f"✗ Failed to install tomli: {e}")
            print("Please install manually: pip install tomli")
            sys.exit(1)
        except ImportError:
            print("✗ Failed to import tomli after installation")
            sys.exit(1)


# Install tomli at startup
print("Databend Meta Log Collector")
print("============================")
tomli = ensure_tomli_available()
print()


def parse_config(config_file):
    """Parse TOML config file and extract log directory."""
    try:
        with open(config_file, "rb") as f:
            config = tomli.load(f)

        # Check for log directory in two possible locations:
        # 1. Top-level: log_dir = "..."
        # 2. In [log.file] section: dir = "..."

        top_level_log_dir = config.get("log_dir")

        log_config = config.get("log", {})
        file_config = log_config.get("file", {})
        nested_log_dir = file_config.get("dir")

        print(f"Top-level log_dir: {top_level_log_dir}")
        print(f"[log.file].dir: {nested_log_dir}")

        # Validate configuration
        if top_level_log_dir and nested_log_dir:
            if top_level_log_dir != nested_log_dir:
                raise ValueError(
                    f"Conflicting log directory settings found:\n"
                    f"  log_dir = '{top_level_log_dir}'\n"
                    f"  [log.file].dir = '{nested_log_dir}'\n"
                    f"Please use only one log directory configuration."
                )
            print("✓ Both log directory settings present and match")
            return top_level_log_dir
        elif top_level_log_dir:
            print("✓ Using top-level log_dir setting")
            return top_level_log_dir
        elif nested_log_dir:
            print("✓ Using [log.file].dir setting")
            return nested_log_dir
        else:
            raise ValueError(
                "No log directory found in config file. "
                "Please set either 'log_dir' or '[log.file].dir'"
            )

    except FileNotFoundError:
        raise FileNotFoundError(f"Config file '{config_file}' not found")
    except Exception as e:
        raise ValueError(f"Error parsing config file: {e}")


def resolve_log_dir(log_dir, config_file):
    """Resolve log directory path (handle relative paths)."""
    log_path = Path(log_dir)

    if not log_path.is_absolute():
        # Resolve relative to config file directory
        config_dir = Path(config_file).parent
        log_path = config_dir / log_path

    return log_path.resolve()


def analyze_log_directory(log_dir):
    """Analyze log directory and return detailed information."""
    print(f"=== Analyzing log directory ===")
    print(f"Log directory path: {log_dir}")
    print(f"Directory exists: {log_dir.exists()}")

    if not log_dir.exists():
        print("✗ Log directory does not exist")
        return None

    if not log_dir.is_dir():
        print("✗ Path is not a directory")
        return None

    print(f"Directory readable: {os.access(log_dir, os.R_OK)}")

    # Get directory contents
    try:
        all_items = list(log_dir.iterdir())
        files = [f for f in all_items if f.is_file()]
        dirs = [d for d in all_items if d.is_dir()]

        print(f"Total items: {len(all_items)}")
        print(f"Files: {len(files)}")
        print(f"Subdirectories: {len(dirs)}")

        if files:
            print("Files found:")
            for f in files[:10]:  # Show first 10 files
                size = f.stat().st_size
                mtime = datetime.fromtimestamp(f.stat().st_mtime).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                print(f"  - {f.name} ({size} bytes, modified: {mtime})")
            if len(files) > 10:
                print(f"  ... and {len(files) - 10} more files")

        if dirs:
            print("Subdirectories found:")
            for d in dirs[:5]:  # Show first 5 directories
                print(f"  - {d.name}/")
            if len(dirs) > 5:
                print(f"  ... and {len(dirs) - 5} more directories")

        return {"files": files, "dirs": dirs, "total": len(all_items)}

    except PermissionError:
        print("✗ Permission denied accessing directory")
        return None
    except Exception as e:
        print(f"✗ Error accessing directory: {e}")
        return None


def create_log_archive(log_dir, output_file):
    """Create tar.gz archive of all files in log directory."""
    analysis = analyze_log_directory(log_dir)
    if analysis is None:
        raise FileNotFoundError(f"Cannot access log directory '{log_dir}'")

    print(f"\n=== Creating archive ===")
    print(f"Output file: {output_file}")

    files = analysis["files"]
    dirs = analysis["dirs"]

    with tarfile.open(output_file, "w:gz") as tar:
        if files or dirs:
            for file_path in files:
                print(f"Adding file: {file_path.name}")
                tar.add(file_path, arcname=file_path.name)
            for dir_path in dirs:
                print(f"Adding directory: {dir_path.name}/")
                tar.add(dir_path, arcname=dir_path.name)
        else:
            print("No files to archive")

    return analysis["total"]


def main():
    parser = argparse.ArgumentParser(
        description="Collect databend-meta logs based on config file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Example: %(prog)s databend-meta-node-1.toml",
    )
    parser.add_argument("config_file", help="Path to databend-meta config file (.toml)")
    parser.add_argument(
        "-o", "--output", help="Output archive filename (default: auto-generated)"
    )

    args = parser.parse_args()

    try:
        # Parse config and extract log directory
        print(f"=== Processing config file ===")
        config_abs_path = Path(args.config_file).resolve()
        print(f"Config file absolute path: {config_abs_path}")
        print(f"Config file exists: {config_abs_path.exists()}")
        print(f"Config file readable: {os.access(config_abs_path, os.R_OK)}")

        log_dir_str = parse_config(config_abs_path)
        print(f"Found log directory setting: '{log_dir_str}'")

        # Resolve log directory path
        log_dir = resolve_log_dir(log_dir_str, config_abs_path)
        print(f"Resolved log directory: {log_dir}")

        # Generate output filename if not provided
        print(f"\n=== Preparing output ===")
        if args.output:
            output_file = args.output
            print(f"Using provided output filename: {output_file}")
        else:
            config_name = Path(args.config_file).stem
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"{config_name}_logs_{timestamp}.tar.gz"
            print(f"Generated output filename: {output_file}")

        output_abs_path = Path(output_file).resolve()
        print(f"Output absolute path: {output_abs_path}")
        print(
            f"Output directory writable: {os.access(output_abs_path.parent, os.W_OK)}"
        )

        # Create log archive
        file_count = create_log_archive(log_dir, output_file)

        print(f"\n=== Archive completed ===")
        if file_count > 0:
            file_size = os.path.getsize(output_file)
            size_mb = file_size / (1024 * 1024)
            print(f"✓ Successfully created log archive: {output_file}")
            print(f"✓ Items archived: {file_count}")
            print(f"✓ Archive size: {size_mb:.2f} MB")
        else:
            print(f"⚠ Warning: Log directory '{log_dir}' is empty")
            print(f"✓ Created empty archive: {output_file}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
