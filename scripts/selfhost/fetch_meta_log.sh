#!/bin/bash

set -euo pipefail

# Parse TOML config to extract log directory
parse_config() {
    local config_file="$1"
    
    if [[ ! -f "$config_file" ]]; then
        echo "Error: Config file '$config_file' not found" >&2
        return 1
    fi
    
    # Parse top-level log_dir
    local top_level_log_dir
    top_level_log_dir=$(grep -E '^[[:space:]]*log_dir[[:space:]]*=' "$config_file" | \
        sed -E 's/^[[:space:]]*log_dir[[:space:]]*=[[:space:]]*["\047]?([^"\047]*)["\047]?.*$/\1/' | \
        head -1)
    
    # Parse [log.file].dir
    local nested_log_dir=""
    local in_log_file_section=false
    while IFS= read -r line; do
        if [[ "$line" =~ ^\[log\.file\] ]]; then
            in_log_file_section=true
            continue
        fi
        
        if [[ "$line" =~ ^\[.*\] ]] && [[ "$in_log_file_section" == true ]]; then
            in_log_file_section=false
            continue
        fi
        
        if [[ "$in_log_file_section" == true ]] && [[ "$line" =~ ^[[:space:]]*dir[[:space:]]*= ]]; then
            nested_log_dir=$(echo "$line" | sed -E 's/^[[:space:]]*dir[[:space:]]*=[[:space:]]*["\047]?([^"\047]*)["\047]?.*$/\1/')
            break
        fi
    done < "$config_file"
    
    # Return the log directory
    if [[ -n "$nested_log_dir" ]]; then
        echo "$nested_log_dir"
    elif [[ -n "$top_level_log_dir" ]]; then
        echo "$top_level_log_dir"
    else
        echo "Error: No log directory found in config" >&2
        return 1
    fi
}

# Try multiple path resolution strategies
resolve_and_find_path() {
    local log_dir="$1"
    local config_file="$2"
    
    # If absolute path, return as-is
    if [[ "$log_dir" == /* ]]; then
        echo "$log_dir"
        return 0
    fi
    
    # Strategy 1: Relative to current working directory
    local path1="$PWD/$log_dir"
    if [[ -d "$path1" ]]; then
        realpath "$path1"
        return 0
    fi
    
    # Strategy 2: Relative to config file directory
    local config_dir
    config_dir=$(dirname "$(realpath "$config_file")")
    local path2="$config_dir/$log_dir"
    if [[ -d "$path2" ]]; then
        realpath "$path2"
        return 0
    fi
    
    # If none found, return the first strategy
    realpath -m "$path1"
    return 1
}

# Show help
show_help() {
    cat <<EOF
Usage: $0 CONFIG_FILE [-o OUTPUT_FILE]

Collect databend-meta logs based on TOML config file.

Arguments:
  CONFIG_FILE       Path to databend-meta config file (.toml)

Options:
  -o OUTPUT_FILE    Output archive filename (default: auto-generated)
  -h                Show this help
EOF
}

# Main function
main() {
    local config_file=""
    local output_file=""
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -h|--help)
                show_help
                exit 0
                ;;
            -o)
                output_file="$2"
                shift 2
                ;;
            -*)
                echo "Error: Unknown option $1" >&2
                exit 1
                ;;
            *)
                config_file="$1"
                shift
                ;;
        esac
    done
    
    if [[ -z "$config_file" ]]; then
        echo "Error: CONFIG_FILE required" >&2
        exit 1
    fi
    
    echo "Databend Meta Log Collector"
    echo "============================"
    
    # Parse config
    echo "Parsing config file: $config_file"
    local log_dir_str
    log_dir_str=$(parse_config "$config_file")
    if [[ $? -ne 0 ]]; then
        exit 1
    fi
    
    echo "Found log directory: '$log_dir_str'"
    
    # Resolve path
    local log_dir
    log_dir=$(resolve_and_find_path "$log_dir_str" "$config_file")
    local path_found=$?
    
    echo "Resolved path: $log_dir"
    
    # Check directory
    if [[ $path_found -ne 0 || ! -d "$log_dir" ]]; then
        echo "Error: Log directory '$log_dir' does not exist" >&2
        exit 1
    fi
    
    # Generate output filename
    if [[ -z "$output_file" ]]; then
        local config_name
        config_name=$(basename "$config_file" .toml)
        local timestamp
        timestamp=$(date "+%Y%m%d_%H%M%S")
        output_file="${config_name}_logs_${timestamp}.tar.gz"
    fi
    
    echo "Creating archive: $output_file"
    
    # Get absolute paths
    local abs_log_dir
    abs_log_dir=$(realpath "$log_dir")
    local abs_output_file
    abs_output_file=$(realpath -m "$output_file")
    
    echo "Log directory: $abs_log_dir"
    echo "Output file: $abs_output_file"
    
    # Count and list files
    local files
    files=$(find "$abs_log_dir" -type f)
    local file_count
    file_count=$(echo "$files" | wc -l)
    
    if [[ -z "$files" ]]; then
        file_count=0
    fi
    
    echo "Found $file_count files to archive"
    
    if [[ $file_count -eq 0 ]]; then
        echo "Warning: No files found in log directory"
        tar -czf "$abs_output_file" --files-from /dev/null
    else
        echo "Files to archive:"
        echo "$files" | while read -r file; do
            echo "  $(basename "$file")"
        done
        
        # Create archive using absolute paths
        tar -czf "$abs_output_file" -C "$abs_log_dir" . || {
            echo "Error: tar command failed" >&2
            echo "Trying alternative method..." >&2
            
            # Alternative: use find and tar
            find "$abs_log_dir" -type f -print0 | tar -czf "$abs_output_file" --null -T - || {
                echo "Error: Both tar methods failed" >&2
                exit 1
            }
        }
    fi
    
    # Verify archive was created
    if [[ -f "$abs_output_file" ]]; then
        local size
        size=$(stat -c%s "$abs_output_file" 2>/dev/null || stat -f%z "$abs_output_file" 2>/dev/null)
        echo "Success: Created $output_file ($(echo "$size" | awk '{printf "%.1fMB", $1/1024/1024}'))"
        
        # Test archive
        echo "Testing archive..."
        if tar -tzf "$abs_output_file" >/dev/null 2>&1; then
            echo "Archive is valid"
        else
            echo "Warning: Archive may be corrupted"
        fi
    else
        echo "Error: Failed to create archive" >&2
        exit 1
    fi
}

main "$@"
