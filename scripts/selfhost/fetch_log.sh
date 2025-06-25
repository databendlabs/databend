#!/bin/bash

# Configuration
DEFAULT_OUTPUT_DIR="."
VERBOSITY=0  # 0=normal, 1=verbose, 2=debug

# Initialize variables
DSN=""
DATE=""
OUTPUT_DIR=""

# Logging functions
log() {
    local level="$1"
    local message="$2"

    case "$level" in
        DEBUG)
            [[ $VERBOSITY -ge 2 ]] && echo "[DEBUG] $message" >&2
            ;;
        INFO)
            [[ $VERBOSITY -ge 1 ]] && echo "[INFO] $message" >&2
            ;;
        ERROR)
            echo "[ERROR] $message" >&2
            ;;
    esac
}

parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --dsn)
                DSN="$2"
                shift 2
                ;;
            --output_dir)
                OUTPUT_DIR="$2"
                shift 2
                ;;
            -v)
                VERBOSITY=1
                shift
                ;;
            -vv)
                VERBOSITY=2
                shift
                ;;
            *)
                if [[ -z "$DATE" ]]; then
                    DATE="$1"
                    shift
                else
                    log ERROR "Unexpected argument: $1"
                    exit 1
                fi
                ;;
        esac
    done

    OUTPUT_DIR="${OUTPUT_DIR:-$DEFAULT_OUTPUT_DIR}"
}

validate_arguments() {
    if [[ -z "$DATE" ]]; then
        log ERROR "Missing required date parameter"
        echo "Usage: $0 [--dsn \"http://...\"] [--output_dir \"path\"] [-v|-vv] YYYYMMDD" >&2
        exit 1
    fi

    if [[ ! "$DATE" =~ ^[0-9]{8}$ ]]; then
        log ERROR "Invalid date format: $DATE (expected YYYYMMDD)"
        exit 1
    fi
}

build_base_command() {
    BASE_CMD="bendsql"
    [[ -n "$DSN" ]] && BASE_CMD+=" --dsn \"$DSN\""
    log INFO "Using command: $BASE_CMD"
}

execute_query() {
    local sql="$1"
    log DEBUG "Executing: ${sql:0:60}..."

    # Execute query directly without capturing output
    eval "$BASE_CMD --query=\"$sql\""
    local retval=$?

    if [[ $retval -ne 0 ]]; then
        log ERROR "Query failed: ${sql:0:60}"
        exit $retval
    fi
}

download_file() {
    local filename="$1"
    local download_dir="$2"

    log INFO "Processing file: $filename"

    presign_result=$(execute_query "PRESIGN DOWNLOAD @fetch_columns/$filename")
    presign_url=$(echo "$presign_result" | awk 'NR>1 {print $3}')

    [[ -z "$presign_url" ]] && { log ERROR "Empty presigned URL"; return 1; }
    [[ "$presign_url" =~ ^https?:// ]] || { log ERROR "Invalid URL format"; return 1; }

    log DEBUG "Downloading from: $presign_url"
    if curl -L -s -S -f -o "$download_dir/$filename" "$presign_url"; then
        log INFO "Downloaded: $filename ($(du -h "$download_dir/$filename" | cut -f1))"
        return 0
    else
        log ERROR "Download failed"
        rm -f "$download_dir/$filename" 2>/dev/null
        return 1
    fi
}

main() {
    parse_arguments "$@"
    validate_arguments
    build_base_command

    mkdir -p "$OUTPUT_DIR"
    log INFO "Output directory: $OUTPUT_DIR"

    execute_query "DROP STAGE a5c7667401c0c728c2ef9703bdaea66d9ae2d906;"
    execute_query "CREATE STAGE a5c7667401c0c728c2ef9703bdaea66d9ae2d906;"

    log INFO "Fetch columns info..."
    execute_query "COPY INTO @a5c7667401c0c728c2ef9703bdaea66d9ae2d906 FROM system.columns;"

    file_list=$(execute_query "list @a5c7667401c0c728c2ef9703bdaea66d9ae2d906;" | awk 'NR>1 {print $1}')
    [[ -z "$file_list" ]] && { log ERROR "No files found"; exit 1; }

    total=0
    success=0

    mkdir -p "$OUTPUT_DIR/columns"
    while IFS= read -r filename; do
        ((total++))
        download_file "$filename" "$OUTPUT_DIR/columns" && ((success++))
    done <<< "$file_list"

    log INFO "Fetch Databend query logs..."
    execute_query "REMOVE @a5c7667401c0c728c2ef9703bdaea66d9ae2d906;"

    execute_query "COPY INTO @a5c7667401c0c728c2ef9703bdaea66d9ae2d906 FROM (SELECT * FROM system_history.query_history);"

    file_list=$(execute_query "list @a5c7667401c0c728c2ef9703bdaea66d9ae2d906;" | awk 'NR>1 {print $1}')
    [[ -z "$file_list" ]] && { log ERROR "No files found"; exit 1; }

    mkdir -p "$OUTPUT_DIR/query_logs"
    while IFS= read -r filename; do
        ((total++))
        download_file "$filename" "$OUTPUT_DIR/query_logs" && ((success++))
    done <<< "$file_list"

    log INFO "Fetch Databend query raw logs..."
    execute_query "REMOVE @a5c7667401c0c728c2ef9703bdaea66d9ae2d906;"

    execute_query "COPY INTO @a5c7667401c0c728c2ef9703bdaea66d9ae2d906 FROM (SELECT * FROM system_history.log_history);"

    file_list=$(execute_query "list @a5c7667401c0c728c2ef9703bdaea66d9ae2d906;" | awk 'NR>1 {print $1}')
    [[ -z "$file_list" ]] && { log ERROR "No files found"; exit 1; }

    mkdir -p "$OUTPUT_DIR/query_raw_logs"
    while IFS= read -r filename; do
        ((total++))
        download_file "$filename" "$OUTPUT_DIR/query_raw_logs" && ((success++))
    done <<< "$file_list"

    log INFO "Fetch Databend query profile logs..."
    execute_query "REMOVE @a5c7667401c0c728c2ef9703bdaea66d9ae2d906;"

    execute_query "COPY INTO @a5c7667401c0c728c2ef9703bdaea66d9ae2d906 FROM (SELECT * FROM system_history.profile_history);"

    file_list=$(execute_query "list @a5c7667401c0c728c2ef9703bdaea66d9ae2d906;" | awk 'NR>1 {print $1}')
    [[ -z "$file_list" ]] && { log ERROR "No files found"; exit 1; }

    mkdir -p "$OUTPUT_DIR/query_profile_logs"
    while IFS= read -r filename; do
        ((total++))
        download_file "$filename" "$OUTPUT_DIR/query_profile_logs" && ((success++))
    done <<< "$file_list"

    echo "Summary:"
    echo "Files processed: $total"
    echo "Successfully downloaded: $success"
    echo "Failed: $((total - success))"

    [[ $success -eq $total ]] || exit 1
}

main "$@"