#!/bin/bash

# Configuration
DEFAULT_OUTPUT_DIR="."
VERBOSITY=0 # 0=normal, 1=verbose, 2=debug

# Initialize variables
DSN=""
DATE=""
OUTPUT_DIR=""
FORMATTED_DATE=""

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

# Show help information
show_help() {
	cat <<EOF
Usage: $0 [OPTIONS] YYYYMMDD

Download Databend system data for the specified date.

OPTIONS:
    --dsn DSN              Database connection string (overrides BENDSQL_DSN env var)
    --output_dir PATH      Output directory (default: current directory)
    -v                     Verbose output
    -vv                    Debug output (very verbose)
    -h, --help             Show this help message

ARGUMENTS:
    YYYYMMDD              Date in YYYYMMDD format (e.g., 20250701)

ENVIRONMENT VARIABLES:
    BENDSQL_DSN           Default database connection string

EXAMPLES:
    # Use environment variable for DSN
    export BENDSQL_DSN="http://username:password@localhost:8000/database?sslmode=enable"
    $0 20250701

    # Override DSN with command line
    $0 --dsn "http://username:password@localhost:8000/database?sslmode=enable" 20250701

    # Specify custom output directory
    $0 --output_dir /tmp/databend_export 20250701

    # Enable verbose logging
    $0 -v 20250701
EOF
}

# Convert YYYYMMDD to YYYY-MM-DD
format_date() {
	local input_date="$1"
	FORMATTED_DATE="${input_date:0:4}-${input_date:4:2}-${input_date:6:2}"
	log DEBUG "Formatted date: $FORMATTED_DATE"
}

parse_arguments() {
	while [[ $# -gt 0 ]]; do
		case "$1" in
		-h | --help)
			show_help
			exit 0
			;;
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
				echo "Use -h or --help for usage information." >&2
				exit 1
			fi
			;;
		esac
	done

	# Set DSN from environment variable if not provided via command line
	if [[ -z "$DSN" && -n "$BENDSQL_DSN" ]]; then
		DSN="$BENDSQL_DSN"
		log DEBUG "Using DSN from environment variable BENDSQL_DSN"
	fi

	OUTPUT_DIR="${OUTPUT_DIR:-$DEFAULT_OUTPUT_DIR}"

	if [[ -n "$DATE" ]]; then
		format_date "$DATE"
	fi
}

validate_arguments() {
	if [[ -z "$DATE" ]]; then
		log ERROR "Missing required date parameter"
		echo "Use -h or --help for usage information." >&2
		exit 1
	fi

	if [[ ! "$DATE" =~ ^[0-9]{8}$ ]]; then
		log ERROR "Invalid date format: $DATE (expected YYYYMMDD)"
		echo "Use -h or --help for usage information." >&2
		exit 1
	fi

	if [[ -z "$DSN" ]]; then
		log ERROR "No DSN provided. Set BENDSQL_DSN environment variable or use --dsn option."
		echo "Use -h or --help for usage information." >&2
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

	eval "$BASE_CMD --quote-style never --query=\"$sql\""
	local retval=$?

	[[ $retval -ne 0 ]] && {
		log ERROR "Query failed"
		exit $retval
	}
}

download_file() {
	local filename="$1"
	local download_dir="$2"

	log INFO "Processing file: $filename"

	presign_result=$(execute_query "PRESIGN DOWNLOAD @a5c7667401c0c728c2ef9703bdaea66d9ae2d906/$filename")
	presign_url=$(echo "$presign_result" | awk '{print $3}')

	[[ -z "$presign_url" ]] && {
		log ERROR "Empty presigned URL"
		return 1
	}
	[[ "$presign_url" =~ ^https?:// ]] || {
		log ERROR "Invalid URL format"
		return 1
	}

	log DEBUG "Downloading from: $presign_url"
	if curl -k -L -s -S -f -o "$download_dir/$filename" "$presign_url"; then
		log INFO "Downloaded: $filename ($(du -h "$download_dir/$filename" | cut -f1))"
		return 0
	else
		log ERROR "Download failed"
		rm -f "$download_dir/$filename" 2>/dev/null
		return 1
	fi
}

create_archive() {
	local date_str="$1"
	shift
	local dirs=("$@")
	local deleted_files=0
	local temp_log=$(mktemp)

	local archive_name="data_${date_str}.tar.gz"

	local missing_dirs=()
	for dir in "${dirs[@]}"; do
		if [[ ! -d "$dir" ]]; then
			missing_dirs+=("$dir")
		elif [[ ! -r "$dir" ]]; then
			log WARN "[WARN] Directory exists but not readable: $dir"
		fi
	done

	if [[ ${#missing_dirs[@]} -gt 0 ]]; then
		log ERROR "[ERROR] Missing directories: ${missing_dirs[*]}"
		return 1
	fi

	local file_list
	file_list=$(mktemp)
	find "${dirs[@]}" -type f -print >"$file_list" 2>/dev/null

	log INFO "[INFO] Creating archive $archive_name from ${#dirs[@]} directories..."
	if ! tar -czf "$archive_name" --files-from="$file_list" 2>>"$temp_log"; then
		log ERROR "[ERROR] Create archive failure, for details:"
		cat "$temp_log" >&2
		rm -f "$file_list" "$temp_log" "$archive_name"
		return 1
	fi

	if ! tar -tzf "$archive_name" &>/dev/null; then
		log ERROR "[ERROR] Create archive failure"
		rm -f "$archive_name"
		return 1
	fi

	log INFO "[INFO] Clean temp files..."
	while IFS= read -r file; do
		if rm -f "$file" 2>>"$temp_log"; then
			((deleted_files++))
		else
			log WARN "[WARN] remove file failure: $file"
		fi
	done <"$file_list"

	rm -f "$file_list" "$temp_log"
	return 0
}

extract_first_column() {
	sed 's/[[:space:]]\{1,\}/\t/g' |
		sed -e 's/^[[:space:]]*//' \
			-e 's/[[:space:]]*$//' \
			-e 's/"//g' |
		awk -F '\t' 'NF>0 {print $1}'
}

main() {
	parse_arguments "$@"
	validate_arguments
	build_base_command

	mkdir -p "$OUTPUT_DIR"
	log INFO "Output directory: $OUTPUT_DIR"

	execute_query "DROP STAGE IF EXISTS a5c7667401c0c728c2ef9703bdaea66d9ae2d906;"
	execute_query "CREATE STAGE a5c7667401c0c728c2ef9703bdaea66d9ae2d906;"

	log INFO "Fetch columns info..."
	execute_query "COPY INTO @a5c7667401c0c728c2ef9703bdaea66d9ae2d906 FROM system.columns;"

	file_list=$(execute_query "list @a5c7667401c0c728c2ef9703bdaea66d9ae2d906;" | awk '{print $1}')
	[[ -z "$file_list" ]] && {
		log ERROR "No files found"
		exit 1
	}

	total=0
	success=0

	mkdir -p "$OUTPUT_DIR/columns"
	while IFS= read -r filename; do
		((total++))
		download_file "$filename" "$OUTPUT_DIR/columns" && ((success++))
	done <<<"$file_list"

	log INFO "Fetch Databend user functions..."
	execute_query "REMOVE @a5c7667401c0c728c2ef9703bdaea66d9ae2d906;"

	execute_query "COPY INTO @a5c7667401c0c728c2ef9703bdaea66d9ae2d906 FROM (SELECT * FROM system.user_functions);"

	file_list=$(execute_query "list @a5c7667401c0c728c2ef9703bdaea66d9ae2d906;" | awk '{print $1}')
	[[ -z "$file_list" ]] && {
		log ERROR "No files found"
		exit 1
	}

	mkdir -p "$OUTPUT_DIR/user_functions"
	while IFS= read -r filename; do
		((total++))
		download_file "$filename" "$OUTPUT_DIR/user_functions" && ((success++))
	done <<<"$file_list"

	log INFO "Fetch Databend query logs..."
	execute_query "REMOVE @a5c7667401c0c728c2ef9703bdaea66d9ae2d906;"

	execute_query "COPY INTO @a5c7667401c0c728c2ef9703bdaea66d9ae2d906 FROM (SELECT * FROM system_history.query_history WHERE event_date = '$FORMATTED_DATE');"

	file_list=$(execute_query "list @a5c7667401c0c728c2ef9703bdaea66d9ae2d906;" | awk '{print $1}')
	[[ -z "$file_list" ]] && {
		log ERROR "No files found"
		exit 1
	}

	mkdir -p "$OUTPUT_DIR/query_logs"
	while IFS= read -r filename; do
		((total++))
		download_file "$filename" "$OUTPUT_DIR/query_logs" && ((success++))
	done <<<"$file_list"

	log INFO "Fetch Databend query raw logs..."
	execute_query "REMOVE @a5c7667401c0c728c2ef9703bdaea66d9ae2d906;"

	execute_query "COPY INTO @a5c7667401c0c728c2ef9703bdaea66d9ae2d906 FROM (SELECT * FROM system_history.log_history WHERE to_date(timestamp) = '$FORMATTED_DATE');"

	file_list=$(execute_query "list @a5c7667401c0c728c2ef9703bdaea66d9ae2d906;" | awk '{print $1}')
	[[ -z "$file_list" ]] && {
		log ERROR "No files found"
		exit 1
	}

	mkdir -p "$OUTPUT_DIR/query_raw_logs"
	while IFS= read -r filename; do
		((total++))
		download_file "$filename" "$OUTPUT_DIR/query_raw_logs" && ((success++))
	done <<<"$file_list"

	log INFO "Fetch Databend query profile logs..."
	execute_query "REMOVE @a5c7667401c0c728c2ef9703bdaea66d9ae2d906;"

	execute_query "COPY INTO @a5c7667401c0c728c2ef9703bdaea66d9ae2d906 FROM (SELECT * FROM system_history.profile_history WHERE to_date(timestamp) = '$FORMATTED_DATE');"

	file_list=$(execute_query "list @a5c7667401c0c728c2ef9703bdaea66d9ae2d906;" | awk '{print $1}')
	[[ -z "$file_list" ]] && {
		log ERROR "No files found"
		exit 1
	}

	mkdir -p "$OUTPUT_DIR/query_profile_logs"
	while IFS= read -r filename; do
		((total++))
		download_file "$filename" "$OUTPUT_DIR/query_profile_logs" && ((success++))
	done <<<"$file_list"

	echo "Summary:"
	echo "Files processed: $total"
	echo "Successfully downloaded: $success"
	echo "Failed: $((total - success))"

	if [[ $success -gt 0 ]]; then
		if create_archive "$FORMATTED_DATE" "$OUTPUT_DIR/columns" "$OUTPUT_DIR/user_functions" "$OUTPUT_DIR/query_logs" "$OUTPUT_DIR/query_raw_logs" "$OUTPUT_DIR/query_profile_logs"; then
			echo "Operation completed successfully"
			exit 0
		else
			exit 1
		fi
	else
		exit 1
	fi
}

main "$@"
