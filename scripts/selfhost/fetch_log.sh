#!/bin/bash

# Configuration
DEFAULT_OUTPUT_DIR="."
DEFAULT_HOURS=24
VERBOSITY=0 # 0=normal, 1=verbose, 2=debug

# Initialize variables
DSN=""
OUTPUT_DIR=""
HOURS=""
DATE=""
ARCHIVE_DATE=""
ARCHIVE_NAME=""
START_TIME=""
SERVER_START_TIME=""
SERVER_END_TIME=""
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
Usage: $0 [OPTIONS] [YYYYMMDD]

Download Databend system data for the specified time range.

OPTIONS:
    --dsn DSN              Database connection string (overrides BENDSQL_DSN env var)
    --output_dir PATH      Output directory (default: current directory)
    --date YYYYMMDD        Specific date to fetch (server date, e.g., 20250109)
    --hours HOURS          Number of hours to look back (default: 24, ignored if --date specified)
    -v                     Verbose output
    -vv                    Debug output (very verbose)
    -h, --help             Show this help message

ARGUMENTS:
    YYYYMMDD              Date in YYYYMMDD format (alternative to --date option)

ENVIRONMENT VARIABLES:
    BENDSQL_DSN           Default database connection string

EXAMPLES:
    # Fetch past 24 hours (default) - uses current date for filename
    export BENDSQL_DSN="http://username:password@localhost:8000/database?sslmode=enable"
    $0
    # Output: data_2025-01-09.tar.gz

    # Fetch past 12 hours - uses current date for filename
    $0 --hours 12
    # Output: data_2025-01-09.tar.gz

    # Fetch specific date (server date)
    $0 --date 20250109
    # Output: data_2025-01-09.tar.gz
    # or
    $0 20250109

    # Fetch specific date with custom output directory
    $0 --date 20250108 --output_dir /tmp/databend_export
    # Output: data_2025-01-08.tar.gz

    # Enable verbose logging
    $0 -v --date 20250109

NOTE:
    - When --date is specified, --hours is ignored
    - All dates are interpreted as server dates (database server timezone)
    - Date format must be YYYYMMDD (e.g., 20250109 for January 9, 2025)
    - Output filename format: data_YYYY-MM-DD.tar.gz (compatible with restore_log.sh)
EOF
}

# Convert YYYYMMDD to YYYY-MM-DD
format_date() {
	local input_date="$1"
	FORMATTED_DATE="${input_date:0:4}-${input_date:4:2}-${input_date:6:2}"
	log DEBUG "Formatted date: $FORMATTED_DATE"
}

calculate_archive_info() {
	if [[ -n "$DATE" ]]; then
		ARCHIVE_DATE="$FORMATTED_DATE"
	else
		# For hours mode, use current date
		ARCHIVE_DATE=$(date '+%Y-%m-%d')
	fi
	# Unified filename format for compatibility with restore_log.sh
	ARCHIVE_NAME="data_${ARCHIVE_DATE}.tar.gz"
	log DEBUG "Archive date: $ARCHIVE_DATE"
	log DEBUG "Archive name: $ARCHIVE_NAME"
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
		--date)
			DATE="$2"
			shift 2
			;;
		--hours)
			HOURS="$2"
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
			if [[ -z "$DATE" && "$1" =~ ^[0-9]{8}$ ]]; then
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
	HOURS="${HOURS:-$DEFAULT_HOURS}"

	if [[ -n "$DATE" ]]; then
		format_date "$DATE"
	fi

	calculate_archive_info
}

validate_arguments() {
	if [[ -z "$DSN" ]]; then
		log ERROR "No DSN provided. Set BENDSQL_DSN environment variable or use --dsn option."
		echo "Use -h or --help for usage information." >&2
		exit 1
	fi

	# Validate date parameter if provided
	if [[ -n "$DATE" ]]; then
		if [[ ! "$DATE" =~ ^[0-9]{8}$ ]]; then
			log ERROR "Invalid date format: $DATE (expected YYYYMMDD)"
			echo "Use -h or --help for usage information." >&2
			exit 1
		fi
	fi

	# Validate hours parameter
	if [[ ! "$HOURS" =~ ^[0-9]+$ ]] || [[ $HOURS -le 0 ]]; then
		log ERROR "Invalid hours value: $HOURS (must be a positive integer)"
		echo "Use -h or --help for usage information." >&2
		exit 1
	fi
}

build_base_command() {
	BASE_CMD="bendsql"
	if [[ -n "$DSN" ]]; then
		BASE_CMD+=" --dsn \"$DSN\""
	fi
	log INFO "Using command: $BASE_CMD"
}

execute_query() {
	local sql="$1"
	log DEBUG "Executing: ${sql:0:80}..."

	eval "$BASE_CMD --quote-style never --query=\"$sql\""
	local retval=$?

	if [[ $retval -ne 0 ]]; then
		log ERROR "Query failed"
		exit $retval
	fi
}

# Get server time range
get_server_time_range() {
	echo "Querying server time range..."

	local time_query
	if [[ -n "$DATE" ]]; then
		time_query="SELECT DATE('$FORMATTED_DATE') AS start_time, DATE('$FORMATTED_DATE') + INTERVAL 1 DAY AS end_time"
	else
		time_query="SELECT NOW() - INTERVAL $HOURS HOUR AS start_time, NOW() AS end_time"
	fi

	local time_result
	time_result=$(execute_query "$time_query")

	if [[ -z "$time_result" ]]; then
		log ERROR "Failed to get server time range"
		exit 1
	fi

	# Parse the result - assuming tab-separated format
	SERVER_START_TIME=$(echo "$time_result" | tail -n 1 | awk '{print $1" "$2}')
	SERVER_END_TIME=$(echo "$time_result" | tail -n 1 | awk '{print $3" "$4}')

	if [[ -n "$DATE" ]]; then
		echo "Server time range: $FORMATTED_DATE (full day, server timezone)"
	else
		echo "Server time range: $SERVER_START_TIME ~ $SERVER_END_TIME ($HOURS hours)"
	fi

	log DEBUG "Server start time: $SERVER_START_TIME"
	log DEBUG "Server end time: $SERVER_END_TIME"
}

download_file() {
	local filename="$1"
	local download_dir="$2"
	local current="$3"
	local total="$4"

	echo "  [$current/$total] Downloading: $filename"
	log INFO "Processing file: $filename"

	local presign_result
	presign_result=$(execute_query "PRESIGN DOWNLOAD @a5c7667401c0c728c2ef9703bdaea66d9ae2d906/$filename")
	local presign_url
	presign_url=$(echo "$presign_result" | awk '{print $3}')

	if [[ -z "$presign_url" ]]; then
		log ERROR "Empty presigned URL for $filename"
		return 1
	fi

	if [[ ! "$presign_url" =~ ^https?:// ]]; then
		log ERROR "Invalid URL format for $filename"
		return 1
	fi

	log DEBUG "Downloading from: $presign_url"
	if curl -k -L -s -S -f -o "$download_dir/$filename" "$presign_url"; then
		local file_size
		file_size=$(du -h "$download_dir/$filename" | cut -f1)
		echo "    ✓ Success ($file_size)"
		log INFO "Downloaded: $filename ($file_size)"
		return 0
	else
		echo "    ✗ Failed"
		log ERROR "Download failed: $filename"
		rm -f "$download_dir/$filename" 2>/dev/null
		return 1
	fi
}

create_archive() {
	local dirs=("$@")
	local deleted_files=0
	local temp_log
	temp_log=$(mktemp)

	echo "Creating archive..."
	local missing_dirs=()
	for dir in "${dirs[@]}"; do
		if [[ ! -d "$dir" ]]; then
			missing_dirs+=("$dir")
		fi
	done

	if [[ ${#missing_dirs[@]} -gt 0 ]]; then
		log ERROR "Missing directories: ${missing_dirs[*]}"
		rm -f "$temp_log"
		return 1
	fi

	local file_list
	file_list=$(mktemp)
	find "${dirs[@]}" -type f -print >"$file_list" 2>/dev/null
	local total_files
	total_files=$(wc -l <"$file_list")

	echo "  Archiving $total_files files..."
	if ! tar -czf "$ARCHIVE_NAME" --files-from="$file_list" 2>>"$temp_log"; then
		log ERROR "Archive creation failed:"
		cat "$temp_log" >&2
		rm -f "$file_list" "$temp_log" "$ARCHIVE_NAME"
		return 1
	fi

	if ! tar -tzf "$ARCHIVE_NAME" &>/dev/null; then
		log ERROR "Archive validation failed"
		rm -f "$ARCHIVE_NAME" "$file_list" "$temp_log"
		return 1
	fi

	local archive_size
	archive_size=$(du -h "$ARCHIVE_NAME" | cut -f1)
	echo "  ✓ Archive created: $ARCHIVE_NAME ($archive_size)"

	echo "  Cleaning up temporary files..."
	while IFS= read -r file; do
		if rm -f "$file" 2>>"$temp_log"; then
			((deleted_files++))
		fi
	done <"$file_list"

	echo "  ✓ Cleaned up $deleted_files files"
	rm -f "$file_list" "$temp_log"
	return 0
}

get_time_condition() {
	if [[ -n "$DATE" ]]; then
		echo "DATE(timestamp) = '$FORMATTED_DATE'"
	else
		echo "timestamp >= NOW() - INTERVAL $HOURS HOUR"
	fi
}

get_event_time_condition() {
	if [[ -n "$DATE" ]]; then
		echo "DATE(event_time) = '$FORMATTED_DATE'"
	else
		echo "event_time >= NOW() - INTERVAL $HOURS HOUR"
	fi
}

process_stage() {
	local stage_name="$1"
	local query="$2"
	local output_subdir="$3"
	local stage_num="$4"
	local total_stages="$5"

	echo ""
	echo "[$stage_num/$total_stages] $stage_name"

	echo "  Preparing data..."
	execute_query "REMOVE @a5c7667401c0c728c2ef9703bdaea66d9ae2d906;"
	execute_query "$query"

	echo "  Listing files..."
	local file_list
	file_list=$(execute_query "list @a5c7667401c0c728c2ef9703bdaea66d9ae2d906;" | awk '{print $1}')
	if [[ -z "$file_list" ]]; then
		echo "  ⚠ No files found"
		stage_total=0
		stage_success=0
		return 0
	fi

	# Count files
	local file_count
	file_count=$(echo "$file_list" | wc -l)
	echo "  Found $file_count files"

	mkdir -p "$OUTPUT_DIR/$output_subdir"

	local current=0
	local success=0
	while IFS= read -r filename; do
		((current++))
		if download_file "$filename" "$OUTPUT_DIR/$output_subdir" "$current" "$file_count"; then
			((success++))
		fi
	done <<<"$file_list"

	echo "  Stage completed: $success/$current files downloaded"

	# Return counts via global variables for main function
	stage_total=$current
	stage_success=$success

	return 0
}

main() {
	START_TIME=$(date +%s)

	parse_arguments "$@"
	validate_arguments

	echo "================================================================"
	echo "Databend Log Fetcher - Started at $(date '+%Y-%m-%d %H:%M:%S')"
	echo "================================================================"
	echo "Output archive: $ARCHIVE_NAME"
	if [[ -n "$DATE" ]]; then
		echo "Mode: Specific date ($FORMATTED_DATE)"
	else
		echo "Mode: Past $HOURS hours (filename uses current date)"
	fi
	echo "================================================================"

	build_base_command

	# Get and display server time range
	get_server_time_range

	mkdir -p "$OUTPUT_DIR"
	echo "Output directory: $OUTPUT_DIR"

	# Initialize workspace
	echo ""
	echo "[0/8] Initializing workspace..."
	execute_query "DROP STAGE IF EXISTS a5c7667401c0c728c2ef9703bdaea66d9ae2d906;"
	execute_query "CREATE STAGE a5c7667401c0c728c2ef9703bdaea66d9ae2d906;"
	echo "  ✓ Workspace initialized"

	local total=0
	local success=0

	# Stage 1: System Settings
	process_stage "Fetch System Settings" \
		"COPY INTO @a5c7667401c0c728c2ef9703bdaea66d9ae2d906 FROM system.settings;" \
		"settings" "1" "8"
	total=$((total + stage_total))
	success=$((success + stage_success))

	# Stage 2: Columns
	process_stage "Fetch System Columns" \
		"COPY INTO @a5c7667401c0c728c2ef9703bdaea66d9ae2d906 FROM system.columns;" \
		"columns" "2" "8"
	total=$((total + stage_total))
	success=$((success + stage_success))

	# Stage 3: User Functions
	process_stage "Fetch User Functions" \
		"COPY INTO @a5c7667401c0c728c2ef9703bdaea66d9ae2d906 FROM (SELECT * FROM system.user_functions);" \
		"user_functions" "3" "8"
	total=$((total + stage_total))
	success=$((success + stage_success))

	# Stage 4: Query History - uses query_logs directory to match restore script expectations
	local event_condition
	event_condition=$(get_event_time_condition)
	process_stage "Fetch Query History" \
		"COPY INTO @a5c7667401c0c728c2ef9703bdaea66d9ae2d906 FROM (SELECT * FROM system_history.query_history WHERE $event_condition);" \
		"query_logs" "4" "8"
	total=$((total + stage_total))
	success=$((success + stage_success))

	# Stage 5: Raw Logs - uses query_raw_logs directory to match restore script expectations
	local time_condition
	time_condition=$(get_time_condition)
	process_stage "Fetch Raw Logs" \
		"COPY INTO @a5c7667401c0c728c2ef9703bdaea66d9ae2d906 FROM (SELECT * FROM system_history.log_history WHERE $time_condition);" \
		"query_raw_logs" "5" "8"
	total=$((total + stage_total))
	success=$((success + stage_success))

	# Stage 6: Login History - uses login_history directory to match restore script expectations
	local login_event_condition
	login_event_condition=$(get_event_time_condition)
	process_stage "Fetch Login History" \
		"COPY INTO @a5c7667401c0c728c2ef9703bdaea66d9ae2d906 FROM (SELECT * FROM system_history.login_history WHERE $login_event_condition);" \
		"login_history" "6" "8"
	total=$((total + stage_total))
	success=$((success + stage_success))

	# Stage 7: Profile Logs - uses query_profile_logs directory to match restore script expectations
	process_stage "Fetch Profile Logs" \
		"COPY INTO @a5c7667401c0c728c2ef9703bdaea66d9ae2d906 FROM (SELECT * FROM system_history.profile_history WHERE $time_condition);" \
		"query_profile_logs" "7" "8"
	total=$((total + stage_total))
	success=$((success + stage_success))

	# Stage 8: Create Archive
	echo ""
	echo "[8/8] Creating archive and cleanup"

	echo ""
	echo "Summary:"
	if [[ -n "$DATE" ]]; then
		echo "  Date: $FORMATTED_DATE (server timezone)"
	else
		echo "  Time range: $SERVER_START_TIME ~ $SERVER_END_TIME"
	fi
	echo "  Files processed: $total"
	echo "  Successfully downloaded: $success"
	echo "  Failed: $((total - success))"
	if [[ $total -gt 0 ]]; then
		echo "  Success rate: $((success * 100 / total))%"
	fi

	if [[ $success -gt 0 ]]; then
		if create_archive "$OUTPUT_DIR/settings" "$OUTPUT_DIR/columns" "$OUTPUT_DIR/user_functions" "$OUTPUT_DIR/query_logs" "$OUTPUT_DIR/query_raw_logs" "$OUTPUT_DIR/login_history" "$OUTPUT_DIR/query_profile_logs"; then
			local end_time
			end_time=$(date +%s)
			local total_time
			total_time=$((end_time - START_TIME))
			echo ""
			echo "================================================================"
			echo "✓ Operation completed successfully in ${total_time}s"
			echo "Archive: $(pwd)/$ARCHIVE_NAME"
			echo "================================================================"
			exit 0
		else
			log ERROR "Archive creation failed"
			exit 1
		fi
	else
		log ERROR "No files were successfully downloaded"
		exit 1
	fi
}

main "$@"
