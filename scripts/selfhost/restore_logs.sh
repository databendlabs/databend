#!/bin/bash

# Simple logging
log() {
	echo "[$(date '+%H:%M:%S')] $1"
}

log_error() {
	echo "[$(date '+%H:%M:%S')] ERROR: $1" >&2
}

log_step() {
	echo "[$(date '+%H:%M:%S')] [$1/$2] $3"
}

# Show help information
show_help() {
	cat <<EOF
Usage: $0 --stage STAGE [YYYYMMDD]

Restore Databend system data from backup archive in stage.

RECOMMENDED USAGE:
    # Interactive file selection (recommended)
    $0 --stage backup_stage

    # Restore specific date
    $0 --stage backup_stage 20250109

OPTIONS:
    --dsn DSN         Database connection string (overrides BENDSQL_DSN env var)
    --stage STAGE     Source stage name (required)
    --file FILENAME   Specific file name in stage
    --interactive     Force interactive mode
    -h, --help        Show this help message

ENVIRONMENT VARIABLES:
    BENDSQL_DSN      Default database connection string

INTERACTIVE CONTROLS:
    ↑/↓ or k/j       Navigate files
    Enter            Select file
    q                Quit

NOTE:
    If a local backup file already exists, it will be used instead of downloading.
    To use a fresh copy, delete the local file first.

EXAMPLES:
    export BENDSQL_DSN="http://username:password@localhost:8000/database"
    
    # Best: Browse and select interactively
    $0 --stage my_backup_stage
    
    # Quick: Restore specific date
    $0 --stage my_backup_stage 20250109
    
    # To use fresh copy, delete local file first
    rm data_2025-01-09.tar.gz && $0 --stage my_backup_stage 20250109
EOF
}

# Format file size
format_size() {
	local size="$1"
	if [[ "$size" =~ ^[0-9]+$ ]]; then
		if ((size >= 1073741824)); then
			echo "$((size / 1073741824)).$(((size % 1073741824) / 107374182))GB"
		elif ((size >= 1048576)); then
			echo "$((size / 1048576)).$(((size % 1048576) / 104857))MB"
		elif ((size >= 1024)); then
			echo "$((size / 1024)).$(((size % 1024) / 102))KB"
		else
			echo "${size}B"
		fi
	else
		echo "$size"
	fi
}

# Interactive file selector
interactive_file_selector() {
	local stage="$1"
	local dsn="$2"

	echo "Fetching file list from stage @${stage}..."

	# Get raw output from list command
	local raw_output
	raw_output=$(bendsql --dsn "${dsn}" --query="list @${stage};" 2>/dev/null)

	if [[ -z "$raw_output" ]]; then
		log_error "Failed to get file list from stage @${stage}"
		return 1
	fi

	# Parse files and sizes
	local files=()
	local sizes=()
	local file_list=""

	# Process each line to extract filename and size
	while IFS= read -r line; do
		# Skip empty lines and headers
		[[ -z "$line" ]] && continue
		[[ "$line" =~ ^[[:space:]]*name ]] && continue
		[[ "$line" =~ ^[[:space:]]*-+ ]] && continue

		# Extract filename (first field) and size (second field)
		local filename=$(echo "$line" | awk '{print $1}')
		local size=$(echo "$line" | awk '{print $2}')

		# Only include tar.gz files
		if [[ "$filename" =~ \.(tar\.gz|tgz)$ ]]; then
			files+=("$filename")
			sizes+=("$(format_size "$size")")
		fi
	done <<<"$raw_output"

	if [[ ${#files[@]} -eq 0 ]]; then
		log_error "No .tar.gz files found in stage @${stage}"
		return 1
	fi

	# Sort files (and corresponding sizes)
	local sorted_files=()
	local sorted_sizes=()
	local indices=($(for i in "${!files[@]}"; do echo "$i:${files[$i]}"; done | sort -t: -k2 | cut -d: -f1))

	for i in "${indices[@]}"; do
		sorted_files+=("${files[$i]}")
		sorted_sizes+=("${sizes[$i]}")
	done

	files=("${sorted_files[@]}")
	sizes=("${sorted_sizes[@]}")

	# Default to last file
	local selected=$((${#files[@]} - 1))
	local total=${#files[@]}

	echo ""
	echo "Found $total backup files in stage @${stage}:"
	echo "Use ↑/↓ or k/j to navigate, Enter to select, q to quit"
	echo ""

	# Calculate max filename length for alignment
	local max_name_len=0
	for filename in "${files[@]}"; do
		if [[ ${#filename} -gt $max_name_len ]]; then
			max_name_len=${#filename}
		fi
	done
	max_name_len=$((max_name_len + 2))

	# Display function
	display_files() {
		# Clear screen and move to top
		echo -ne "\033[2J\033[H"
		echo "Stage: @${stage} ($total files)"
		echo "Use ↑/↓ or k/j to navigate, Enter to select, q to quit"
		echo ""
		printf "%-${max_name_len}s %s\n" "Filename" "Size"
		printf "%-${max_name_len}s %s\n" "$(printf '%*s' $((max_name_len - 1)) '' | tr ' ' '-')" "----"

		for i in "${!files[@]}"; do
			local display_line="$(printf "%-${max_name_len}s %s" "${files[$i]}" "${sizes[$i]}")"
			if [[ $i -eq $selected ]]; then
				echo -e "\033[7m> $display_line\033[0m" # Highlighted
			else
				echo "  $display_line"
			fi
		done

		echo ""
		echo "Selected: ${files[$selected]} (${sizes[$selected]})"
	}

	# Initial display
	display_files

	# Input loop
	while true; do
		# Read single character
		read -rsn1 key

		case "$key" in
		$'\033') # Escape sequence
			read -rsn2 key
			case "$key" in
			'[A') # Up arrow
				((selected > 0)) && ((selected--))
				display_files
				;;
			'[B') # Down arrow
				((selected < total - 1)) && ((selected++))
				display_files
				;;
			esac
			;;
		'k' | 'K') # Up (vim-style)
			((selected > 0)) && ((selected--))
			display_files
			;;
		'j' | 'J') # Down (vim-style)
			((selected < total - 1)) && ((selected++))
			display_files
			;;
		'') # Enter
			echo ""
			echo "Selected: ${files[$selected]} (${sizes[$selected]})"
			SELECTED_FILE="${files[$selected]}"
			return 0
			;;
		'q' | 'Q') # Quit
			echo ""
			echo "Selection cancelled."
			return 1
			;;
		esac
	done
}

# Parse date from filename
parse_date_from_filename() {
	local filename="$1"
	local basename=$(basename "$filename" .tar.gz)

	# Try different patterns
	if [[ "$basename" =~ data_([0-9]{4})-([0-9]{2})-([0-9]{2}) ]]; then
		# Pattern: data_YYYY-MM-DD
		YEAR="${BASH_REMATCH[1]}"
		MONTH="${BASH_REMATCH[2]}"
		DAY="${BASH_REMATCH[3]}"
	elif [[ "$basename" =~ ([0-9]{4})([0-9]{2})([0-9]{2}) ]]; then
		# Pattern: YYYYMMDD anywhere in filename
		YEAR="${BASH_REMATCH[1]}"
		MONTH="${BASH_REMATCH[2]}"
		DAY="${BASH_REMATCH[3]}"
	else
		# Fallback to current date
		log "Could not extract date from filename: $filename, using current date"
		YEAR=$(date '+%Y')
		MONTH=$(date '+%m')
		DAY=$(date '+%d')
	fi

	FORMATTED_DATE="${YEAR}-${MONTH}-${DAY}"
	DATE_ARG="${YEAR}${MONTH}${DAY}"
}

# Parse arguments
FILE_MODE=false
INTERACTIVE_MODE=false
STAGE=""
DSN=""
DATE_ARG=""
TAR_FILE=""
STAGE_FILE=""

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
	--stage)
		STAGE="$2"
		shift 2
		;;
	--file)
		STAGE_FILE="$2"
		FILE_MODE=true
		shift 2
		;;
	--interactive)
		INTERACTIVE_MODE=true
		shift
		;;
	*)
		if [[ "$1" =~ ^[0-9]{8}$ ]]; then
			if [[ "$FILE_MODE" = true || "$INTERACTIVE_MODE" = true ]]; then
				log_error "Cannot specify date with --file or --interactive"
				echo "Use -h or --help for usage information." >&2
				exit 1
			fi
			DATE_ARG="$1"
			shift
		else
			log_error "Unknown parameter: $1"
			echo "Use -h or --help for usage information." >&2
			exit 1
		fi
		;;
	esac
done

# Validate parameters
if [[ -z "$STAGE" ]]; then
	log_error "Missing required parameter: --stage"
	echo "Use -h or --help for usage information." >&2
	exit 1
fi

# Validate DSN early for interactive mode
if [[ -z "$DSN" ]]; then
	DSN="$BENDSQL_DSN"
	if [[ -z "$DSN" ]]; then
		log_error "DSN not provided and BENDSQL_DSN not set"
		echo "Use -h or --help for usage information." >&2
		exit 1
	fi
fi

# Determine mode
if [[ "$INTERACTIVE_MODE" = true ]] || [[ -z "$DATE_ARG" && -z "$STAGE_FILE" ]]; then
	# Interactive mode
	if ! interactive_file_selector "$STAGE" "$DSN"; then
		exit 1
	fi

	STAGE_FILE="$SELECTED_FILE"
	FILE_MODE=true
	parse_date_from_filename "$SELECTED_FILE"
	TAR_FILE="$SELECTED_FILE"

elif [[ "$FILE_MODE" = true ]]; then
	# File mode validation
	if [[ -z "$STAGE_FILE" ]]; then
		log_error "Missing required parameter: --file"
		echo "Use -h or --help for usage information." >&2
		exit 1
	fi

	# Parse date from filename
	parse_date_from_filename "$STAGE_FILE"
	TAR_FILE="$STAGE_FILE"

else
	# Date mode validation
	if [[ -z "$DATE_ARG" ]]; then
		log_error "Missing required parameter: yyyymmdd date"
		echo "Use -h or --help for usage information." >&2
		exit 1
	fi

	# Format date
	YEAR=${DATE_ARG:0:4}
	MONTH=${DATE_ARG:4:2}
	DAY=${DATE_ARG:6:2}
	FORMATTED_DATE="${YEAR}-${MONTH}-${DAY}"
	TAR_FILE="data_${FORMATTED_DATE}.tar.gz"
fi

# Show operation mode
if [[ "$FILE_MODE" = true ]]; then
	log "Starting log restoration from stage file: ${TAR_FILE}"
	log "Source stage: @${STAGE}, Extracted date: ${FORMATTED_DATE}"
else
	log "Starting log restoration for date: ${FORMATTED_DATE}"
	log "Source stage: @${STAGE}, Target file: ${TAR_FILE}"
fi

# Check if local file exists
LOCAL_TAR_FILE=$(basename "$TAR_FILE")
USE_LOCAL_FILE=false

if [[ -f "${LOCAL_TAR_FILE}" && -s "${LOCAL_TAR_FILE}" ]]; then
	FILE_SIZE=$(du -h "${LOCAL_TAR_FILE}" | cut -f1)
	log "Local file ${LOCAL_TAR_FILE} exists (${FILE_SIZE}) - using local copy"
	USE_LOCAL_FILE=true
fi

# Step 1 & 2: Download or use local file
if [[ "$USE_LOCAL_FILE" = true ]]; then
	log_step "1" "6" "Using existing local file: ${LOCAL_TAR_FILE}"
	FILE_SIZE=$(du -h "${LOCAL_TAR_FILE}" | cut -f1)
	log "Using local file ${LOCAL_TAR_FILE} (${FILE_SIZE})"
else
	# Step 1: Generate download URL
	log_step "1" "6" "Generating presigned download URL for @${STAGE}/${TAR_FILE}"
	DOWNLOAD_SQL="PRESIGN DOWNLOAD @${STAGE}/${TAR_FILE}"
	DOWNLOAD_URL=$(bendsql --dsn "${DSN}" --query="${DOWNLOAD_SQL}" | awk '{print $3}')

	if [[ -z "$DOWNLOAD_URL" ]]; then
		log_error "Failed to generate download URL for ${TAR_FILE}"
		exit 1
	fi
	log "Download URL generated successfully"

	# Step 2: Download backup
	log_step "2" "6" "Downloading ${TAR_FILE} from stage @${STAGE}"
	curl -s -o "${LOCAL_TAR_FILE}" "${DOWNLOAD_URL}"

	if [[ ! -f "${LOCAL_TAR_FILE}" ]]; then
		log_error "Failed to download ${TAR_FILE}"
		exit 1
	fi

	FILE_SIZE=$(du -h "${LOCAL_TAR_FILE}" | cut -f1)
	log "Downloaded ${TAR_FILE} successfully (${FILE_SIZE})"
fi

# Step 3: Extract archive
log_step "3" "6" "Extracting ${LOCAL_TAR_FILE} to temporary directory"
TEMP_DIR="temp_extracted_${DATE_ARG}"
mkdir -p "${TEMP_DIR}"
tar -xzf "${LOCAL_TAR_FILE}" -C "${TEMP_DIR}"

EXTRACTED_FILES=$(find "${TEMP_DIR}" -type f | wc -l)
log "Extracted ${EXTRACTED_FILES} files from ${LOCAL_TAR_FILE}"

# Step 4: Detect path prefix
log_step "4" "6" "Analyzing directory structure for path prefix"
TARGET_DIRS=("settings" "columns" "user_functions" "query_raw_logs" "query_logs" "login_history" "query_profile_logs")
PREFIX=""

for target_dir in "${TARGET_DIRS[@]}"; do
	SAMPLE_FILE=$(find "${TEMP_DIR}" -path "*/${target_dir}/*" -type f | head -1)
	if [[ -n "$SAMPLE_FILE" ]]; then
		RELATIVE_PATH="${SAMPLE_FILE#${TEMP_DIR}/}"
		PREFIX=$(echo "$RELATIVE_PATH" | sed "s|/${target_dir}/.*||" | sed "s|${target_dir}/.*||")
		if [[ -n "$PREFIX" ]]; then
			PREFIX="${PREFIX}/"
		fi
		break
	fi
done

if [[ -n "$PREFIX" ]]; then
	log "Path prefix detected: '${PREFIX}' - will be stripped during upload"
else
	log "No path prefix detected - using original file paths"
fi

# Step 5: Upload files
UPLOAD_STAGE="${STAGE}_${YEAR}_${MONTH}_${DAY}"
log_step "5" "6" "Uploading ${EXTRACTED_FILES} files to stage @${UPLOAD_STAGE}"

bendsql --dsn "${DSN}" --query="DROP STAGE IF EXISTS ${UPLOAD_STAGE}" >/dev/null 2>&1
bendsql --dsn "${DSN}" --query="CREATE STAGE ${UPLOAD_STAGE}" >/dev/null 2>&1
log "Created destination stage: @${UPLOAD_STAGE}"

TOTAL_FILES=$(find "${TEMP_DIR}" -type f | wc -l)
CURRENT_FILE=0
UPLOAD_SUCCESS=0
UPLOAD_FAILED=0

find "${TEMP_DIR}" -type f | while read -r FILE; do
	CURRENT_FILE=$((CURRENT_FILE + 1))
	RELATIVE_PATH="${FILE#${TEMP_DIR}/}"

	if [[ -n "$PREFIX" && "$RELATIVE_PATH" == ${PREFIX}* ]]; then
		UPLOAD_PATH="${RELATIVE_PATH#${PREFIX}}"
	else
		UPLOAD_PATH="$RELATIVE_PATH"
	fi

	printf "\rUploading: %d/%d files (Success: %d, Failed: %d)" "$CURRENT_FILE" "$TOTAL_FILES" "$UPLOAD_SUCCESS" "$UPLOAD_FAILED"

	UPLOAD_SQL="PRESIGN UPLOAD @${UPLOAD_STAGE}/${UPLOAD_PATH}"
	UPLOAD_URL=$(bendsql --dsn "${DSN}" --query="${UPLOAD_SQL}" | awk '{print $3}')

	if [[ -n "$UPLOAD_URL" ]]; then
		if curl -s -X PUT -T "${FILE}" "${UPLOAD_URL}"; then
			UPLOAD_SUCCESS=$((UPLOAD_SUCCESS + 1))
		else
			UPLOAD_FAILED=$((UPLOAD_FAILED + 1))
		fi
	else
		UPLOAD_FAILED=$((UPLOAD_FAILED + 1))
	fi
done

echo # New line after progress
log "Upload completed: ${UPLOAD_SUCCESS} successful, ${UPLOAD_FAILED} failed"

# Cleanup temporary directory (keep local tar file for reuse)
log "Cleaning up: removing temporary directory ${TEMP_DIR}"
rm -rf "${TEMP_DIR}"
log "Local file ${LOCAL_TAR_FILE} retained for future use"

# Step 6: Restore database
RESTORE_DATABASE="${STAGE}_${YEAR}_${MONTH}_${DAY}"
log_step "6" "6" "Creating database '${RESTORE_DATABASE}' and restoring tables"

bendsql --dsn "${DSN}" --query="DROP DATABASE IF EXISTS ${RESTORE_DATABASE}" >/dev/null 2>&1
bendsql --dsn "${DSN}" --query="CREATE DATABASE ${RESTORE_DATABASE}" >/dev/null 2>&1
log "Created database: ${RESTORE_DATABASE}"

# Restore tables
declare -A TABLE_MAP=(
	["settings"]="system.settings:settings"
	["columns"]="system.columns:columns"
	["user_functions"]="system.user_functions:user_functions"
	["log_history"]="system_history.log_history:query_raw_logs"
	["query_history"]="system_history.query_history:query_logs"
	["login_history"]="system_history.login_history:login_history"
	["profile_history"]="system_history.profile_history:query_profile_logs"
)

for table_name in "${!TABLE_MAP[@]}"; do
	IFS=':' read -r source_table source_path <<<"${TABLE_MAP[$table_name]}"

	log "Restoring table: ${RESTORE_DATABASE}.${table_name} from @${UPLOAD_STAGE}/${source_path}"

	bendsql --dsn "${DSN}" --database "${RESTORE_DATABASE}" --query="CREATE TABLE ${table_name} LIKE ${source_table};" >/dev/null 2>&1
	bendsql --dsn "${DSN}" --database "${RESTORE_DATABASE}" --query="COPY INTO ${table_name} FROM @${UPLOAD_STAGE}/${source_path};" >/dev/null 2>&1

	ROW_COUNT=$(bendsql --dsn "${DSN}" --database "${RESTORE_DATABASE}" --query="SELECT COUNT(*) FROM ${table_name};" | tail -1)
	log "Table ${table_name} restored: ${ROW_COUNT} rows"
done

log "Log restoration completed successfully"
log "Restored database: ${RESTORE_DATABASE}"
log "Tables available: settings, columns, user_functions, log_history, query_history, login_history, profile_history"
