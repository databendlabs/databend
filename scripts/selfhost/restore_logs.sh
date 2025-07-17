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

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dsn)
            DSN="$2"
            shift 2
            ;;
        --stage)
            STAGE="$2"
            shift 2
            ;;
        *)
            if [[ "$1" =~ ^[0-9]{8}$ ]]; then
                DATE_ARG="$1"
                shift
            else
                log_error "Unknown parameter: $1"
                exit 1
            fi
            ;;
    esac
done

# Validate parameters
if [[ -z "$STAGE" || -z "$DATE_ARG" ]]; then
    log_error "Missing required parameters: --stage or yyyymmdd date"
    exit 1
fi

if [[ -z "$DSN" ]]; then
    DSN="$BENDSQL_DSN"
    if [[ -z "$DSN" ]]; then
        log_error "DSN not provided and BENDSQL_DSN not set"
        exit 1
    fi
fi

# Format date
YEAR=${DATE_ARG:0:4}
MONTH=${DATE_ARG:4:2}
DAY=${DATE_ARG:6:2}
FORMATTED_DATE="${YEAR}-${MONTH}-${DAY}"
TAR_FILE="data_${FORMATTED_DATE}.tar.gz"

log "Starting log restoration for date: ${FORMATTED_DATE}"
log "Source stage: @${STAGE}, Target file: ${TAR_FILE}"

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
curl -s -o "${TAR_FILE}" "${DOWNLOAD_URL}"

if [[ ! -f "${TAR_FILE}" ]]; then
    log_error "Failed to download ${TAR_FILE}"
    exit 1
fi

FILE_SIZE=$(du -h "${TAR_FILE}" | cut -f1)
log "Downloaded ${TAR_FILE} successfully (${FILE_SIZE})"

# Step 3: Extract archive
log_step "3" "6" "Extracting ${TAR_FILE} to temporary directory"
TEMP_DIR="temp_extracted_${DATE_ARG}"
mkdir -p "${TEMP_DIR}"
tar -xzf "${TAR_FILE}" -C "${TEMP_DIR}"

EXTRACTED_FILES=$(find "${TEMP_DIR}" -type f | wc -l)
log "Extracted ${EXTRACTED_FILES} files from ${TAR_FILE}"

# Step 4: Detect path prefix
log_step "4" "6" "Analyzing directory structure for path prefix"
TARGET_DIRS=("columns" "user_functions" "query_raw_logs" "query_logs" "query_profile_logs")
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

echo  # New line after progress
log "Upload completed: ${UPLOAD_SUCCESS} successful, ${UPLOAD_FAILED} failed"

# Cleanup
log "Cleaning up: removing ${TEMP_DIR} and ${TAR_FILE}"
rm -rf "${TEMP_DIR}" "${TAR_FILE}"

# Step 6: Restore database
RESTORE_DATABASE="${STAGE}_${YEAR}_${MONTH}_${DAY}"
log_step "6" "6" "Creating database '${RESTORE_DATABASE}' and restoring tables"

bendsql --dsn "${DSN}" --query="DROP DATABASE IF EXISTS ${RESTORE_DATABASE}" >/dev/null 2>&1
bendsql --dsn "${DSN}" --query="CREATE DATABASE ${RESTORE_DATABASE}" >/dev/null 2>&1
log "Created database: ${RESTORE_DATABASE}"

# Restore tables
declare -A TABLE_MAP=(
    ["columns"]="system.columns:columns"
    ["user_functions"]="system.user_functions:user_functions"
    ["log_history"]="system_history.log_history:query_raw_logs"
    ["query_history"]="system_history.query_history:query_logs"
    ["profile_history"]="system_history.profile_history:query_profile_logs"
)

for table_name in "${!TABLE_MAP[@]}"; do
    IFS=':' read -r source_table source_path <<< "${TABLE_MAP[$table_name]}"
    
    log "Restoring table: ${RESTORE_DATABASE}.${table_name} from @${UPLOAD_STAGE}/${source_path}"
    
    bendsql --dsn "${DSN}" --database "${RESTORE_DATABASE}" --query="CREATE TABLE ${table_name} LIKE ${source_table};" >/dev/null 2>&1
    bendsql --dsn "${DSN}" --database "${RESTORE_DATABASE}" --query="COPY INTO ${table_name} FROM @${UPLOAD_STAGE}/${source_path};" >/dev/null 2>&1
    
    ROW_COUNT=$(bendsql --dsn "${DSN}" --database "${RESTORE_DATABASE}" --query="SELECT COUNT(*) FROM ${table_name};" | tail -1)
    log "Table ${table_name} restored: ${ROW_COUNT} rows"
done

log "Log restoration completed successfully"
log "Restored database: ${RESTORE_DATABASE}"
log "Tables available: columns, user_functions, log_history, query_history, profile_history"
