#!/bin/bash

# Simple logging
log() {
	echo "[$(date '+%H:%M:%S')] $1"
}

log_error() {
	echo "[$(date '+%H:%M:%S')] ERROR: $1" >&2
}

# Show help information
show_help() {
	cat <<EOF
Usage: $0 --stage STAGE [OPTIONS] [TAR_FILE]

Upload Databend log archive to a stage.

OPTIONS:
    --dsn DSN         Database connection string (overrides BENDSQL_DSN env var)
    --stage STAGE     Target stage name (required)
    --file TAR_FILE   Specific tar.gz file to upload (optional if provided as argument)
    -h, --help        Show this help message

ARGUMENTS:
    TAR_FILE          Path to tar.gz file to upload

ENVIRONMENT VARIABLES:
    BENDSQL_DSN      Default database connection string

EXAMPLES:
    export BENDSQL_DSN="http://username:password@localhost:8000/database"
    
    # Upload log archive to existing stage
    $0 --stage log_stage data_2025-08-23.tar.gz
    
    # Using --file option
    $0 --stage log_stage --file data_2025-08-23.tar.gz

NOTE:
    - Stage name should not include @ prefix
    - Stage must already exist
    - File will be uploaded with its original filename
EOF
}

# Parse arguments
STAGE=""
DSN=""
TAR_FILE=""

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
		TAR_FILE="$2"
		shift 2
		;;
	*)
		if [[ -z "$TAR_FILE" && "$1" =~ \.(tar\.gz|tgz)$ ]]; then
			TAR_FILE="$1"
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

if [[ -z "$TAR_FILE" ]]; then
	log_error "Missing tar.gz file to upload"
	echo "Use -h or --help for usage information." >&2
	exit 1
fi

# Validate DSN
if [[ -z "$DSN" ]]; then
	DSN="$BENDSQL_DSN"
	if [[ -z "$DSN" ]]; then
		log_error "DSN not provided and BENDSQL_DSN not set"
		echo "Use -h or --help for usage information." >&2
		exit 1
	fi
fi

# Check if file exists
if [[ ! -f "$TAR_FILE" ]]; then
	log_error "File not found: $TAR_FILE"
	exit 1
fi

# Get file info
FILENAME=$(basename "$TAR_FILE")
FILE_SIZE=$(du -h "$TAR_FILE" | cut -f1)

log "Starting upload of $FILENAME (${FILE_SIZE}) to stage @${STAGE}"

# Generate upload URL
log "Generating presigned upload URL for @${STAGE}/${FILENAME}"
UPLOAD_SQL="PRESIGN UPLOAD @${STAGE}/${FILENAME}"
UPLOAD_URL=$(bendsql --dsn "${DSN}" --query="${UPLOAD_SQL}" | awk '{print $3}')

if [[ -z "$UPLOAD_URL" ]]; then
	log_error "Failed to generate upload URL for ${FILENAME}"
	log_error "Please check if stage @${STAGE} exists and you have upload permissions"
	exit 1
fi

log "Upload URL generated successfully"

# Upload file
log "Uploading ${FILENAME} to @${STAGE}..."
if curl -s -X PUT -T "${TAR_FILE}" "${UPLOAD_URL}"; then
	log "Upload completed successfully"
else
	log_error "Upload failed"
	exit 1
fi

# Verify upload
log "Verifying upload..."
VERIFY_RESULT=$(bendsql --dsn "${DSN}" --query="LIST @${STAGE};" 2>/dev/null | grep "$FILENAME")

if [[ -n "$VERIFY_RESULT" ]]; then
	# Extract size from list output (second column)
	UPLOADED_SIZE=$(echo "$VERIFY_RESULT" | awk '{print $2}')
	log "✓ Upload verified: ${FILENAME} (${UPLOADED_SIZE} bytes) in stage @${STAGE}"
else
	log_error "Upload verification failed - file not found in stage"
	exit 1
fi

log "Upload operation completed successfully"
log "File: ${FILENAME} -> @${STAGE}/${FILENAME}"
