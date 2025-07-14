#!/bin/bash

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
                echo "Unknown parameter: $1"
                exit 1
            fi
            ;;
    esac
done

# Check required parameters
if [[ -z "$STAGE" || -z "$DATE_ARG" ]]; then
    echo "Missing required parameters: --stage or yyyymmdd date"
    exit 1
fi

# If DSN not provided, try to get from environment variable
if [[ -z "$DSN" ]]; then
    DSN="$BENDSQL_DSN"
    if [[ -z "$DSN" ]]; then
        echo "--dsn parameter not provided and BENDSQL_DSN environment variable not set"
        exit 1
    fi
fi

# Format date
YEAR=${DATE_ARG:0:4}
MONTH=${DATE_ARG:4:2}
DAY=${DATE_ARG:6:2}
FORMATTED_DATE="${YEAR}-${MONTH}-${DAY}"

# 1. Get download presigned URL
DOWNLOAD_SQL="PRESIGN DOWNLOAD @${STAGE}/data_${FORMATTED_DATE}.tar.gz"
DOWNLOAD_URL=$(bendsql --dsn "${DSN}" --query="${DOWNLOAD_SQL}" | awk '{print $3}')

if [[ -z "$DOWNLOAD_URL" ]]; then
    echo "Failed to get download URL"
    exit 1
fi

# 2. Download the file
TAR_FILE="data_${FORMATTED_DATE}.tar.gz"
echo "Downloading file: ${TAR_FILE}"
curl -o "${TAR_FILE}" "${DOWNLOAD_URL}"

if [[ ! -f "${TAR_FILE}" ]]; then
    echo "File download failed"
    exit 1
fi

# 3. Extract the file
echo "Extracting file: ${TAR_FILE}"
TEMP_DIR="temp_extracted_${DATE_ARG}"
mkdir -p "${TEMP_DIR}"
tar -xzf "${TAR_FILE}" -C "${TEMP_DIR}"

# 4. Process and upload each file
UPLOAD_STAGE="${STAGE}_${YEAR}_${MONTH}_${DAY}"

bendsql --dsn "${DSN}" --query="DROP STAGE IF EXISTS ${UPLOAD_STAGE}"
bendsql --dsn "${DSN}" --query="CREATE STAGE ${UPLOAD_STAGE}"

find "${TEMP_DIR}" -type f | while read -r FILE; do
    RELATIVE_PATH="${FILE#${TEMP_DIR}/}"

    # Get upload presigned URL
    UPLOAD_SQL="PRESIGN UPLOAD @${UPLOAD_STAGE}/${RELATIVE_PATH}"
    UPLOAD_URL=$(bendsql --dsn "${DSN}" --query="${UPLOAD_SQL}" | awk '{print $3}')

    if [[ -z "$UPLOAD_URL" ]]; then
        echo "Failed to get upload URL for: ${RELATIVE_PATH}"
        continue
    fi

    echo "Uploading file: ${RELATIVE_PATH}"
    curl -X PUT -T "${FILE}" "${UPLOAD_URL}"

    if [[ $? -eq 0 ]]; then
        echo "Upload successful: ${RELATIVE_PATH}"
    else
        echo "Upload failed: ${RELATIVE_PATH}"
    fi
done

# Cleanup temporary files
echo "Cleaning up temporary files"
rm -rf "${TEMP_DIR}"
rm -f "${TAR_FILE}"
echo "Temporary files cleanup is finished."

RESTORE_DATABASE="${STAGE}_${YEAR}_${MONTH}_${DAY}"
bendsql --dsn "${DSN}" --query="DROP DATABASE IF EXISTS ${RESTORE_DATABASE}"
bendsql --dsn "${DSN}" --query="CREATE DATABASE ${RESTORE_DATABASE}"

echo "Restoring the '${RESTORE_DATABASE}.columns' table..."
bendsql --dsn "${DSN}" --database "${RESTORE_DATABASE}" --query="create table columns like system.columns;"
bendsql --dsn "${DSN}" --database "${RESTORE_DATABASE}" --query="COPY INTO columns FROM @${UPLOAD_STAGE}/columns;"
echo "The '${RESTORE_DATABASE}.columns' table has been successfully restored"

echo "Restoring the '${RESTORE_DATABASE}.user_functions' table..."
bendsql --dsn "${DSN}" --database "${RESTORE_DATABASE}" --query="create table user_functions like system.user_functions;"
bendsql --dsn "${DSN}" --database "${RESTORE_DATABASE}" --query="COPY INTO user_functions FROM @${UPLOAD_STAGE}/user_functions;"
echo "The '${RESTORE_DATABASE}.user_functions' table has been successfully restored"

echo "Restoring the '${RESTORE_DATABASE}.log_history' table..."
bendsql --dsn "${DSN}" --database "${RESTORE_DATABASE}" --query="create table log_history like system_history.log_history;"
bendsql --dsn "${DSN}" --database "${RESTORE_DATABASE}" --query="COPY INTO log_history FROM @${UPLOAD_STAGE}/query_raw_logs;"
echo "The '${RESTORE_DATABASE}.log_history' table has been successfully restored"

echo "Restoring the '${RESTORE_DATABASE}.query_history' table..."
bendsql --dsn "${DSN}" --database "${RESTORE_DATABASE}" --query="create table query_history like system_history.query_history;"
bendsql --dsn "${DSN}" --database "${RESTORE_DATABASE}" --query="COPY INTO query_history FROM @${UPLOAD_STAGE}/query_logs;"
echo "The '${RESTORE_DATABASE}.query_history' table has been successfully restored"

echo "Restoring the '${RESTORE_DATABASE}.profile_history' table..."
bendsql --dsn "${DSN}" --database "${RESTORE_DATABASE}" --query="create table profile_history like system_history.profile_history;"
bendsql --dsn "${DSN}" --database "${RESTORE_DATABASE}" --query="COPY INTO profile_history FROM @${UPLOAD_STAGE}/query_profile_logs;"
echo "The '${RESTORE_DATABASE}.profile_history' table has been successfully restored"

echo "Processing completed"