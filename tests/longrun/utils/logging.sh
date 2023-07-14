log_command() {
    command="$@"
    command_id=$(date +%s%N)  # Generate a unique ID based on the current timestamp in nanoseconds
    start_time=$(date +"%Y-%m-%d %H:%M:%S.%N")
    work_id="$WORK_ID"  # Assuming the work ID is stored in the WORK_ID environment variable

    # Log command start time and ID

    stdout=$(eval "$command" 2>&1)
    exit_code=$?
    end_time=$(date +"%Y-%m-%d %H:%M:%S.%N")
    elapsed_time=$(awk "BEGIN {printf \"%.6f\", $(date -d "$end_time" +%s.%N) - $(date -d "$start_time" +%s.%N)}")  # Calculate elapsed time as float

    command_escaped=$(echo "$command" | sed 's/"/\\"/g'| tr -d '"')
    stdout_escaped=$(echo "$stdout" | sed 's/"/\\"/g' | tr -d '"')
    csv_string=$(printf "\"%s\", \"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n" "$work_id" "$command_id" "$command_escaped" "$start_time" "$end_time" "$elapsed_time" "$exit_code" "$stdout_escaped")
    echo "$csv_string"
}

