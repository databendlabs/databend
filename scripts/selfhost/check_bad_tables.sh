#!/bin/bash

# Initialize variables
DSN=""
SHOW_HELP=false

# Function to display help
display_help() {
	cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --dsn <connection_string>  Specify the DSN connection string for bendsql (overrides BENDSQL_DSN)
  --help                     Display this help message and exit

Environment Variables:
  BENDSQL_DSN                Default DSN connection string if --dsn is not specified

This script performs the following operations:
1. Lists all databases
2. For each database, attempts to query its columns directly
3. If that fails, lists all tables in the database
4. For each table, attempts to query its columns
5. Reports any databases/tables where column queries fail

Examples:
  $0 --dsn "user:password@localhost:5432/dbname"
  BENDSQL_DSN="user:password@localhost:5432/dbname" $0
  $0 --help
EOF
}

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
	case $1 in
	--dsn)
		DSN="$2"
		shift
		;;
	--help) SHOW_HELP=true ;;
	*)
		echo "Error: Unknown parameter: $1"
		display_help
		exit 1
		;;
	esac
	shift
done

# Show help if requested
if [ "$SHOW_HELP" = true ]; then
	display_help
	exit 0
fi

# Check for DSN in environment variable if not specified via command line
if [ -z "$DSN" ] && [ -n "$BENDSQL_DSN" ]; then
	DSN="$BENDSQL_DSN"
fi

# Function to execute bendsql query and handle errors
execute_query() {
	local query="$1"
	local result
	local bendsql_cmd="bendsql"

	# Add DSN if specified
	if [ -n "$DSN" ]; then
		bendsql_cmd="$bendsql_cmd --dsn=\"$DSN\""
	fi

	bendsql_cmd="$bendsql_cmd --query=\"$query\""

	# Use eval to properly handle quotes in the command
	result=$(eval "$bendsql_cmd" 2>&1)
	if [ $? -ne 0 ]; then
		return 1
	fi
	echo "$result" | tail -n +2 # Skip header row
}

# Get all databases
echo "Fetching list of databases..."
databases=$(execute_query "SELECT name FROM system.databases")
if [ $? -ne 0 ]; then
	echo "Error: Failed to get databases list" >&2
	exit 1
fi

# Process each database
while IFS= read -r db; do
	if [ -z "$db" ]; then
		continue
	fi

	echo "Processing database: $db"

	# Try to get columns directly
	columns_query="SELECT * FROM system.columns WHERE database = '$db'"
	if ! execute_query "$columns_query" >/dev/null 2>&1; then
		# If error, get tables first
		tables=$(execute_query "SELECT name FROM system.tables WHERE database = '$db'")
		if [ $? -ne 0 ]; then
			echo "Error: Failed to get tables for database '$db'" >&2
			continue
		fi

		# Process each table
		while IFS= read -r table; do
			if [ -z "$table" ]; then
				continue
			fi

			echo "  Processing table: $table"
			# Try to get columns for this table
			table_columns_query="SELECT * FROM system.columns WHERE database = '$db' AND table = '$table'"
			if ! execute_query "$table_columns_query" >/dev/null 2>&1; then
				echo "Error encountered for database: '$db', table: '$table'"
			fi
		done <<<"$tables"
	fi
done <<<"$databases"

echo "Processing complete."
