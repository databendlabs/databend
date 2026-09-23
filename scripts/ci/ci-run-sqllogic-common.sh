#!/bin/bash

# Each CI key maps to one exact sqllogictest filter. Keep all --run_suite,
# --run_dir, and --skip_dir choices here instead of constructing them in callers.
sqllogic_filter() {
	unset SQLLOGIC_FILTER
	case "$1" in
		all) SQLLOGIC_FILTER=() ;;
		standalone-all) SQLLOGIC_FILTER=(--skip_dir management,ee,temp_table) ;;
		skip-management-all) SQLLOGIC_FILTER=(--skip_dir management,ee) ;;

		base) SQLLOGIC_FILTER=(--run_suite base) ;;
		crdb) SQLLOGIC_FILTER=(--run_suite crdb) ;;
		duckdb) SQLLOGIC_FILTER=(--run_suite duckdb) ;;
		ee) SQLLOGIC_FILTER=(--run_suite ee) ;;
		http_handler) SQLLOGIC_FILTER=(--run_suite http_handler) ;;
		management) SQLLOGIC_FILTER=(--run_suite management) ;;
		no_table_meta_cache) SQLLOGIC_FILTER=(--run_suite no_table_meta_cache) ;;
		paimon) SQLLOGIC_FILTER=(--run_dir paimon) ;;
		query) SQLLOGIC_FILTER=(--run_suite query,dictionaries) ;;
		stage) SQLLOGIC_FILTER=(--run_suite stage) ;;
		task) SQLLOGIC_FILTER=(--run_suite task) ;;
		tpcds) SQLLOGIC_FILTER=(--run_suite tpcds) ;;
		tpch) SQLLOGIC_FILTER=(--run_suite tpch) ;;
		tpch_iceberg) SQLLOGIC_FILTER=(--run_suite tpch_iceberg) ;;
		udf_native) SQLLOGIC_FILTER=(--run_suite udf_native) ;;
		udf_server) SQLLOGIC_FILTER=(--run_suite udf_server) ;;
		ydb) SQLLOGIC_FILTER=(--run_suite ydb) ;;

		cluster) SQLLOGIC_FILTER=(--run_dir cluster) ;;
		standalone) SQLLOGIC_FILTER=(--run_dir standalone) ;;
		temp-table) SQLLOGIC_FILTER=(--run_dir temp_table) ;;
	esac

	if ! declare -p SQLLOGIC_FILTER >/dev/null 2>&1; then
		echo "Unknown sqllogic CI filter: $1" >&2
		return 1
	fi
}
