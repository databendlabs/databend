echo "Checking sandbox health and stats endpoints"
status_url=$(find_status_url)
if [ -z "$status_url" ]; then
	echo "[Test Error] health endpoint not found in cloud-control.out" >&2
	exit 1
fi
check_health "$status_url"
stats_url="${status_url%/health}/stats"
check_stats "$stats_url"
