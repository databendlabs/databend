echo "*-------------------------------------*"
echo "* setting up env RUST_LOG for tracing *"
echo "*-------------------------------------*"
echo ""

export RUST_LOG=info
export OTEL_BSP_MAX_EXPORT_BATCH_SIZE=1
export DATABEND_JAEGER=on

function set_env() {
	while [[ $# -gt 0 ]]; do
		arg="$1"
		case $arg in
		-d)
			echo "- flight rpc tracing enabled"
			export TRACE_RPC=",databend_query::api::rpc=debug"
			shift
			;;
		-p)
			echo "- parquet reader tracing enabled"
			export TRACE_PARQUET_READER=",common_streams::sources::source_parquet=debug"
			shift
			;;
		*)
			echo "! unknown option [$arg], ignored"
			shift
			;;
		esac
	done
}

if [ $# -eq 0 ]; then
	set_env "-d" "-p"
else
	set_env $@
fi

export LOG_LEVEL=${RUST_LOG}${TRACE_RPC}${TRACE_PARQUET_READER}
echo ""
echo "envs:"
echo "*** RUST_LOG=$RUST_LOG"
echo "*** OTEL_BSP_MAX_EXPORT_BATCH_SIZE=$OTEL_BSP_MAX_EXPORT_BATCH_SIZE"
echo "*** DATABEND_JAEGER=$DATABEND_JAEGER"
echo ""

echo "please be sure that jaeger is started"
echo "    e.g. docker run -d -p6831:6831/udp -p6832:6832/udp -p16686:16686 jaegertracing/all-in-one:latest"
