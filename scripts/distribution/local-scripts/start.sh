echo "Stop old Databend instances"
killall -9 databend-meta
killall -9 databend-query
echo "Deploy new Databend(standalone)"
ulimit -n 65535
nohup bin/databend-meta --config-file=configs/databend-meta.toml 2>&1 >meta.log &
sleep 3
# export STORAGE_S3_ENABLE_VIRTUAL_HOST_STYLE=true
nohup bin/databend-query --config-file=configs/databend-query.toml 2>&1 >query.log &
sleep 3
tail -f meta.log query.log &
