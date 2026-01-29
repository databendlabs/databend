## Databend Cloud Control grpc Server Tests


### How to run
#### setup cloud control mock grpc server
```sh
# Install dependencies using uv
uv sync
# start UDF server
uv run python simple_server.py
```
The mock UDF control plane will build and run a Docker
container from the `spec` sent by Databend (protobuf runtime spec).
This requires a working `docker` CLI and will return a local endpoint like
`http://127.0.0.1:<port>`.
#### make sure databend config is correct
you need to add the setting to your config.toml
```toml
[query]
cloud_control_grpc_server_address = "http://0.0.0.0:50051"
```

#### run sql-logic-test on cloud control server
```sh
./target/debug/databend-sqllogictests --run_dir task
```
