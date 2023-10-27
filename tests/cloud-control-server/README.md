## Databend Cloud Control grpc Server Tests


### How to run
#### setup cloud control mock grpc server
```sh
pip install grpcio grpcio-reflection protobuf
# start UDF server
python3 simple_server.py
```
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

