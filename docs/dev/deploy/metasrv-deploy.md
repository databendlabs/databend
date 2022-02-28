## Deploy metasrv cluster of 3 nodes


A metasrv cluster is formed in 2 steps:
1. Bring up a cluster of only one node.
2. Add other nodes into this cluster.

The config files are almost the same except the cluster formation arg:
`--single` for the first node, `--join <add>` for other nodes.


### Create a config file for the first node:


```sh
# cat metasrv-1.toml

log_dir            = "./_logs1"
metric_api_address = "0.0.0.0:28100"
admin_api_address  = "0.0.0.0:28101"
grpc_api_address   = "0.0.0.0:9191"

[raft_config]
id            = 1
raft_dir      = "./_meta1"
raft_api_port = 28103

# assign raft_{listen|advertise}_host in test config,
# so if something wrong in raft meta nodes communication we will catch bug in unit tests.
raft_listen_host = "127.0.0.1"
raft_advertise_host = "localhost"

# Start up mode: single node cluster
single        = true
```

- `metric_api_address` is the service for metrics collection.
- `admin_api_address` is the service for retrieving cluster status.
- `grpc_api_address` is the service for applications to write or read metadata.

- `raft_config.id` is the globally unique id for this node; it is a `u64`.
- `raft_config.raft_dir` is the local dir to store metadata, including raft log
    and state machine etc.

- `raft_config.raft_api_port`,`raft_config.raft_listen_host` and `raft_config.raft_advertise_host`
  defines the service for internal raft communication.  Application should never touch this port.

  `raft_listen_host` is the host the internal raft server listens on.
  `raft_advertise_host` is the host the internal raft client to connect to.

- `single` tells the node to initialize a single node cluster if it is not
    initialized. Otherwise, this arg is just ignored.


### Start the first node.

```sh
./target/debug/databend-meta -c metasrv-1.toml
```

### Create config files for other nodes:

The config for other nodes are similar, except the `single` should be replaced
with `join`, and the `id` has to be different.

```sh
# cat metasrv-2.toml

log_dir            = "./_logs2"
metric_api_address = "0.0.0.0:28200"
admin_api_address  = "0.0.0.0:28201"
grpc_api_address   = "0.0.0.0:28202"

[raft_config]
id            = 2
raft_dir      = "./_meta2"
raft_api_port = 28203

raft_listen_host = "127.0.0.1"
raft_advertise_host = "localhost"

# Start up mode: join a cluster
join          = ["127.0.0.1:28103"]
```

The arg `join` specifies a list of addresses of nodes in the cluster it wants to
be joined to.


### Start other nodes.

```sh
./target/debug/databend-meta -c metasrv-2.toml &
./target/debug/databend-meta -c metasrv-3.toml &
```
