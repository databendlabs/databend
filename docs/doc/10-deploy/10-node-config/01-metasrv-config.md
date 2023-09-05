---
title: Meta Configurations
---

This page describes the Meta configurations available in the [databend-meta.toml](https://github.com/datafuselabs/databend/blob/main/scripts/distribution/configs/databend-meta.toml) configuration file.

- Some parameters listed in the table may not be present in [databend-meta.toml](https://github.com/datafuselabs/databend/blob/main/scripts/distribution/configs/databend-meta.toml). If you require these parameters, you can manually add them to the file.

- You can find [sample configuration files](https://github.com/datafuselabs/databend/tree/main/scripts/ci/deploy/config) on GitHub that set up Databend for various deployment environments. These files were created for internal testing ONLY. Please do NOT modify them for your own purposes. But if you have a similar deployment, it is a good idea to reference them when editing your own configuration files.

| Parameter                       | Description                                                                                                             |
|---------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| log_dir                         | Directory where Databend stores its log files.                                                                          |
| admin_api_address               | IP address and port for the admin API of Databend.                                                                      |
| grpc_api_address                | IP address and port for the gRPC API of Databend.                                                                       |
| grpc_api_advertise_host         | IP address used for advertising the gRPC API (used for updating Databend-meta endpoints).                               |
| [raft_config] id                  | Unique identifier for the Raft configuration.                                                                           |
| [raft_config] raft_dir            | Directory where Raft data is stored.                                                                                    |
| [raft_config] raft_api_port       | Port for the Raft API of Databend.                                                                                      |
| [raft_config] raft_listen_host    | IP address for Raft to listen on.                                                                                       |
| [raft_config] raft_advertise_host | IP address used for advertising the Raft API.                                                                           |
| single                          | Boolean indicating whether Databend should run in single-node cluster mode (true or false).                             |
| join                            | List of addresses (<raft_advertise_host>:<raft_api_port>) of nodes in an existing cluster that a new node is joined to. |