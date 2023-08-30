---
title: Configuring with Environment Variables
---

Databend provides the option to configure your Meta and Query nodes using environment variables in addition to configuration files. This method proves useful for making adjustments without directly modifying configuration files, and it's particularly effective for enabling dynamic changes, containerized deployments, and secure handling of sensitive data.

It's important to note that there is a mapping relationship between the parameters set in environment variables and those specified in configuration files. In cases where a configuration parameter is defined both via an environment variable and in a configuration file, Databend will prioritize the value provided by the environment variable.

## Meta Environment Variables

Below is a list of available environment variables, each correspondingly mapped to parameters found in the configuration file [databend-meta.toml](https://github.com/datafuselabs/databend/blob/main/scripts/distribution/configs/databend-meta.toml). For detailed explanations of each parameter, see [Meta Configurations](01-metasrv-config.md).

| Environment Variable            	| Mapped to               	|
|---------------------------------	|-------------------------	|
| METASRV_LOG_DIR                 	| log_dir                 	|
| ADMIN_API_ADDRESS               	| admin_api_address       	|
| METASRV_GRPC_API_ADDRESS        	| grpc_api_address        	|
| METASRV_GRPC_API_ADVERTISE_HOST 	| grpc_api_advertise_host 	|
| KVSRV_ID                        	| id                      	|
| KVSRV_RAFT_DIR                  	| raft_dir                	|
| KVSRV_API_PORT                  	| raft_api_port           	|
| KVSRV_LISTEN_HOST               	| raft_listen_host        	|
| KVSRV_ADVERTISE_HOST            	| raft_advertise_host     	|
| KVSRV_SINGLE                    	| single                  	|

## Query Environment Variables

The parameters under the [query] and [storage] sections in the configuration file [databend-query.toml](https://github.com/datafuselabs/databend/blob/main/scripts/distribution/configs/databend-query.toml) can be configured using environment variables. 

The names of the environment variables are formed by combining the word QUERY or STORAGE with the corresponding parameter names using underscores. For example, the environment variable for the parameter **admin_api_address** under the [query] section is QUERY_ADMIN_API_ADDRESS, and the environment variable for the parameter **bucket** under the [storage.s3] section is STORAGE_S3_BUCKET.