---
title: Setting Environment Variables
---

Databend offers you the flexibility to configure your Meta and Query nodes in two ways: using environment variables or configuration files. Moreover, you have the option to utilize environment variables to point to custom configuration files. This capability empowers you to make modifications without disrupting the default setups. This is especially advantageous when you require swift adjustments, are working with containers, or need to safeguard sensitive data.

:::note
- A mapping relationship exists between the parameters set through environment variables and those specified in configuration files. In cases where a configuration parameter is defined both via an environment variable and in a configuration file, Databend will prioritize the value provided by the environment variable.

- Not all configurations can be managed solely through environment variables. In some cases, adjustments might necessitate modifications within the configuration files rather than relying on environment variables.
:::

## Setting Configuration File Paths

METASRV_CONFIG_FILE and CONFIG_FILE are environment variables used to designate the locations of your configuration files, [databend-meta.toml](https://github.com/datafuselabs/databend/blob/main/scripts/distribution/configs/databend-meta.toml) and [databend-query.toml](https://github.com/datafuselabs/databend/blob/main/scripts/distribution/configs/databend-query.toml). They provide a way to tailor the paths of configuration files in Databend. If you'd like to depart from the default setup and opt for custom configuration file locations, these variables empower you to specify the exact paths for your files.

```sql title='Example'
export METASRV_CONFIG_FILE='/etc/databend/databend-meta.toml'
export CONFIG_FILE='/etc/databend/databend-query.toml'
```

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

The parameters under the [query] and [storage] sections in the configuration file [databend-query.toml](https://github.com/datafuselabs/databend/blob/main/scripts/distribution/configs/databend-query.toml) can be configured using environment variables. The names of the environment variables are formed by combining the word QUERY or STORAGE with the corresponding parameter names using underscores. 

Databend also accepts environment variables from storage services when they match Databend's environment variables. This allows you to work with the environment variable naming conventions you are most familiar with, eliminating the need to remember additional variable names and simplifying your configuration process.

To illustrate with an example, if you want to set the access key ID for S3 using an environment variable, you have the flexibility to use either STORAGE_S3_ACCESS_KEY_ID provided by Databend or the well-known AWS_ACCESS_KEY_ID typically associated with AWS S3.