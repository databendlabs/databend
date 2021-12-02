---
title: Config
---

Get the config of the Databend query server.

## Examples

```
curl http://127.0.0.1:8080/v1/config

Config { log_level: "INFO", log_dir: "./_logs", num_cpus: 16, mysql_handler_host: "127.0.0.1", mysql_handler_port: 3307, max_active_sessions: 256, clickhouse_handler_host: "127.0.0.1", clickhouse_handler_port: 9000, flight_api_address: "127.0.0.1:9090", http_api_address: "127.0.0.1:8080", metric_api_address: "127.0.0.1:7070", store_api_address: "127.0.0.1:9191", store_api_username: ******, store_api_password: ******, config_file: "" }
```
