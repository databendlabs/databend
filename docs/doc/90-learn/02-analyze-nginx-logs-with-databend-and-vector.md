---
title: Analyzing Nginx Access Logs With Databend
sidebar_label: Analyzing Nginx Logs
---

<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/integration-databend-vector.png" width="550"/>
</p>

Systems are producing all kinds metrics and logs time by time, do you want to gather them and analyze the logs in real time? 

Databend provides [integration with Vector](../40-integrations/10-data-tool/00-vector.md), easy to do it now!

Lets ingesting Nginx access logs into Databend from Vector step by step.


## Step 1. Databend

### 1.1 Deploy Databend

Make sure you have installed Databend, if not please see(Choose one):

* [How to Deploy Databend With Amazon S3](../10-deploy/01-s3.md)
* [How to Deploy Databend With Tencent COS](../10-deploy/02-cos.md)
* [How to Deploy Databend With Alibaba OSS](../10-deploy/03-oss.md)
* [How to Deploy Databend With Wasabi](../10-deploy/05-wasabi.md)
* [How to Deploy Databend With Scaleway OS](../10-deploy/06-scw.md)


### 1.2 Create a Database and Table

Connect to Databend server with MySQL client:
```shell
mysql -h127.0.0.1 -uroot -P3307 
```

```sql
CREATE DATABASE nginx;
```

```sql
CREATE TABLE nginx.access_logs (
  `timestamp` TIMESTAMP,
  `remote_addr` VARCHAR,
  `remote_port` INT,
  `request_method` VARCHAR,
  `request_uri` VARCHAR,
  `server_protocol` VARCHAR,
  `status` INT,
  `bytes_sent` INT,
  `http_referer` VARCHAR,
  `http_user_agent` VARCHAR,
  `upstream_addr` VARCHAR,
  `scheme` VARCHAR,
  `gzip_ratio` VARCHAR,
  `request_length` INT,
  `request_time` FLOAT,
  `ssl_protocol` VARCHAR,
  `upstream_response_time` VARCHAR
);
```

### 1.3 Create a User for Vector Auth

Connect to Databend server with MySQL client:
```shell
mysql -h127.0.0.1 -uroot -P3307 
```

Create a user:
```sql
CREATE USER user1 IDENTIFIED BY 'abc123';
```

Grant privileges for the user:
```sql
GRANT INSERT ON nginx.* TO user1;
```

## Step 2. Nginx

### 2.1 Install Nginx

If you don't install Nginx, please refer to [How to Install Nginx](https://www.nginx.com/resources/wiki/start/topics/tutorials/install/).

### 2.2 Configure Nginx

```shell title='nginx.conf'
user www-data;
worker_processes 4;
pid /var/run/nginx.pid;

events {
        worker_connections 768;
}

http {
        ##
        # Logging Settings
        ##
	    log_format upstream '$remote_addr "$time_local" $host "$request_method $request_uri $server_protocol" $status $bytes_sent "$http_referer" "$http_user_agent" $remote_port $upstream_addr $scheme $gzip_ratio $request_length $request_time $ssl_protocol "$upstream_response_time"';

        access_log /var/log/nginx/access.log upstream;
        error_log /var/log/nginx/error.log;

        include /etc/nginx/conf.d/*.conf;
        include /etc/nginx/sites-enabled/*;
}
```
This is how the log message looks:
```text
::1 "09/Apr/2022:11:13:39 +0800" localhost "GET /?xx HTTP/1.1" 304 189 "-" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36" 50758 - http - 1202 0.000 - "-"
```

Use the new `nginx.conf` replace your Nginx configuration and restart the Nginx server.

## Step 3. Vector

### 3.1 Install Vector

Your can [install Vector](https://vector.dev/docs/setup/installation/) with the installation script:

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.vector.dev | bash
```

### 3.2 Configure Vector

```toml title='vector.toml'
[sources.nginx_access_log]
type = "file"
// highlight-next-line
include = ["/var/log/nginx/access.log"]
file_key = "file"
max_read_bytes = 10240

[transforms.nginx_access_log_parser]
type = "remap"
inputs = ["nginx_access_log"]
drop_on_error = true

// highlight-next-line
#nginx log_format upstream '$remote_addr "$time_local" $host "$request_method $request_uri $server_protocol" $status $bytes_sent "$http_referer" "$http_user_agent" $remote_port $upstream_addr $scheme $gzip_ratio $request_length $request_time $ssl_protocol "$upstream_response_time"';

source = """
    parsed_log, err = parse_regex(.message, r'^(?P<remote_addr>\\S+) \
\"(?P<time_local>\\S+ \\S+)\" \
(?P<host>\\S+) \
\"(?P<request_method>\\S+) (?P<request_uri>.+) (?P<server_protocol>HTTP/\\S+)\" \
(?P<status>\\d+) \
(?P<bytes_sent>\\d+) \
\"(?P<http_referer>.*)\" \
\"(?P<http_user_agent>.*)\" \
(?P<remote_port>\\d+) \
(?P<upstream_addr>.+) \
(?P<scheme>\\S+) \
(?P<gzip_ratio>\\S+) \
(?P<request_length>\\d+) \
(?P<request_time>\\S+) \
(?P<ssl_protocol>\\S+) \
\"(?P<upstream_response_time>.+)\"$')
    if err != null {
      log("Unable to parse access log: " + string!(.message), level: "warn")
      abort
    }
    . = merge(., parsed_log)
    .timestamp = parse_timestamp!(.time_local, format: "%d/%b/%Y:%H:%M:%S %z")
    .timestamp = format_timestamp!(.timestamp, format: "%F %X")

    # Convert from string into integer.
    .remote_port, err = to_int(.remote_port)
    if err != null {
      log("Unable to parse access log: " + string!(.remote_port), level: "warn")
      abort
    }

    # Convert from string into integer.
    .status, err  = to_int(.status)
    if err != null {
      log("Unable to parse access log: " + string!(.status), level: "warn")
      abort
    }

    # Convert from string into integer.
    .bytes_sent, err = to_int(.bytes_sent)
    if err != null {
      log("Unable to parse access log: " + string!(.bytes_sent), level: "warn")
      abort
    }

    # Convert from string into integer.
    .request_length, err = to_int(.request_length)
    if err != null {
      log("Unable to parse access log: " + string!(.request_length), level: "warn")
      abort
    }

    # Convert from string into float.
    .request_time, err = to_float(.request_time)
    if err != null {
      log("Unable to parse access log: " + string!(.request_time), level: "warn")
      abort
    }
  """


[sinks.nginx_access_log_to_databend]
  type = "clickhouse"
  inputs = ["nginx_access_log_parser"]
  // highlight-next-line
  database = "nginx" #Your database
  // highlight-next-line
  table = "access_logs" #Your table
  #Databend ClickHouse REST API: http://{http_handler_host}:{http_handler_port}/clickhouse
  // highlight-next-line
  endpoint = "http://localhost:8000/clickhouse"
  compression = "gzip"


[sinks.nginx_access_log_to_databend.auth]
  strategy = "basic"
  // highlight-next-line
  user = "user1" #Databend username
  // highlight-next-line
  password = "abc123" #Databend password

[[tests]]
name = "extract fields from access log"

[[tests.inputs]]
insert_at = "nginx_access_log_parser"
type = "raw"
// highlight-next-line
value = '::1 "09/Apr/2022:11:13:39 +0800" localhost "GET /?xx HTTP/1.1" 304 189 "-" "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36" 50758 - http - 1202 0.000 - "-"'

[[tests.outputs]]
extract_from = "nginx_access_log_parser"

[[tests.outputs.conditions]]
type = "vrl"
source = """
        assert_eq!(.remote_addr, "::1")
        assert_eq!(.time_local, "09/Apr/2022:11:13:39 +0800")
        assert_eq!(.timestamp, "2022-04-09 03:13:39")
        assert_eq!(.request_method, "GET")
        assert_eq!(.request_uri, "/?xx")
        assert_eq!(.server_protocol, "HTTP/1.1")
        assert_eq!(.status, 304)
        assert_eq!(.bytes_sent, 189)
        assert_eq!(.http_referer, "-")
        assert_eq!(.http_user_agent, "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36")
        assert_eq!(.remote_port, 50758)
        assert_eq!(.upstream_addr, "-")
        assert_eq!(.scheme, "http")
        assert_eq!(.gzip_ratio, "-")
        assert_eq!(.request_length, 1202)
        assert_eq!(.request_time, 0.000)
        assert_eq!(.ssl_protocol, "-")
        assert_eq!(.upstream_response_time, "-")
      """

[[tests]]
name = "no event from wrong access log"
no_outputs_from = ["nginx_access_log_parser"]

[[tests.inputs]]
insert_at = "nginx_access_log_parser"
type = "raw"
value = 'I am not access log'

```

### 3.3 Configuration Validation

Check the `nginx_access_log_parser` transform works or not:

```shell
vector test ./vector.toml
```

If it works the output is:
```shell
Running tests
test extract fields from access log ... passed
2022-04-09T04:03:09.704557Z  WARN transform{component_kind="transform" component_id=nginx_access_log_parser component_type=remap component_name=nginx_access_log_parser}: vrl_stdlib::log: "Unable to parse access log: I am not access log" internal_log_rate_secs=1 vrl_position=479
test no event from wrong access log ... passed
```
  
### 3.4 Run Vector

```shell
vector -c ./vector.toml
```

## Step 4. Analyze Nginx Log in Databend

### 4.1 Generate logs

Reload the home page at `http://localhost/xx/yy?mm=nn` many times, or using the [wrk](https://github.com/wg/wrk) HTTP benchmarking tool to generate a large amount Nginx logs quickly:

```shell
wrk -t12 -c400 -d30s http://localhost
```

### 4.2 Analyzing the Nginx Access Logs in Databend

```shell
mysql -h127.0.0.1 -uroot -P3307 
```

- __Top 10 Request Status__
```sql
SELECT count() AS count, status FROM nginx.access_logs GROUP BY status LIMIT 10;
+-----------+--------+
| count     | status |
+-----------+--------+
| 106218701 |    404 |
+-----------+--------+
```

- __Top 10 Request Method__ 
```sql
SELECT count() AS count, request_method FROM nginx.access_logs GROUP BY request_method LIMIT 10;
+-----------+----------------+
| count     | request_method |
+-----------+----------------+
| 106218701 |      GET       |
+-----------+----------------+
```

- __Top 10 Request IPs__
```sql
SELECT count(*) AS Count, remote_addr AS client FROM nginx.access_logs GROUP BY client ORDER BY count DESC LIMIT 10;
+----------+-----------+
| count    | client    |
+----------+-----------+
| 98231357 | 127.0.0.1 |
|        2 | ::1       |
+----------+-----------+
```

- __Top 10 Request Pages__
```sql
SELECT count(*) AS count, request_uri AS uri FROM nginx.access_logs GROUP BY uri ORDER BY count DESC LIMIT 10;
+----------+--------------------+
| count    | uri                |
+----------+--------------------+
| 60645174 | /db/abc            |
| 41727953 | /a/b/c/d/e/f/d     |
|   199852 | /index.html        |
|        2 | /xx/yy             |
+----------+--------------------+
```


- __Top 10 HTTP 404 Pages__
```sql
SELECT count_if(status=404) AS count, request_uri AS uri FROM nginx.access_logs GROUP BY uri ORDER BY count DESC LIMIT 10;
+----------+--------------------+
| count    | uri                |
+----------+--------------------+
| 64290894 | /db/abc            |
| 41727953 | /a/b/c/d/e/f/d     |
|   199852 | /index.html        |
|        2 | /xx/yy             |
+----------+--------------------+
```

- __Top 10 Requests__
```sql
SELECT count(*) AS count, request_uri AS request FROM nginx.access_logs GROUP BY request ORDER BY count DESC LIMIT 10;
+--------+-----------------------------------------------------------------------------------------------------+
| count  | request                                                                                             |
+--------+-----------------------------------------------------------------------------------------------------+
| 199852 | /index.html HTTP/1.0                                                                                |
|   1000 | /db/abc?good=iphone&uuid=9329836906 HTTP/1.1                                                        |
|    900 | /miaosha/i/miaosha?goodsRandomName=0e67e331-c521-406a-b705-64e557c4c06c&mobile=17967444396 HTTP/1.1 |
|    900 | /miaosha/i/miaosha?goodsRandomName=0e67e331-c521-406a-b705-64e557c4c06c&mobile=16399821384 HTTP/1.1 |
|    900 | /miaosha/i/miaosha?goodsRandomName=0e67e331-c521-406a-b705-64e557c4c06c&mobile=17033481055 HTTP/1.1 |
|    900 | /miaosha/i/miaosha?goodsRandomName=0e67e331-c521-406a-b705-64e557c4c06c&mobile=17769945743 HTTP/1.1 |
|    900 | /miaosha/i/miaosha?goodsRandomName=0e67e331-c521-406a-b705-64e557c4c06c&mobile=15414263117 HTTP/1.1 |
|    900 | /miaosha/i/miaosha?goodsRandomName=0e67e331-c521-406a-b705-64e557c4c06c&mobile=18945218607 HTTP/1.1 |
|    900 | /miaosha/i/miaosha?goodsRandomName=0e67e331-c521-406a-b705-64e557c4c06c&mobile=19889051988 HTTP/1.1 |
|    900 | /miaosha/i/miaosha?goodsRandomName=0e67e331-c521-406a-b705-64e557c4c06c&mobile=15249667263 HTTP/1.1 |
+--------+-----------------------------------------------------------------------------------------------------+
```

**Enjoy your journey.** 
