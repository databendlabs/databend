---
title: Analyzing Nginx Logs with Databend and Vector
sidebar_label: Analyzing Nginx Logs
---

<p align="center">
<img src="https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/integration-databend-vector.png" width="550"/>
</p>

Systems are producing all kinds metrics and logs time by time, do you want to gather them and analyze the logs in real time? 

Databend provides [integration with Vector](../40-integrations/00-vector.md), easy to do it now!

Lets ingesting Nginx access logs into Databend from Vector step by step.


## Step 1. Databend

### 1.1 Deploy Databend

Make sure you have installed Databend, if not please see(Choose one):

* [How to Deploy Databend with Amazon S3](../10-deploy/01-s3.md)
* [How to Deploy Databend with Tencent COS](../10-deploy/02-cos.md)
* [How to Deploy Databend with Alibaba OSS](../10-deploy/03-oss.md)
* [How to Deploy Databend with Wasabi](../10-deploy/05-wasabi.md)
* [How to Deploy Databend with Scaleway OS](../10-deploy/06-scw.md)


### 1.2 Create a Database and Table

Connect to Databend server with MySQL client:
```shell
mysql -h127.0.0.1 -uroot -P3307 
```

```shell title='mysql>'
create database nginx;
```

```sql title='mysql>'
create table nginx.access_logs (
  `time` Datetime32 Null,
  `client` String Null,
  `host` String Null,
  `uri` String Null,
  `args` String Null,
  `connection` Int64 Null,
  `content_type` String Null,
  `method` String Null,
  `request` String Null,
  `status` Int32 Null,
  `referer` String Null,
  `user_agent` String Null,
  `bytes_sent` Int32 Null,
  `body_bytes_sent` Int32 Null,
  `request_time` Float32 Null
);
```

### 1.3 Create a User for Vector Auth

Connect to Databend server with MySQL client:
```shell
mysql -h127.0.0.1 -uroot -P3307 
```

```shell title='mysql>'
create user 'vector' identified by 'vector123';
```
Please replace `vector`, `vector123` to your own username and password.

```shell title='mysql>'
grant insert on nginx.* TO 'vector'@'%';
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

        log_format json_combined escape=json
  '{'
    '"time":"$time_iso8601",' # local time in the ISO 8601 standard format 
    '"client":"$remote_addr",' # client IP
    '"host":"$host",'
    '"uri":"$uri",'
    '"args":"$args",' # args
    '"connection":$connection,' # connection serial number
    '"content_type":"$content_type",'
    '"method":"$request_method",' # request method
    '"request":"$request",' # full path no arguments if the request
    '"status":$status,' # response status code
    '"referer":"$http_referer",' # HTTP referer
    '"user_agent":"$http_user_agent",' # HTTP user agent
    '"bytes_sent":$bytes_sent,'  # the number of bytes sent to a client
    '"body_bytes_sent":$body_bytes_sent,' # the number of body bytes exclude headers sent to a client
    '"request_time":$request_time' # request processing time in seconds with msec resolution
  '}';
 
        access_log /var/log/nginx/access.log json_combined;
        error_log /var/log/nginx/error.log;

        include /etc/nginx/conf.d/*.conf;
        include /etc/nginx/sites-enabled/*;
}
```
This is how the log message looks in JSON style log_format:
```json
{
  "time": "2022-04-03T09:02:36+08:00",
  "client": "127.0.0.1",
  "host": "localhost",
  "uri": "/db/abc",
  "args": "good=iphone&uuid=8308140226",
  "connection": 643334,
  "content_type": "",
  "method": "GET",
  "request": "GET /db/abc?good=iphone&uuid=8308140226 HTTP/1.1",
  "status": 404,
  "referer": "",
  "user_agent": "",
  "bytes_sent": 326,
  "body_bytes_sent": 162,
  "request_time": 0
}
```

Use the new `nginx.conf` replace your Nginx configuration and restart the Nginx server.

## Step 3. Vector

### 3.1 Install Vector

Your can [install Vector](https://vector.dev/docs/setup/installation/) with the installation script:

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.vector.dev | bash
```

### 3.2 Configure Vector

```shell title='vector.toml'
data_dir = "./vlog/"
[sources.nginx_access_logs]
  type         = "file"
  include      = ["/var/log/nginx/access.log"]
  ignore_older = 86400

[transforms.nginx_access_logs_json]
  type         = "remap"
  inputs       = ["nginx_access_logs"]
  source = '''
. = parse_json!(.message)
  #ISO8601/RFC3339, 2001-07-08T00:34:60.026490+09:30
.time = parse_timestamp!(.time, format: "%+")
  #2001-07-08 00:34:60
.time = format_timestamp!(.time, format: "%F %X")
'''

[sinks.databend_sink]
  type = "clickhouse"
  inputs = [ "nginx_access_logs_json" ]
  database = "nginx" #Your database
  table = "access_logs" #Your table
  #Databend ClickHouse REST API: http://{http_handler_host}:{http_handler_port}/clickhouse
  endpoint = "http://localhost:8000/clickhouse"
  compression = "gzip"

[sinks.databend_sink.auth]
  strategy = "basic"
  user = "vector" #Databend username
  password = "vector123" #Databend password
```
  
### 3.3 Run Vector

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
```shell title='mysql>'
select count() as count, status from nginx.access_logs group by status limit 10;
```

```shell
+-----------+--------+
| count     | status |
+-----------+--------+
| 106218701 |    404 |
+-----------+--------+
```

- __Top 10 Request Method__ 
```shell title='mysql>'
select count() as count, method from nginx.access_logs  group by method limit 10;
```

```shell
+-----------+--------+
| count     | method |
+-----------+--------+
| 106218701 | GET    |
+-----------+--------+
```

- __Top 10 Request IPs__
```shell title='mysql>'
select count(*) as count, client from nginx.access_logs group by client order by count desc limit 10;
```

```shell
+----------+-----------+
| count    | client    |
+----------+-----------+
| 98231357 | 127.0.0.1 |
|        2 | ::1       |
+----------+-----------+
```

- __Top 10 Request Pages__
```shell title='mysql>'
select count(*) as count, uri from nginx.access_logs group by uri order by count desc limit 10;
```

```shell
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
```shell title='mysql>'
select countif(status=404) as count, uri from nginx.access_logs group by uri order by count desc limit 10;
```

```shell
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
```shell title='mysql>'
select count(*) as count, request from nginx.access_logs group by request order by count desc limit 10;
```

```shell
+--------+---------------------------------------------------------------------------------------------------------+
| count  | request                                                                                                 |
+--------+---------------------------------------------------------------------------------------------------------+
| 199852 | GET /index.html HTTP/1.0                                                                                |
|   1000 | GET /db/abc?good=iphone&uuid=9329836906 HTTP/1.1                                                        |
|    900 | GET /miaosha/i/miaosha?goodsRandomName=0e67e331-c521-406a-b705-64e557c4c06c&mobile=17967444396 HTTP/1.1 |
|    900 | GET /miaosha/i/miaosha?goodsRandomName=0e67e331-c521-406a-b705-64e557c4c06c&mobile=16399821384 HTTP/1.1 |
|    900 | GET /miaosha/i/miaosha?goodsRandomName=0e67e331-c521-406a-b705-64e557c4c06c&mobile=17033481055 HTTP/1.1 |
|    900 | GET /miaosha/i/miaosha?goodsRandomName=0e67e331-c521-406a-b705-64e557c4c06c&mobile=17769945743 HTTP/1.1 |
|    900 | GET /miaosha/i/miaosha?goodsRandomName=0e67e331-c521-406a-b705-64e557c4c06c&mobile=15414263117 HTTP/1.1 |
|    900 | GET /miaosha/i/miaosha?goodsRandomName=0e67e331-c521-406a-b705-64e557c4c06c&mobile=18945218607 HTTP/1.1 |
|    900 | GET /miaosha/i/miaosha?goodsRandomName=0e67e331-c521-406a-b705-64e557c4c06c&mobile=19889051988 HTTP/1.1 |
|    900 | GET /miaosha/i/miaosha?goodsRandomName=0e67e331-c521-406a-b705-64e557c4c06c&mobile=15249667263 HTTP/1.1 |
+--------+---------------------------------------------------------------------------------------------------------+
```

**Enjoy your journey.** 
