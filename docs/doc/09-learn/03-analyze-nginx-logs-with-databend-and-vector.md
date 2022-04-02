---
title: Analyzing Nginx Logs with Databend and Vector
sidebar_label: Analyzing Nginx Logs
---

Systems are producing all kinds metrics and logs time by time, do you want to gather them and analyze the logs in real time? 

Databend provides [integration with Vector](../04-integrations/00-vector.md), easy to do it now!

Lets ingesting Nginx access logs into Databend from Vector step by step.


## Step 1. Databend

### 1.1 Deploy Databend

Make sure you have installed Databend, if not please see(Choose one):

* [How to Deploy Databend with Amazon S3](../01-deploy/01-s3.md)
* [How to Deploy Databend with Tencent COS](../01-deploy/02-cos.md)
* [How to Deploy Databend with Alibaba OSS](../01-deploy/03-oss.md)
* [How to Deploy Databend with Wasabi](../01-deploy/05-wasabi.md)
* [How to Deploy Databend with Scaleway OS](../01-deploy/06-scw.md)


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
    '"time":"$time_iso8601",' 
    '"client":"$remote_addr",'
    '"host":"$host",'
    '"uri":"$uri",'
    '"args":"$args",' 
    '"connection":$connection,' 
    '"content_type":"$content_type",'
    '"method":"$request_method",'
    '"request":"$request",'
    '"status":$status,'
    '"referer":"$http_referer",'
    '"user_agent":"$http_user_agent",'
    '"bytes_sent":$bytes_sent,'
    '"body_bytes_sent":$body_bytes_sent,'
    '"request_time":$request_time'
  '}';
 
        access_log /var/log/nginx/access.log json_combined;
        error_log /var/log/nginx/error.log;

        include /etc/nginx/conf.d/*.conf;
        include /etc/nginx/sites-enabled/*;
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

## Step 4. Analyze nginx log in Databend

### 4.1 Generate logs

Reload the home page at `http://localhost/xx/yy?mm=nn` many times.

### 4.2 View the logs in Databend

```shell
mysql -h127.0.0.1 -uroot -P3307 
```

```shell title='mysql>'
select * from nginx.access_logs;
```

```shell
+---------------------+--------+-----------+--------+-------+------------+--------------+--------+---------------------------+--------+---------+-----------------------------------------------------------------------------------------------------------+------------+-----------------+--------------+
| time                | client | host      | uri    | args  | connection | content_type | method | request                   | status | referer | user_agent                                                                                                | bytes_sent | body_bytes_sent | request_time |
+---------------------+--------+-----------+--------+-------+------------+--------------+--------+---------------------------+--------+---------+-----------------------------------------------------------------------------------------------------------+------------+-----------------+--------------+
| 2022-04-02 16:17:35 | ::1    | localhost | /xx/yy | mm=nn |          1 |              | GET    | GET /xx/yy?mm=nn HTTP/1.1 |    404 |         | Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36 |        728 |             564 |            0 |
| 2022-04-02 16:17:35 | ::1    | localhost | /xx/yy | mm=nn |          1 |              | GET    | GET /xx/yy?mm=nn HTTP/1.1 |    404 |         | Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36 |        728 |             564 |            0 |
+---------------------+--------+-----------+--------+-------+------------+--------------+--------+---------------------------+--------+---------+-----------------------------------------------------------------------------------------------------------+------------+-----------------+--------------+
```

### 4.3 Analyze the logs

- Top 10 Request IPs
```shell title='mysql>'
select count(*) as count from nginx.access_logs group by client order by count desc limit 10;
```
- Top 10 Request Pages
```shell title='mysql>'
select count(*) as count from nginx.access_logs group by request order by count desc limit 10;
```
- Top 10 HTTP 404 Pages
```shell title='mysql>'
select countif(status=404), uri as count from nginx.access_logs group by uri order by count desc limit 10;
```