---
title: Analyzing Nginx Logs with Databend and Vector
sidebar_label: Analyzing Nginx Logs on S3
---

Systems are producing all kinds metrics and logs time by time, do you want to gather them and analyze in real time? 

Databend provides integration with Vector, easy to do it now!

Lets ingesting Nginx access logs into Databend from Vector step by step.


## Step 1. Deploy Databend

Make sure you have installed Databend, if not please see(Choose one):

* [How to Deploy Databend with Amazon S3](../01-deploy/01-s3.md)
* [How to Deploy Databend with Tencent COS](../01-deploy/02-cos.md)
* [How to Deploy Databend with Alibaba OSS](../01-deploy/03-oss.md)
* [How to Deploy Databend with Wasabi](../01-deploy/05-wasabi.md)
* [How to Deploy Databend with Scaleway OS](../01-deploy/06-scw.md)


## Step 2. Create a Database and Table

```shell
mysql -h127.0.0.1 -uroot -P3307 
```

```shell title='mysql>'
create database nginx;
```

```shell title='mysql>'
create table nginx.access_logs (
  `agent` String,
  `client` String,
  `file` String,
  `host` String,
  `message` String,
  `path` String,
  `method` String,
  `request` String,
  `protocol` String,
  `size` Int32,
  `source_type` String,
  `status` Int32,
  `timestamp` String,
);
```

## Step 3. Nginx configuration

```shell title='nginx.conf'
user www-data;
worker_processes 4;
pid /var/run/nginx.pid;

events {
        worker_connections 768;
        # multi_accept on;
}

http {
        ##
        # Basic Settings
        ##

        sendfile on;
        tcp_nopush on;
        tcp_nodelay on;
        keepalive_timeout 65;
        types_hash_max_size 2048;
        include /etc/nginx/mime.types;
        default_type application/octet-stream;

        ##
        # Logging Settings
        ##

        access_log /var/log/nginx/access.log combined;
        error_log /var/log/nginx/error.log;


        ##
        # Virtual Host Configs
        ##

        include /etc/nginx/conf.d/*.conf;
        include /etc/nginx/sites-enabled/*;
}
```

## Step 4. Vector

### 4.1 Installation Vector

Your can [install Vector](https://vector.dev/docs/setup/installation/) with the installation script:

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.vector.dev | bash
```

### 4.2 Configuration Vector

```shell title='vector.toml'
data_dir = "./vlog/"
[sources.nginx_access_logs]
  type         = "file"
  include      = ["/var/log/nginx/access.log"]
  ignore_older = 86400

[transforms.parsed_nginx]
  type = "remap"
  inputs = ["nginx_access_logs"]
  source = '''
  parsed, err = parse_nginx_log(.message, "combined")
  if is_null(err) {
    . = merge(., parsed)
  }
  '''

[sinks.databend_sink]
  type = "clickhouse"
  inputs = [ "parsed_nginx" ]
  database = "nginx"
  table = "access_logs"
  endpoint = "http://127.0.0.1:8000/clickhouse"
  compression = "gzip"

[sinks.console]
  type = "console"
  inputs = ["parsed_nginx"] # required
  target = "stdout"
  encoding.codec = "json" # required
```

### 4.3 Run Vector

```shell
vector -c ./vector.toml
```

## Step 5. Analyze the logs in Databend

### 5.1 Generate logs

Reload the home page at http://localhost/ many times.

### 5.2 View the logs in Databend

```shell
mysql -h127.0.0.1 -uroot -P3307 
```

