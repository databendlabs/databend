---
title: Ingest Nginx Ingress Access Logs from Kubernetes into Databend using Vector
sidebar_label: Ingest Nginx Logs with Vector
---

# Ingest Nginx Ingress Access Logs from Kubernetes into Databend using Vector

## Step 1. Deploy Databend

We provided a [guide](https://databend.rs/doc/deploy/cluster_k8s_minio) to deploy a databend query cluster on kubernetes.


## Step 2. Create a database and table

```sql
CREATE DATABASE nginx;

CREATE TABLE nginx.access_logs
(
    namespace        String NOT NULL,
    ingress_name     String NOT NULL,
    service_name     String NOT NULL,
    time             DateTime NOT NULL,
    remote_addr      String NOT NULL,
    x_forward_for    String NOT NULL,
    request_id       String NOT NULL,
    remote_user      String NOT NULL,
    bytes_sent       UInt32 NOT NULL,
    request_time     Float32 NOT NULL,
    status           UInt32 NOT NULL,
    vhost            String NOT NULL,
    request_proto    String NOT NULL,
    path             String NOT NULL,
    request_query    String NOT NULL,
    request_length   UInt32 NOT NULL,
    duration         UInt32 NOT NULL,
    method           String NOT NULL,
    http_referrer    String NOT NULL,
    http_user_agent  String NOT NULL
);
```


## Step 3. Deploy & Configure Ingress

> If you haven't had a nginx ingress controller, you can refer to the [official installation guide](https://kubernetes.github.io/ingress-nginx/deploy/).

We would change the nginx log format to `JSON` here to avoid complicated and costing regex parsing in vector, ref: [log-format-upstream](https://kubernetes.github.io/ingress-nginx/user-guide/nginx-configuration/configmap/#log-format-upstream)

:::tip
We could add additional fields `namespace`, `ingress_name`, `service_name`, `service_port` supported by nginx ingress controller, ref: [log-format-fields](https://kubernetes.github.io/ingress-nginx/user-guide/nginx-configuration/log-format/#log-format)
:::


```yaml title='ingress-nginx-controller-configmap.yaml'
apiVersion: v1
kind: ConfigMap
metadata:
  name: ingress-nginx-controller
  namespace: ingress-nginx
data:
  log-format-upstream: '{"time":"$time_iso8601", "namespace":"$namespace", "ingress_name":"$ingress_name", "service_name":"$service_name", "remote_addr":"$proxy_protocol_addr", "x_forward_for":"$proxy_add_x_forwarded_for", "request_id":"$req_id", "remote_user":"$remote_user", "bytes_sent":$bytes_sent, "request_time":$request_time, "status":$status, "vhost":"$host", "request_proto":"$server_protocol", "path":"$uri", "request_query":"$args", "request_length":$request_length, "duration":$request_time, "method":"$request_method", "http_referrer":"$http_referer", "http_user_agent":"$http_user_agent"}'
```

ref: [custom-configuration](https://kubernetes.github.io/ingress-nginx/examples/customization/custom-configuration/)

```shell
kubectl apply -f ingress-nginx-controller-configmap.yaml
```

Now the nginx access logs would be in json format.

## Step 4. deploy a service with ingress

For example, we can deploy a httpbin service for test.

```
kubectl apply -f https://raw.githubusercontent.com/istio/istio/master/samples/httpbin/httpbin.yaml
```

And a ingress for the httpbin service

```yaml title='httpbin-ingress.yaml'
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: httpbin
spec:
  ingressClassName: nginx
  rules:
  - host: httpbin.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: httpbin
            port:
              number: 8000
```

```shell
kubectl apply -f httpbin-ingress.yaml
```

Well done! Now after making request with curl:
```shell
curl http://httpbin.example.com/get
```

you can see access logs from ingress container logs like

```json
{"time":"2022-03-24T16:47:44+00:00", "namespace":"default", "ingress_name":"httpbin", "service_name":"httpbin", "remote_addr":"-", "x_forward_for":"172.17.0.1", "request_id":"da3d880fc74fc53da4e5afd11858e3e0", "remote_user":"-", "bytes_sent":551, "request_time":0.011, "status":200, "vhost":"httpbin.example.com", "request_proto":"HTTP/1.1", "path":"/get", "request_query":"-", "request_length":142, "duration":0.011, "method":"GET", "http_referrer":"-", "http_user_agent":"curl/7.79.1"}
```

## Step 5. Install & Configure Vector

[Installing via helm](https://vector.dev/docs/setup/installation/package-managers/helm/) is recommended, and we should change the config to our own one, steps are as follows.

First of all we add helm repo for vector:

```shell
helm repo add vector https://helm.vector.dev
helm repo update
```

Then we create a namespace for vector agents:

```shell
kubectl create namespace --dry-run=client -o yaml vector | kubectl apply -f -
```

and a configmap for our agents:

```yaml title='agent.yaml'
data_dir: /vector-data-dir
sources:
  nginx_ingress_logs:
    type: kubernetes_logs
    extra_field_selector: metadata.namespace=ingress-nginx
    extra_label_selector: app.kubernetes.io/instance=ingress-nginx,app.kubernetes.io/component=controller
    max_read_bytes: 2048
    max_line_bytes: 32768
    fingerprint_lines: 1
    glob_minimum_cooldown_ms: 60000
    delay_deletion_ms: 60000
transforms:
  extract_nginx_ingress_logs:
    type: remap
    inputs:
      - nginx_ingress_logs
    source: |-
      . = parse_json!(.message)
sinks:
  sink_to_databend:
    type: clickhouse
    inputs:
      - extract_nginx_ingress_logs
    database: nginx
    endpoint: http://query-service.tenant1:8000/v1/clickhouse
    table: access_logs
    compression: gzip
    healthcheck:
      enabled: true
    skip_unknown_fields: true
```

```shell
kubectl create configmap --dry-run=client -o yaml vector-agent-config --namespace vector --from-file=./agent.yaml | kubectl apply -f -
```

At last, we deploy vector with our configmap:

```shell
helm upgrade --install vector vector/vector \
  --namespace vector \
  --set "role=Agent" \
  --set "existingConfigMaps={vector-agent-config}" \
  --set "service.enabled=false" \
  --set "dataDir=/vector-data-dir"
```

## Step 6. View the logs in Databend
