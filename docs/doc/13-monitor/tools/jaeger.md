---
title: Jaeger
---

[Jaeger](https://github.com/jaegertracing/jaeger) is an open-source, end-to-end distributed tracing tool that originated from [Uber](https://www.uber.com/). It helps monitor and troubleshoot microservices-based applications.

Databend has the ability to export tracing data to Jaeger by integrating with the [OpenTelemetry](https://opentelemetry.io/) SDK. The following tutorial shows you how to deploy and use Jaeger to trace Databend.

## Tutorial: Trace Databend with Jaeger

### Step 1. Deploy Jaeger

This tutorial uses the All In One image to deploy Jaeger in Docker. If you already have a running Jaeger instance, you can skip this step.

```bash
docker run --rm -d --name jaeger \
  -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 \
  -p 6831:6831/udp \
  -p 6832:6832/udp \
  -p 5778:5778 \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 14250:14250 \
  -p 14268:14268 \
  -p 14269:14269 \
  -p 9411:9411 \
  jaegertracing/all-in-one:latest
```

### Step 2. Set Environment Variables

Set the following environment variables according to your actual tracing level requirements and Jaeger endpoint.

- `DATABEND_TRACING_CAPTURE_LOG_LEVEL`: Sets the log level that will attach to spans.  
- `DATABEND_OTEL_EXPORTER_OTLP_ENDPOINT`: Sets the endpoint the OpenTelemetry Collector is listening on.

```bash
export DATABEND_TRACING_CAPTURE_LOG_LEVEL=DEBUG
export DATABEND_OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317
```

### Step 3. Deploy Databend

1. Follow the [Deployment Guide](https://databend.rs/doc/deploy) to deploy Databend.

2. Run the following SQL statements:

```sql
CREATE TABLE t1(a INT);
INSERT INTO t1 VALUES(1);
INSERT INTO t1 SELECT * FROM t1;
```

### Step 4. Check Tracing Information on Jaegar

1. Go to <http://127.0.0.1:16686/> and select the **Search** tab.

2. Select a service in the **Service** drop-down list. For example, select the databend-query service.

3. Click **Find Traces** to show the traces.

![](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/jaeger-tracing-show.png)
