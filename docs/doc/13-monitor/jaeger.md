---
title: Monitoring with Jaeger
---

Observability is key to understanding a system's behavior. Databend uses [OpenTelemetry](https://opentelemetry.io/) sdk to export tracing information to Jaeger.

## Jaeger

[Jaeger](https://github.com/jaegertracing/jaeger) is an open-source, end-to-end distributed tracing tool that originated from [Uber](https://www.uber.com/). It helps monitor and troubleshoot microservices-based applications.

If you're using Docker Compose for local development, there is already a Jaeger instance running on your local system. To use Jaeger in your production environment, use the following environment variables:

- `RUST_LOG` To control the log messages.
- `DATABEND_JAEGER_AGENT_ENDPOINT` The endpoint the Jaeger agent is listening on.

Here is a quick guide on how to monitor Databend with Jaeger:

### Deploy Jaeger

**Note:** For convenience, we use an All In One image to deploy Jaeger. If you already have a running Jaeger instance, you can skip this step.

```
docker run -d -p6831:6831/udp -p6832:6832/udp -p16686:16686 jaegertracing/all-in-one:latest
```

### Deploy Databend

You need to set environment variables according to your tracing level requirements and actual jaeger endpoint.

```bash
export RUST_LOG=DEBUG
export DATABEND_JAEGER_AGENT_ENDPOINT=localhost:6831
```

Please follow the [Deployment Guide](https://databend.rs/doc/deploy) to deploy Databend.

```bash
cd databend-v1.0.5-nightly-x86_64-unknown-linux-gnu
./scripts/start.sh
```

### Execute Some Query Operations (Optional)

By executing queries, we can trace some internal execution processes.

```sql
CREATE TABLE t1(a INT);
INSERT INTO t1 VALUES(1);
INSERT INTO t1 SELECT * FROM t1;
```

### Open Jaeger's Web UI

Open Jaeger’s Web UI to check tracing.

In this example, visit <http://127.0.0.1:16686/>.

![](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/jaeger-tracing-show.png)
