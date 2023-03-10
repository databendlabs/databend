---
title: Monitoring with Sentry
---

With automated error reporting, you can not only get alerted when something breaks, but also access the details you need to reproduce the issue.

## Sentry

Databend provides integration with [Sentry](https://github.com/getsentry/sentry), a developer-first error tracking and performance monitoring platform.

### Deploy Sentry

You can use Sentry as a cloud service by signing up at [sentry.io](https://sentry.io), or you can host it yourself by following the instructions at [Self-Hosted Sentry](https://develop.sentry.dev/self-hosted/).

<img src="/img/tracing/sentry-hosted.png"/>

### Create a Project

To use Sentry with Databend, you need to create a project in Sentry and get its DSN (Data Source Name). The DSN is a unique identifier for your project that tells Sentry where to send your data. In this example, the DSN is `http://0c44e65426cb4f87ba059464c0740502@127.0.0.1:9000/5` 。

<img src="/img/tracing/sentry-get-dsn.png"/>

### Start Databend

You can start Databend with Sentry in two ways:

- **Enable Error Tracking Only**

  This option will only use the `sentry-log` feature, which will send error logs to Sentry.

  ```bash
  export DATABEND_SENTRY_DSN="<your-sentry-dsn>"
  ```

  <img src="/img/tracing/sentry-error.png"/>

- **Also Enable Performance Monitoring**

  Setting `SENTRY_TRACES_SAMPLE_RATE` greater than `0.0` will allow sentry to perform trace sampling, which will help set up performance monitoring.

  ```bash
  export DATABEND_SENTRY_DSN="<your-sentry-dsn>"
  export SENTRY_TRACES_SAMPLE_RATE=1.0 LOG_LEVEL=DEBUG
  ```

  **Note:** Set `SENTRY_TRACES_SAMPLE_RATE` a to lower value in production.

  <img src="/img/tracing/sentry-performance.png"/>
