# Query Service Trace Debugging

## Structure

The trace debugging support under `src/query/service/tests/it/trace_debug/` is intentionally split
into two layers:

- Infrastructure: shared capture, tree formatting, raw span formatting, and runtime helpers.
- Application: concrete debugging-oriented test entry points that export complete trace data.

Files:

- `infra.rs`
  - Common tracing test infrastructure.
- `direct.rs`
  - Direct SQL trace debugging test.
- `http.rs`
  - Self-contained HTTP query trace debugging OTLP dump utility.

## HTTP Query Trace Debugging

### Test Name

- `test_dump_http_query_trace_debug`

### Purpose

Inspect real HTTP query tracing, including:

- `POST /v1/query`;
- follow-up page pulls;
- query finalization;
- executor spans under the HTTP request path.
- No trace-shape assertions; this utility writes complete OTLP JSON traces.

### How To Run

```bash
cargo test -p databend-query --test it test_dump_http_query_trace_debug -- --ignored --nocapture
```

### Environment Variables

- `DATABEND_TRACE_DEBUG_SQLS`
- `DATABEND_TRACE_DEBUG_WAIT_SECS`
- `DATABEND_TRACE_DEBUG_MAX_ROWS_PER_PAGE`
- `DATABEND_TRACE_DEBUG_MAX_PAGES`
- `DATABEND_TRACE_DEBUG_OUTPUT_DIR`

## Direct SQL Trace Debugging

### Test Name

- `test_dump_direct_sql_trace_debug`

### Purpose

Bypass HTTP and inspect tracing across:

- SQL parsing;
- planning;
- interpreter construction;
- pipeline execution;
- result stream collection.
- The output is a complete OTLP JSON export request instead of an ad hoc summary file.

### How To Run

```bash
cargo test -p databend-query --test it test_dump_direct_sql_trace_debug -- --ignored --nocapture
```

### Environment Variables

- `DATABEND_DIRECT_TRACE_DEBUG_SQLS`
- `DATABEND_TRACE_DEBUG_OUTPUT_DIR`

## Notes

- Trace-debug tests enable `config.log.structlog.on` in their test fixture so regular log records can
  be bridged into fastrace span events without requiring an external OTLP tracing exporter.
- In practice, most attached OTLP events come from log-to-fastrace bridging through
  `logforth::append::FastraceEvent`, not from the pipeline `Processor::event()` scheduling enum.
- Event-rich spans are usually planner, binder, optimizer, interpreter, HTTP query state, and some
  processor summary spans. Many processor spans still expose only span names plus attributes.
- Shared raw span output is configured through `RawSpanFormatter` and `RawSpanFormatOptions`.
- Raw span output can include attached events via `RawSpanFormatOptions::with_events(true)`.
- Trace-debug tests always write the complete output directly to a file.
- The default output directory is `${TMPDIR:-/tmp}/databend-trace-debug/`.
- Output files use short prefixes plus a timestamp, for example:
  `direct-1743341019-851.otlp.json`,
  `http-1743341028-104.otlp.json`.
- The persisted payload uses OpenTelemetry OTLP JSON `ExportTraceServiceRequest`.
- Direct and HTTP helpers keep extra debugging context inside standard spans and attributes so the
  dump remains a complete trace export instead of a private summary schema.
- Shared test capture is protected by a process-local mutex to avoid reporter interference.

## Inspecting Events

List spans that contain attached OTLP events:

```bash
jq -r '
  .resourceSpans[].scopeSpans[].spans[]
  | select((.events | length) > 0)
  | "events=\(.events | length)\t\(.name)"
' /tmp/databend-trace-debug/direct-*.otlp.json
```

Expand the attached event messages and their attributes:

```bash
jq -r '
  .resourceSpans[].scopeSpans[].spans[]
  | select((.events | length) > 0)
  | . as $span
  | .events[]
  | [
      $span.name,
      .name,
      ((.attributes // [])
        | map("\(.key)=\(.value.stringValue // .value.intValue // .value.boolValue // .value.doubleValue // "")")
        | join(","))
    ]
  | @tsv
' /tmp/databend-trace-debug/direct-*.otlp.json
```

Typical event payloads include:

- binder status updates and CTE/reference diagnostics
- optimizer summary statistics
- physical plan dumps from `SelectInterpreter`
- HTTP query lifecycle logs such as query creation, first response, stop/finalize, and close reason
- processor summary logs such as aggregate input/output row counts and throughput
