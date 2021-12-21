---
name: Bug report
about: Create a report to help us improve Databend
title: ''
labels: bug
assignees: ''

---

**Hold on**: `RUST_TEST_THREADS=1 make test` will solve most timeout failures.

Since `metasrv` is distributed(raft driven), it is sensitive to event delay.
It is likely to encounter a **timeout** error when running a bunch of unit-tests in parallel.
If in single thread mode, i.e., `RUST_TEST_THREADS=1 make test` or `RUST_TEST_THREADS=1 cargo test` passes, 
then it is very likely not a bug. E.g., the log about a delayed test can be found in
`metasrv/_logs/*` such as:

```
2021-12-21T02:27:21.534917Z DEBUG ThreadId(221) ut{test_meta_node_snapshot_replication}:open_create_boot ...
...
2021-12-21T02:27:24.655324Z DEBUG ThreadId(216) ut{test_meta_node_snapshot_replication}:log{want_log=15 msg="non-voter replicated all logs"}: async_raft::metrics: enter
```

**Summary**

Description for this bug.

**How to reproduce**

**Error message and/or stacktrace**

If applicable, add screenshots to help explain your problem.

[How to get the server logs](https://databend.rs/development/how-to-get-server-logs/)

**Logs**

The logs provides a lot information for debug:
- For databend-query unit test failure: logs are in `/query/_logs/`
- For databend-meta unit test failure: logs are in `/metasrv/_logs/`
- For integration test failure: logs are in `/_logs*`

**Additional context**

Add any other context about the problem here.
