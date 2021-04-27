---
id: development-howtoprofile
title: How to profile Datafuse
---

We use [flamegraph-rs](https://github.com/flamegraph-rs/flamegraph) to profile Datafuse.

## How to use

```text
sudo apt install -y linux-tools-common linux-tools-generic
cargo install flamegraph
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
make profile
```

More to see [flamegraph-rs](https://github.com/flamegraph-rs/flamegraph#installation)
