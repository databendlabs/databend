# Public Three-Node Benchmark Reference for Meta-service

> Purpose: provide a performance baseline for the `raft-log` / meta-service projects.
> Scope: etcd, TiKV, Apache ZooKeeper, and HashiCorp Consul, all with three-node deployments using strong consistency.
> Note: hardware, key/value sizes, and concurrency settings vary widely. **Align these parameters before comparing systems**.

---

## 0. Competitor Overview (Quick Reference)

| System | Write throughput under heavy load on SSDs in one data center | Write latency | Read throughput | Read latency |
|--------|-------------------------------------------------------------|---------------|-----------------|--------------|
| **etcd** | ~50,000 QPS | avg 20ms | ~140k QPS (linearizable) | avg 5.5ms |
| **TiKV** | ~43,200 OPS (update) | `avg <10ms` | ~212,000 OPS (point get) | `avg <10ms` |
| **ZooKeeper** | ~2,300 ops/s (set, single ensemble) | avg 0.4-1ms | ~5,900 ops/s (get) | avg 0.17ms |
| **Consul** | ~14,000 writes/s (community measurement) | avg a few ms, unstable P99 | ~23k-98k reads/s | sub-ms |

> How to read this table: test parameters differ widely between rows. **Compare only orders of magnitude**. See each system's section for details.

---

## 1. etcd (3 Nodes)

### 1.1 Official Benchmark (etcd.io)

Hardware: GCE n1-standard-4 or a comparable AWS instance, with local SSD storage. Key=8B, value=256B.

**Writes**

| Scenario | Concurrency | Throughput | Average latency |
|----------|-------------|------------|-----------------|
| Light load (leader) | 1 conn / 1 client | 583 QPS | 1.6 ms |
| Heavy load (leader) | 100 conn / 1000 clients | 44,341 QPS | 22 ms |
| Heavy load (all members) | 100 conn / 1000 clients | 50,104 QPS | 20 ms |

**Reads**

| Consistency | Light-load QPS | Light-load latency | Heavy-load QPS | Heavy-load latency |
|-------------|----------------|--------------------|----------------|--------------------|
| Linearizable | 1,353 | 0.7 ms | 141,578 | 5.5 ms |
| Serializable | 2,909 | 0.3 ms | 185,758 | 2.2 ms |

> Official summary, paraphrased: latency below 1ms under light load; throughput above 30,000 QPS under heavy load.

### 1.2 Measured P50/P90/P99 (Berops Multi-cloud Evaluation)

Three-node sequential put baseline, with 1 client / 1 connection:

| Percentile | Latency |
|------------|---------|
| P50 | 4.9 ms |
| P90 | 5.9 ms |
| P99 | 8.9 ms |
| Throughput | 196 req/s |

Throughput drops sharply across regions (>1600 km), because Raft commit latency depends on the network RTT.

### 1.3 Production Health Thresholds

- `wal_fsync_duration_seconds` P99 **< 10ms** (etcd's official recommendation), **< 25ms** (D2iQ's tolerance limit)
- `backend_commit_duration_seconds` P99 < 25 ms
- Request P99 **> 50ms becomes noticeable to users**; **> 100ms reaches the range associated with alerts and cascading failures**
- Recommended node count: ≤ 7

---

## 2. TiKV (3 Nodes)

### 2.1 Official Performance Overview

**Hardware (per node):**
- 40 vCPU (Intel Xeon E5-2630 v4 @ 2.20GHz)
- 64 GB RAM
- 500 GB NVMe SSD
- Mode: RawKV

**Test setup:** 12-pod deployment, 40 threads per pod, 10M operations over 10M records, using go-ycsb.

| Workload | Throughput | Latency |
|----------|------------|---------|
| YCSB workload C (point get reads) | 212,000 OPS | `avg < 10ms` |
| YCSB workload A (updates) | 43,200 OPS | `avg < 10ms` |
| Peak (any workload) | 200,000 OPS | `P99 < 10ms` |

### 2.2 Notes

- TiKV is a **transactional** KV store by default. The RawKV results above use a mode without transactions, which raises the reported performance.
- The official documentation states that write latency grows much faster than read latency under write-heavy workloads.
- TiKV uses Raft + RocksDB, with regions of ~96MB that split automatically. Its architecture differs fundamentally from etcd's single Raft group.

---

## 3. Apache ZooKeeper (3-Node Ensemble)

### 3.1 ServiceLatencyOverview (Sequential Operations on 10,000 znodes)

**4 cores / 10 clients (best configuration):**

| Operation | Throughput | Latency per operation |
|-----------|------------|-----------------------|
| create | 1,828 ops/s | 0.547 ms |
| set | 2,334 ops/s | 0.429 ms |
| get | 5,899 ops/s | 0.170 ms |
| delete | 2,630 ops/s | 0.380 ms |

**4 cores / 20 clients (high load):**

| Operation | Throughput | Latency per operation |
|-----------|------------|-----------------------|
| create | 912 ops/s | 1.097 ms |
| set | 1,311 ops/s | 0.763 ms |
| get | 2,042 ops/s | 0.490 ms |
| delete | 1,107 ops/s | 0.904 ms |

### 3.2 Historical ZooKeeper 3.2 Results (dbtester Comparison, 1KB Writes, 1M Requests)

- Maximum write throughput: 25,124 req/s
- Average write throughput: 16,842 req/s

### 3.3 Notes

- ZooKeeper znodes include stat metadata, ACLs, watchers, and ephemeral / sequential semantics. These features add overhead per operation compared with a plain KV store.
- **Reads scale well**: followers serve reads directly, so adding nodes can increase read throughput linearly. Writes still use the leader + ZAB.
- The lower figures reflect generally small znodes (`< 1KB`) and strongly sequential operation patterns within each client.

---

## 4. HashiCorp Consul (3-Server Cluster)

### 4.1 Historical dbtester Comparison (1KB Writes)

- Maximum write throughput: 15,865 req/s
- Average write throughput: 5,588 req/s

### 4.2 Results from Community Reports and Official Sources

- Community read measurement: ~23,000 req/s (single instance, 24-core Xeon, ramdisk, 200B value)
- Peak read throughput after optimization: ~98,000 reads/s (Consul 0.7.0rc2, 128B value)
- Write throughput: reported as 500 commits/s or more under heavy load; another community report averages ~14,000 writes/s
- Write latency: the target is a few ms. Measured spikes range from 370µs to 190ms, with an average of ~680µs
- **No official public P50/P90/P99 data is provided**

### 4.3 Notes

- Consul writes are **strongly I/O-bound** (Raft log fsync); reads are **CPU-bound**
- Its three main uses are service discovery, KV storage, and service mesh. KV storage is not its strongest area
- Cross-data-center support is a selling point for Consul, but RTT limits Raft commits just as it does in etcd

---

## 5. Considerations for Comparing Systems

`★ Insight ─────────────────────────────────────`
- **Differences of tens of times often come from test parameters**. The same system can show a 100-fold throughput difference between 1 client and 1000 clients. Increasing value size from 256B to 4KB can halve throughput. Check the parameters first when reading a benchmark.
- **The latency floor for a consensus system = network RTT + quorum fsync latency**. Check the deployment behind any claim of `P99 < 5ms`. Local NVMe in one data center and EBS across AZs operate at very different latency scales.
- **TiKV's numbers appear far ahead of the others**, but TiKV uses multiple Raft groups with regions running in parallel. etcd/ZK/Consul use a single Raft group, with a single-leader bottleneck. The difference is structural, rather than simply a matter of implementation quality.
`─────────────────────────────────────────────────`

Align these parameters before comparing systems:

1. **Node count**: this note uses 3 nodes throughout. Increasing to 5 or 7 nodes reduces throughput and raises latency for all systems.
2. **Value size**: etcd's official benchmark uses 256B; classic ZooKeeper tests use 1KB; production workloads commonly use 1-10KB.
3. **fsync policy**: by default, all systems fsync every commit. Disabling fsync (unsafe) can raise performance 10x, but makes the results incomparable.
4. **Client concurrency**: use one client to measure latency and many clients to measure throughput. **Keep these results separate**.
5. **Disk and network**: local NVMe (fsync ~100µs) vs EBS gp3 (~1ms) vs HDD (~10ms); RTT within one data center ~0.1ms vs across AZs ~1ms vs across regions 10ms+.
6. **Read consistency**: linearizable reads use Raft; serializable reads are local. Local reads can deliver an order of magnitude more throughput.

### 5.1 EBS gp3 / NVMe Storage Parameters

Here, `EBS gp3 / NVMe` refers to two types of test storage. `EBS gp3` is AWS persistent network block storage. In benchmarks, `NVMe` usually means local NVMe instance store on EC2. On Nitro instances, EBS also appears as an NVMe block device. A `/dev/nvme*` device name alone therefore cannot identify a local disk.

| Storage | Persistence | IOPS | Throughput | Latency characteristics | Benchmark implications |
|---------|-------------|------|------------|-------------------------|------------------------|
| EBS gp3 | Persistent, independent of the EC2 instance lifecycle | Baseline 3,000; maximum 80,000 | Baseline 125 MiB/s; maximum 2,000 MiB/s | Single-digit ms; actual latency depends on EC2 EBS bandwidth and queue depth | Closer to persistent disks in cloud production; useful for measuring the latency floor of durable writes |
| Local NVMe instance store | Temporary; data is lost when the instance stops, terminates, or hibernates | Tens of thousands to millions of 4KiB random IOPS, depending on instance size | Usually GB/s, depending on instance size and I/O pattern | Usually lower latency than EBS, with a lower fsync latency floor | Closer to the limits of local SSDs; useful for measuring raft-log / WAL implementation overhead |

Examples of local NVMe performance from official instance specifications:

| Instance type | Local NVMe | 4KiB random read IOPS | 4KiB random write IOPS |
|---------------|------------|-----------------------|------------------------|
| i4i.large | 1 x 468 GB | 50,000 | 27,500 |
| i4i.8xlarge | 2 x 3,750 GB | 800,000 | 440,000 |
| i4i.32xlarge | 8 x 3,750 GB | 3,200,000 | 1,760,000 |
| i8g.48xlarge | 12 x 3,750 GB | 7,200,000 | 3,960,000 |

For WAL/fsync-sensitive benchmarks such as `raft-log`, report the two sets of results separately. `gp3` mainly reflects persistent cloud disk and EBS network path constraints. Local NVMe mainly reflects the limits of the log implementation, batching, and serialization path.

## 6. Suggested Benchmarks for the raft-log Project

| Metric | etcd baseline | Suggested test |
|--------|---------------|----------------|
| Single-client sequential write P99 | ~9ms (256B value) | Compare with the same parameters to see whether latency can fall below 5ms |
| Write throughput under heavy load (1000 clients) | ~50k QPS | Reach at least comparable throughput |
| Linearizable read throughput | ~140k QPS | Check whether the read path can use the Raft local read optimization |
| wal_fsync P99 | `< 10ms` | Run one set on EBS gp3 and another on NVMe |

---

## Sources

### etcd
- [etcd v3.5 Performance (official)](https://etcd.io/docs/v3.5/op-guide/performance/)
- [Benchmarking etcd v3 demo (official)](https://etcd.io/docs/v3.6/benchmarks/etcd-3-demo-benchmarks/)
- [Evaluating etcd's performance in multi-cloud (Berops, includes P50/P90/P99)](https://medium.com/berops-blog/evaluating-etcds-performance-in-multi-cloud-83ee5a2fd70c)
- [Etcd Performance Benchmarking (D2iQ, fsync thresholds)](https://eng.d2iq.com/blog/etcd-performance-benchmarking/)
- [Recommended etcd practices (OKD)](https://docs.okd.io/latest/etcd/etcd-practices.html)
- [Autonomous testing of etcd's robustness (CNCF)](https://www.cncf.io/blog/2025/09/25/autonomous-testing-of-etcds-robustness/)
- [Key metrics for monitoring etcd (Datadog)](https://www.datadoghq.com/blog/etcd-key-metrics/)

### TiKV
- [TiKV Performance Overview (official)](https://tikv.org/docs/6.1/deploy/performance/overview/)
- [TiKV Benchmark and Performance (official)](https://tikv.org/docs/6.1/deploy/performance/performance/)
- [TiKV Benchmark Instructions (official)](https://tikv.org/docs/5.1/deploy/performance/instructions/)
- [Building, Running, and Benchmarking TiKV and TiDB (PingCAP)](https://www.pingcap.com/blog/building-running-and-benchmarking-tikv-and-tidb/)

### ZooKeeper
- [ZooKeeper ServiceLatencyOverview (Apache wiki)](https://cwiki.apache.org/confluence/display/ZOOKEEPER/ServiceLatencyOverview)
- [ZooKeeper 3.2 Performance (Apache wiki)](https://cwiki.apache.org/confluence/display/ZOOKEEPER/Performance)
- [ZooKeeper: Wait-free coordination for Internet-scale systems (USENIX paper)](https://www.usenix.org/legacy/event/atc10/tech/full_papers/Hunt.pdf)
- [zookeeper-benchmark (Brown University)](https://github.com/brownsys/zookeeper-benchmark)

### Consul
- [Consul Server Resource Requirements (official)](https://developer.hashicorp.com/consul/docs/install/performance)
- [Consul capacity planning (official)](https://developer.hashicorp.com/consul/docs/reference/architecture/capacity)
- [Configure Consul for performance at scale (Criteo)](https://medium.com/criteo-engineering/configure-consul-for-performance-at-scale-f6a089706377)
- [Jepsen: etcd and Consul (Aphyr)](https://aphyr.com/posts/316-jepsen-etcd-and-consul)

### Cross-system Comparisons
- [Consul vs Zookeeper vs etcd (StackShare)](https://stackshare.io/stackups/consul-vs-etcd-vs-zookeeper)
- [In-Depth Comparison of Distributed Coordination Tools (Medium)](https://medium.com/@karim.albakry/in-depth-comparison-of-distributed-coordination-tools-consul-etcd-zookeeper-and-nacos-a6f8e5d612a6)
- [etcd-io/dbtester (official comparison tool)](https://github.com/etcd-io/dbtester)
- [Exploring Performance of Etcd, Zookeeper and Consul Key-value stores 2017 (HN)](https://news.ycombinator.com/item?id=23711431)

### AWS Storage
- [Amazon EBS General Purpose SSD volumes (official gp3 specifications)](https://docs.aws.amazon.com/ebs/latest/userguide/general-purpose.html)
- [Amazon EBS and NVMe on Nitro instances (EBS device naming)](https://docs.aws.amazon.com/ebs/latest/userguide/nvme-ebs-volumes.html)
- [Amazon EC2 instance store lifetime (local disk lifecycle)](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-store-lifetime.html)
- [Storage optimized instances (local NVMe IOPS examples)](https://docs.aws.amazon.com/ec2/latest/instancetypes/so.html)
