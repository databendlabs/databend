# ClickBench 本地版本对比（构建参数对齐，可复盘）

日期：2026-11-21。所有输入、脚本、原始结果都在本目录 `benchmark/clickbench-versions/`。结论见 `RESULTS.md`。仅作测试记录，不打算合入。

本目录复制自测试时的工作目录 `/home/kould/tmp-build/clickbench-versions/`，以下几点和原目录不同：

- 两个本地构建的 `databend-query`（各约 650 MB）没有复制进来，仍在 `/home/kould/tmp-build/clickbench-versions/bin/NAME/`。它们的 sha256 记录在 `bin/NAME/build-info.json` 和 `manifest.json` 里。
- `build-official.sh` 与测试时完全相同，sha256 和 `manifest.json` 一致。它会把输出写到 `/home/kould/tmp-build/clickbench-versions/bin/NAME/`。
- `bench.py`、`releases.json`、`clickbench/*.sql` 的 sha256 也和 `manifest.json` 一致。
- `report.py` 在 manifest 记录之后改过一行：导入记录改从 `loads/` 读取，原来是从 `/tmp/cb-versions/NAME/load.json` 读取。在本目录运行 `python3 report.py`，生成的内容与 `report.md` 完全相同。

## 目录

| 路径 | 内容 |
|---|---|
| `releases.json` | 参测的 5 个版本、来源、顺序 |
| `build-official.sh` | 按 x86_64 nightly 发布 CI 的参数在干净 worktree 里构建 databend-query |
| `bin/NAME/build-info.json` | 每个本地构建的提交、git log、rustc、cargo 命令、环境变量、profile、起止时间、sha256 |
| `bench.py` | 导入（`load`）、记录环境（`manifest`）、交错测试（`run`） |
| `report.py` → `report.md` | 汇总、结果一致性、逐题、逐 case 对比 |
| `charts.py` → `charts/*.svg` | 用 `results/` 和 `probes/format.json` 画三张图：逐题热跑比值、逐题峰值 RSS、43 题合计；只用 Python 标准库 |
| `manifest.json` | 机器（CPU、内核、调频、THP、cgroup、磁盘）、脚本与 SQL 的 sha256、源数据 sha256、配置模板、每个二进制的 sha256 和 `--version` |
| `loads/NAME.json` | 导入用的 SQL、COPY/INSERT 耗时、行数、块数、数据大小、`SHOW CREATE TABLE`、完整 `system.configs`、与默认值不同的 settings |
| `results/round{0,1}.json` | 每格（轮、题、版本）的时间戳、开始前主机 CPU 占用、启动耗时、3 次运行的耗时、峰值 RSS、行数、结果哈希，以及第 1 次运行的前 20 行结果 |
| `clickbench/` | 所用的 ClickBench databend 脚本副本（`create.sql`、`queries.sql`、原始 `benchmark.sh` 等） |
| `logs/run.log` | 测试过程输出 |
| `probes/run_probes.py` | 补充测试（2026-11-23）：`format` 在 v0.9.53 里建 Parquet 表做同格式对比，`q03` 对比单列聚合，`small` 测小查询的 segment 统计、block meta 缓存，以及 `length` 和 `octet_length` |
| `probes/{format,q03,small}.json`、`*.log` | 补充测试的结果和输出 |

## 版本与构建参数

| 名称 | 来源 |
|---|---|
| `v0.9.53` | 官方包 `v0.9.53-nightly`，ClickBench 现用版本 |
| `v1.2.955` | 官方包，`058a1e8fbd`，编码列 #20819 之前 |
| `v1.2.967` | 官方包，`25ca45599d` |
| `official-967` | `25ca45599d` 本地构建 |
| `official-main-prs` | upstream/main `177b395dd3`（含 #21129）+ #21138 + #21144，提交 `581c894436`（本地分支 `bench/main-with-prs`，未推送） |

两个本地构建都按 `.github/workflows/release.yml`（build_default，x86_64-unknown-linux-gnu）和 `.github/actions/build_linux` 的参数构建：

- Cargo.toml 的 `[profile.release]`：`lto = "thin"`、`codegen-units = 1`、`debug = "line-tables-only"`；
- `CARGO_PROFILE_RELEASE_OPT_LEVEL=s`、`CARGO_INCREMENTAL=0`、`--features default`、`--target x86_64-unknown-linux-gnu`；
- `RUSTFLAGS="-C target-feature=+sse4.2"`；
- 构建后用 `objcopy`/`strip --strip-debug` 剥离调试信息，和 CI 一样。

唯一的偏差：`RUSTFLAGS` 里多了 `-C link-arg=-fuse-ld=mold`。设置 `RUSTFLAGS` 会覆盖 `.cargo/config.toml` 里的 mold 配置，而官方构建镜像用的是它自己的链接器。链接器不改变生成的代码：本地构建的 `.text` 是 165.7 MiB，官方包是 165.6 MiB；`.symtab/.strtab` 本地更大（28/324 MiB 对 21/236 MiB）。工具链是 `rust-toolchain.toml` 指定的 `nightly-2026-10-25`，两个提交一样。本地构建的 meta 用的是 v1.2.967 官方包里的 `databend-meta`。

## 测法

- 配置：ClickBench 的 `config.toml`（fs 存储加 meta），其余全部默认。1.x 额外加了 `tenant_id`、`cluster_id` 和 root 用户，否则无法启动或登录。`loads/*.json` 里记录了每个版本"与默认值不同的 settings"，全部为空。
- 表：ClickBench 的 `create.sql`，带 `CLUSTER BY`。从 `hits.parquet` 分两步导入，先 COPY 进 `hits_raw`，再 `INSERT … SELECT` 进 `hits`。原因是 v0.9.53 不能把 Parquet 里的整数秒/天直接 COPY 进 `TIMESTAMP`/`DATE` 列。五个版本用的是同一组 SQL。
- 每格（轮 r、题 q、版本 v）的步骤：
  1. 等主机 CPU 占用低于 10%；
  2. 停掉 query 和 meta；
  3. 用 `posix_fadvise(DONTNEED)` 把五个版本的全部文件逐出页缓存（容器里 `/proc/sys` 只读，`drop_caches` 不能用）；
  4. 先启动 meta，再启动 query，两者都 `taskset -c 0-15`；
  5. 等 `SELECT 1` 返回后，连跑 3 次。
- 第 1 次运行算冷跑，热跑取 `min(第 2 次, 第 3 次)`。
- 交错顺序：每一题依次跑完五个版本再进入下一题。第 0 轮按 `releases.json` 的顺序，第 1 轮倒序，让机器状态的漂移平均落到各个版本上。
- 汇总时，冷跑取两轮的平均，热跑取两轮中的最小值，峰值取全部运行中的最大 RSS。只有两轮都快或都慢超过 5%、并且差值超过 5 ms，才标为"稳定"变化。

## 复盘步骤

```bash
cd benchmark/clickbench-versions   # bench.py / report.py 以脚本所在目录为根
# 1. 官方包解压到 /tmp/cb-versions/{v0.9.53,v1.2.955,v1.2.967}/（bin/databend-query、bin/databend-meta）
# 2. 本地构建（每个约 15 分钟）
./build-official.sh official-967 25ca45599d
./build-official.sh official-main-prs 581c894436   # 需要本地分支 bench/main-with-prs
for n in official-967 official-main-prs; do mkdir -p /tmp/cb-versions/$n/bin
  cp /home/kould/tmp-build/clickbench-versions/bin/$n/databend-query /tmp/cb-versions/$n/bin/; cp /tmp/cb-versions/v1.2.967/bin/databend-meta /tmp/cb-versions/$n/bin/; done
# 3. 导入（每个约 2–4 分钟，5 个合计约 80 GB），记录环境，跑两轮（约 45 分钟），出报告
for n in v0.9.53 v1.2.955 v1.2.967 official-967 official-main-prs; do python3 bench.py load $n; done
python3 bench.py manifest && python3 bench.py run 2
for n in v0.9.53 v1.2.955 v1.2.967 official-967 official-main-prs; do cp /tmp/cb-versions/$n/load.json loads/$n.json; done
python3 report.py > report.md
python3 charts.py
# 4. 补充测试（需要 v0.9.53 和 official-main-prs 的表已导入；format 会在 v0.9.53 里多建一张约 15 GB 的 hits_parquet）
for p in format q03 small; do python3 probes/run_probes.py $p > probes/$p.log 2>&1; done
```
