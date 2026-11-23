# ClickBench: Databend releases compared locally (aligned build parameters, reproducible)

Date: 2026-11-21. All inputs, scripts and raw results are in this directory, `benchmark/clickbench-versions/`. Conclusions are in `RESULTS.md`. This is a record of a benchmark run and is not meant to be merged.

This directory was copied from the working directory of the run, `/home/kould/tmp-build/clickbench-versions/`, with these differences:

- The two locally built `databend-query` binaries (about 650 MB each) are not included; they remain in `/home/kould/tmp-build/clickbench-versions/bin/NAME/`. Their sha256 are recorded in `bin/NAME/build-info.json` and `manifest.json`.
- `build-official.sh` is unchanged from the run, so its sha256 matches `manifest.json`. It writes its output to `/home/kould/tmp-build/clickbench-versions/bin/NAME/`.
- The sha256 of `bench.py`, `releases.json` and `clickbench/*.sql` also match `manifest.json`.
- `report.py` changed after the manifest was recorded: it reads the load records from `loads/` instead of `/tmp/cb-versions/NAME/load.json`, and its output text was translated to English. The numbers it produces are unchanged.

## Files

| path | content |
|---|---|
| `releases.json` | the 5 releases, their sources and order |
| `build-official.sh` | builds databend-query in a clean worktree with the parameters of the x86_64 nightly release CI |
| `bin/NAME/build-info.json` | per local build: commit, git log, rustc, cargo command, environment variables, profile, start and end time, sha256 |
| `bench.py` | loads data (`load`), records the environment (`manifest`), runs the interleaved benchmark (`run`) |
| `report.py` → `report.md` | summary, result consistency, per-query and per-case comparisons |
| `charts.py` → `charts/*.svg` | draws three charts from `results/` and `probes/format.json`: per-query hot ratios, per-query peak RSS, 43-query sums; Python standard library only |
| `manifest.json` | machine (CPU, kernel, governor, THP, cgroup, disk), sha256 of the scripts and SQL, sha256 of the source data, config template, sha256 and `--version` of every binary |
| `loads/NAME.json` | load SQL, COPY/INSERT times, rows, blocks, data size, `SHOW CREATE TABLE`, the full `system.configs`, and settings that differ from their defaults |
| `results/round{0,1}.json` | per cell (round, query, release): timestamp, host CPU use before the cell, startup time, the 3 run times, peak RSS, row count, result hash, and the first 20 rows of run 1 |
| `clickbench/` | copy of the ClickBench databend scripts used (`create.sql`, `queries.sql`, the original `benchmark.sh`, ...) |
| `logs/run.log` | benchmark output |
| `probes/run_probes.py` | follow-up probes (2026-11-23): `format` builds a Parquet table in v0.9.53 for a same-format comparison, `q03` compares single-column aggregates, `small` collects segment statistics of small queries, the block meta cache comparison, and `length` vs `octet_length` |
| `probes/{format,q03,small}.json`, `*.log` | probe results and output |

## Releases and build parameters

| name | source |
|---|---|
| `v0.9.53` | official package `v0.9.53-nightly`, the release ClickBench runs |
| `v1.2.955` | official package, `058a1e8fbd`, before encoded columns #20819 |
| `v1.2.967` | official package, `25ca45599d` |
| `official-967` | `25ca45599d` built locally |
| `official-main-prs` | upstream/main `177b395dd3` (includes #21129) + #21138 + #21144, commit `581c894436` (local branch `bench/main-with-prs`, not pushed) |

Both local builds follow `.github/workflows/release.yml` (build_default, x86_64-unknown-linux-gnu) and `.github/actions/build_linux`:

- Cargo.toml `[profile.release]`: `lto = "thin"`, `codegen-units = 1`, `debug = "line-tables-only"`;
- `CARGO_PROFILE_RELEASE_OPT_LEVEL=s`, `CARGO_INCREMENTAL=0`, `--features default`, `--target x86_64-unknown-linux-gnu`;
- `RUSTFLAGS="-C target-feature=+sse4.2"`;
- debug info split off and stripped with `objcopy`/`strip --strip-debug` afterwards, as in CI.

The one deviation: `RUSTFLAGS` also passes `-C link-arg=-fuse-ld=mold`. Setting `RUSTFLAGS` drops the mold setting of `.cargo/config.toml`, and the official build image links with its own toolchain. The linker does not change the generated code: `.text` is 165.7 MiB in the local build and 165.6 MiB in the official package; `.symtab/.strtab` are larger locally (28/324 MiB vs 21/236 MiB). The toolchain is `nightly-2026-10-25` from `rust-toolchain.toml`, the same for both commits. The local builds use `databend-meta` from the official v1.2.967 package.

## Method

- Configuration: ClickBench's `config.toml` (fs storage plus meta), everything else default. 1.x additionally needs `tenant_id`, `cluster_id` and a root user to start and log in. `loads/*.json` records the settings that differ from their defaults for every release; all are empty.
- Table: ClickBench's `create.sql`, with `CLUSTER BY`. Loaded from `hits.parquet` in two steps: COPY into `hits_raw`, then `INSERT … SELECT` into `hits`. v0.9.53 cannot COPY Parquet's integer seconds/days straight into `TIMESTAMP`/`DATE` columns. All five releases use the same SQL.
- Steps for each cell (round r, query q, release v):
  1. wait until host CPU use is below 10%;
  2. stop query and meta;
  3. evict all files of the five releases from the page cache with `posix_fadvise(DONTNEED)` (`/proc/sys` is read-only in this container, so `drop_caches` is unavailable);
  4. start meta, then query, both under `taskset -c 0-15`;
  5. once `SELECT 1` answers, run the query 3 times.
- Run 1 is cold; hot is `min(run 2, run 3)`.
- Interleaving: each query runs on all five releases before the next query. Round 0 follows the `releases.json` order and round 1 the reverse, so drift in machine state spreads over all releases.
- In the summaries, cold is the mean over the two rounds, hot is the minimum over the two rounds, and peak is the maximum RSS over all runs. A change counts as consistent only when both rounds move the same way by more than 5% and by more than 5 ms.

## Reproduce

```bash
cd benchmark/clickbench-versions   # bench.py / report.py use their own directory as the root
# 1. Extract the official packages to /tmp/cb-versions/{v0.9.53,v1.2.955,v1.2.967}/ (bin/databend-query, bin/databend-meta)
# 2. Local builds (about 15 minutes each)
./build-official.sh official-967 25ca45599d
./build-official.sh official-main-prs 581c894436   # needs the local branch bench/main-with-prs
for n in official-967 official-main-prs; do mkdir -p /tmp/cb-versions/$n/bin
  cp /home/kould/tmp-build/clickbench-versions/bin/$n/databend-query /tmp/cb-versions/$n/bin/; cp /tmp/cb-versions/v1.2.967/bin/databend-meta /tmp/cb-versions/$n/bin/; done
# 3. Load (2-4 minutes each, about 80 GB for all five), record the environment, run two rounds (about 45 minutes), report
for n in v0.9.53 v1.2.955 v1.2.967 official-967 official-main-prs; do python3 bench.py load $n; done
python3 bench.py manifest && python3 bench.py run 2
for n in v0.9.53 v1.2.955 v1.2.967 official-967 official-main-prs; do cp /tmp/cb-versions/$n/load.json loads/$n.json; done
python3 report.py > report.md
python3 charts.py
# 4. Follow-up probes (need the v0.9.53 and official-main-prs tables loaded; `format` adds a ~15 GB hits_parquet table to v0.9.53)
for p in format q03 small; do python3 probes/run_probes.py $p > probes/$p.log 2>&1; done
```
