## Environment

- Recorded 2026-11-21T21:01:32Z; CPU 12th Gen Intel(R) Core(TM) i9-12900KF; kernel 6.17.6-arch1-1; governor `powersave`; THP `[always] madvise never`
- Pinned with `taskset -c 0-15`; cgroup cpu.max `max 100000`, memory.max `max`; drop_caches writable: no: [Errno 30] Read-only file system: '/proc/sys/vm/drop_caches'
- Source data `/tmp/tpch-local/hits/hits.parquet`, 14779976446 bytes, sha256 `a390f6cb782f6aae…`
- 2 rounds; before each cell the harness waits until host CPU use is below 10% (1 s sample, at most 300 s); the highest observed was 9.8%

| release | source | databend-query sha256 | `--version` |
|---|---|---|---|
| v0.9.53 | official release v0.9.53-nightly (the version ClickBench uses) | `ed24921e5697` | `databend-query v0.9.53-nightly-c960892(rust-1.68.0-nightly-2023-02-26T07:06:28.204406701Z)` |
| v1.2.955 | official release v1.2.955-nightly, commit 058a1e8fbd, before encoded columns #20819 | `aa0e764d2774` | `databend-query v1.2.955-058a1e8fbd(rust-1.100.0-nightly-2026-10-25T22:13:30.190776770Z)` |
| v1.2.967 | official release v1.2.967-nightly, commit 25ca45599d | `79544051130d` | `databend-query v1.2.967-25ca45599d(rust-1.101.0-nightly-2026-11-20T04:42:21.196252733Z)` |
| official-967 | commit 25ca45599d (= v1.2.967) built locally by build-official.sh with the release CI parameters; meta from the v1.2.967 package | `7c531a49ca62` | `databend-query v1.2.967-25ca45599d(rust-1.101.0-nightly-2026-11-21T20:09:51.167759719Z)` |
| official-main-prs | upstream/main 177b395dd3 + #21138 + #21144 (581c894436) built by build-official.sh; meta from the v1.2.967 package | `6c59b5f5ed49` | `databend-query v1.2.967-581c894436(rust-1.101.0-nightly-2026-11-21T20:24:58.225847978Z)` |

## Load

| release | COPY | INSERT … SELECT | total | rows | blocks | data size | non-default settings |
|---|---:|---:|---:|---:|---:|---:|---|
| v0.9.53 | 48.2 s | 164.0 s | 212.2 s | 99997497 | 779 | 20.75 GB | none |
| v1.2.955 | 57.2 s | 55.8 s | 112.9 s | 99997497 | 731 | 14.79 GB | none |
| v1.2.967 | 57.1 s | 56.5 s | 113.6 s | 99997497 | 729 | 14.78 GB | none |
| official-967 | 58.0 s | 57.0 s | 115.0 s | 99997497 | 730 | 14.79 GB | none |
| official-main-prs | 58.1 s | 57.1 s | 115.2 s | 99997497 | 735 | 14.78 GB | none |

## Summary (43 / 43 queries succeeded on every release)

| release | cold sum | hot sum | hot sum R1 | hot sum R2 | Q32 peak RSS | median startup | hot geomean / v0.9.53 | hot geomean / official-967 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| v0.9.53 | 38.66 s | 30.36 s | 30.59 s | 30.63 s | 13.1 GiB | 0.21 s | 1.000 | 0.815 |
| v1.2.955 | 40.61 s | 32.36 s | 32.61 s | 33.15 s | 15.0 GiB | 1.54 s | 1.582 | 1.289 |
| v1.2.967 | 31.74 s | 23.43 s | 23.60 s | 23.79 s | 16.6 GiB | 1.59 s | 1.192 | 0.972 |
| official-967 | 31.39 s | 23.61 s | 23.80 s | 23.89 s | 16.6 GiB | 1.53 s | 1.227 | 1.000 |
| official-main-prs | 30.61 s | 22.47 s | 22.59 s | 22.99 s | 13.3 GiB | 1.28 s | 1.189 | 0.969 |

Round-to-round noise (geomean absolute deviation of the hot-time ratio between the two rounds of a release): v0.9.53 5.0%, v1.2.955 5.0%, v1.2.967 8.2%, official-967 3.8%, official-main-prs 3.6%

## Result consistency

Row count and result hash (rows sorted) of run 1 of every query. Releases may format numbers differently, so a different hash does not by itself mean a wrong result.

- Q03 hash groups: v0.9.53 / v1.2.955, v1.2.967, official-967, official-main-prs
- Q17 v0.9.53: the 3 runs of this release return different results
- Q17 hash groups: v0.9.53 / v1.2.955, v1.2.967, official-967, official-main-prs
- Q24 v1.2.955: the 3 runs of this release return different results
- Q24 v1.2.967: the 3 runs of this release return different results
- Q24 official-967: the 3 runs of this release return different results
- Q24 official-main-prs: the 3 runs of this release return different results
- Q24 hash groups: v0.9.53 / v1.2.955, official-967 / v1.2.967 / official-main-prs
- Q27 hash groups: v0.9.53 / v1.2.955, v1.2.967, official-967, official-main-prs
- Q28 hash groups: v0.9.53 / v1.2.955, v1.2.967, official-967, official-main-prs
- Q30 v0.9.53: the 3 runs of this release return different results
- Q30 v1.2.955: the 3 runs of this release return different results
- Q30 official-967: the 3 runs of this release return different results
- Q30 official-main-prs: the 3 runs of this release return different results
- Q30 hash groups: v0.9.53, official-main-prs / v1.2.955, v1.2.967, official-967
- Q31 v1.2.955: the 3 runs of this release return different results
- Q31 v1.2.967: the 3 runs of this release return different results
- Q31 official-967: the 3 runs of this release return different results
- Q31 official-main-prs: the 3 runs of this release return different results
- Q31 hash groups: v0.9.53 / v1.2.955 / v1.2.967 / official-967 / official-main-prs
- Q32 v0.9.53: the 3 runs of this release return different results
- Q32 v1.2.955: the 3 runs of this release return different results
- Q32 v1.2.967: the 3 runs of this release return different results
- Q32 official-967: the 3 runs of this release return different results
- Q32 official-main-prs: the 3 runs of this release return different results
- Q32 hash groups: v0.9.53 / v1.2.955 / v1.2.967 / official-967 / official-main-prs
- Q38 v1.2.955: the 3 runs of this release return different results
- Q38 v1.2.967: the 3 runs of this release return different results
- Q38 official-967: the 3 runs of this release return different results
- Q38 official-main-prs: the 3 runs of this release return different results
- Q38 hash groups: v0.9.53 / v1.2.955 / v1.2.967 / official-967 / official-main-prs
- Q39 v0.9.53: the 3 runs of this release return different results
- Q39 v1.2.955: the 3 runs of this release return different results
- Q39 v1.2.967: the 3 runs of this release return different results
- Q39 official-967: the 3 runs of this release return different results
- Q39 official-main-prs: the 3 runs of this release return different results
- Q39 hash groups: v0.9.53 / v1.2.955 / v1.2.967 / official-967 / official-main-prs
- Q40 v0.9.53: the 3 runs of this release return different results
- Q40 v1.2.955: the 3 runs of this release return different results
- Q40 v1.2.967: the 3 runs of this release return different results
- Q40 official-967: the 3 runs of this release return different results
- Q40 official-main-prs: the 3 runs of this release return different results
- Q40 hash groups: v0.9.53 / v1.2.955 / v1.2.967 / official-967 / official-main-prs
- Q41 v1.2.955: the 3 runs of this release return different results
- Q41 v1.2.967: the 3 runs of this release return different results
- Q41 official-967: the 3 runs of this release return different results
- Q41 official-main-prs: the 3 runs of this release return different results
- Q41 hash groups: v0.9.53 / v1.2.955 / v1.2.967 / official-967 / official-main-prs

## Per query (cold / hot, seconds; ratios are hot)

| Q | v0.9.53 | v1.2.955 | v1.2.967 | official-967 | official-main-prs | official-main-prs / official-967 | official-main-prs / v0.9.53 | official-967 / v1.2.967 |
|---|---|---|---|---|---|---:|---:|---:|
| Q00 | 0.012 / 0.005 | 0.044 / 0.011 | 0.037 / 0.003 | 0.043 / 0.011 | 0.037 / 0.016 | 1.46 | 3.38 ↑ | 3.21 |
| Q01 | 0.164 / 0.018 | 0.151 / 0.035 | 0.156 / 0.033 | 0.157 / 0.031 | 0.144 / 0.030 | 0.98 | 1.73 ↑ | 0.93 |
| Q02 | 0.189 / 0.028 | 0.202 / 0.055 | 0.211 / 0.053 | 0.191 / 0.051 | 0.185 / 0.054 | 1.06 | 1.93 ↑ | 0.96 |
| Q03 | 0.200 / 0.028 | 0.228 / 0.070 | 0.217 / 0.070 | 0.223 / 0.071 | 0.209 / 0.064 | 0.90 ↓ | 2.28 ↑ | 1.02 |
| Q04 | 0.502 / 0.351 | 0.477 / 0.310 | 0.450 / 0.285 | 0.435 / 0.287 | 0.448 / 0.277 | 0.97 | 0.79 ↓ | 1.01 |
| Q05 | 0.584 / 0.410 | 0.566 / 0.373 | 0.540 / 0.354 | 0.531 / 0.356 | 0.541 / 0.345 | 0.97 | 0.84 ↓ | 1.00 |
| Q06 | 0.149 / 0.016 | 0.038 / 0.008 | 0.054 / 0.017 | 0.049 / 0.016 | 0.048 / 0.018 | 1.07 | 1.12 | 0.98 |
| Q07 | 0.149 / 0.021 | 0.182 / 0.037 | 0.172 / 0.033 | 0.159 / 0.034 | 0.187 / 0.034 | 0.99 | 1.64 ↑ | 1.02 |
| Q08 | 0.552 / 0.395 | 0.681 / 0.484 | 0.526 / 0.349 | 0.525 / 0.347 | 0.503 / 0.345 | 0.99 | 0.87 ↓ | 0.99 |
| Q09 | 0.620 / 0.443 | 0.769 / 0.569 | 0.758 / 0.568 | 0.739 / 0.573 | 0.764 / 0.560 | 0.98 | 1.27 ↑ | 1.01 |
| Q10 | 0.310 / 0.133 | 0.369 / 0.221 | 0.329 / 0.174 | 0.328 / 0.173 | 0.323 / 0.150 | 0.86 ↓ | 1.13 ↑ | 1.00 |
| Q11 | 0.311 / 0.122 | 0.370 / 0.207 | 0.352 / 0.186 | 0.334 / 0.188 | 0.334 / 0.161 | 0.86 ↓ | 1.31 ↑ | 1.01 |
| Q12 | 0.661 / 0.465 | 0.671 / 0.484 | 0.652 / 0.444 | 0.618 / 0.443 | 0.623 / 0.423 | 0.96 | 0.91 ↓ | 1.00 |
| Q13 | 0.910 / 0.704 | 1.017 / 0.781 | 0.969 / 0.706 | 0.950 / 0.708 | 0.948 / 0.695 | 0.98 | 0.99 | 1.00 |
| Q14 | 0.727 / 0.532 | 0.780 / 0.567 | 0.676 / 0.480 | 0.683 / 0.482 | 0.672 / 0.463 | 0.96 | 0.87 ↓ | 1.00 |
| Q15 | 0.619 / 0.450 | 0.605 / 0.416 | 0.515 / 0.342 | 0.482 / 0.338 | 0.503 / 0.331 | 0.98 | 0.74 ↓ | 0.99 |
| Q16 | 1.804 / 1.541 | 1.105 / 0.851 | 0.980 / 0.711 | 1.013 / 0.716 | 0.965 / 0.702 | 0.98 | 0.46 ↓ | 1.01 |
| Q17 | 1.059 / 0.764 | 0.839 / 0.705 | 0.189 / 0.039 | 0.168 / 0.039 | 0.190 / 0.043 | 1.10 | 0.06 ↓ | 1.00 |
| Q18 | 2.864 / 2.535 | 2.025 / 1.729 | 1.690 / 1.385 | 1.729 / 1.391 | 1.728 / 1.348 | 0.97 | 0.53 ↓ | 1.00 |
| Q19 | 0.143 / 0.004 | 0.184 / 0.007 | 0.175 / 0.007 | 0.181 / 0.006 | 0.167 / 0.006 | 1.10 | 1.52 | 0.87 |
| Q20 | 0.632 / 0.316 | 1.323 / 1.038 | 1.239 / 0.958 | 1.247 / 0.978 | 0.970 / 0.710 | 0.73 ↓ | 2.25 ↑ | 1.02 |
| Q21 | 0.734 / 0.390 | 1.551 / 1.213 | 1.320 / 0.998 | 1.333 / 1.006 | 1.270 / 0.928 | 0.92 ↓ | 2.38 ↑ | 1.01 |
| Q22 | 1.242 / 0.741 | 2.439 / 1.980 | 2.227 / 1.736 | 2.192 / 1.732 | 2.107 / 1.613 | 0.93 | 2.18 ↑ | 1.00 |
| Q23 | 0.549 / 0.124 | 0.682 / 0.451 | 0.452 / 0.218 | 0.422 / 0.216 | 0.422 / 0.211 | 0.98 | 1.70 ↑ | 0.99 |
| Q24 | 0.156 / 0.016 | 0.213 / 0.059 | 0.191 / 0.056 | 0.206 / 0.060 | 0.197 / 0.051 | 0.85 ↓ | 3.29 ↑ | 1.08 |
| Q25 | 0.268 / 0.110 | 0.428 / 0.252 | 0.410 / 0.253 | 0.408 / 0.252 | 0.388 / 0.234 | 0.93 | 2.14 ↑ | 1.00 |
| Q26 | 0.168 / 0.016 | 0.718 / 0.482 | 0.224 / 0.071 | 0.214 / 0.077 | 0.211 / 0.067 | 0.88 ↓ | 4.12 ↑ | 1.08 |
| Q27 | 0.661 / 0.300 | 1.492 / 1.165 | 1.409 / 1.117 | 1.395 / 1.119 | 1.344 / 1.050 | 0.94 ↓ | 3.50 ↑ | 1.00 |
| Q28 | 8.770 / 9.255 | 7.227 / 7.565 | 4.011 / 4.184 | 4.048 / 4.281 | 4.037 / 4.240 | 0.99 | 0.46 ↓ | 1.02 |
| Q29 | 0.215 / 0.052 | 0.304 / 0.064 | 0.271 / 0.063 | 0.270 / 0.063 | 0.275 / 0.063 | 1.01 | 1.21 ↑ | 1.00 |
| Q30 | 0.556 / 0.325 | 0.800 / 0.550 | 0.734 / 0.494 | 0.685 / 0.492 | 0.697 / 0.475 | 0.97 | 1.46 ↑ | 1.00 |
| Q31 | 0.917 / 0.620 | 1.130 / 0.822 | 1.042 / 0.723 | 0.999 / 0.722 | 0.971 / 0.670 | 0.93 ↓ | 1.08 | 1.00 |
| Q32 | 4.030 / 3.810 | 3.612 / 3.307 | 2.103 / 1.712 | 2.023 / 1.731 | 1.903 / 1.609 | 0.93 | 0.42 ↓ | 1.01 |
| Q33 | 2.698 / 2.368 | 2.647 / 2.334 | 2.430 / 2.053 | 2.379 / 2.038 | 2.378 / 1.999 | 0.98 | 0.84 ↓ | 0.99 |
| Q34 | 2.787 / 2.390 | 2.688 / 2.342 | 2.391 / 2.060 | 2.463 / 2.082 | 2.324 / 2.002 | 0.96 | 0.84 ↓ | 1.01 |
| Q35 | 0.559 / 0.401 | 0.781 / 0.581 | 0.486 / 0.294 | 0.457 / 0.296 | 0.445 / 0.285 | 0.96 | 0.71 ↓ | 1.01 |
| Q36 | 0.171 / 0.033 | 0.200 / 0.048 | 0.182 / 0.041 | 0.149 / 0.042 | 0.178 / 0.041 | 0.98 | 1.24 ↑ | 1.02 |
| Q37 | 0.170 / 0.020 | 0.168 / 0.025 | 0.168 / 0.019 | 0.154 / 0.021 | 0.153 / 0.022 | 1.05 | 1.11 | 1.07 |
| Q38 | 0.158 / 0.013 | 0.177 / 0.029 | 0.162 / 0.025 | 0.169 / 0.024 | 0.164 / 0.022 | 0.93 | 1.72 ↑ | 0.96 |
| Q39 | 0.222 / 0.064 | 0.224 / 0.077 | 0.223 / 0.068 | 0.204 / 0.067 | 0.213 / 0.065 | 0.97 | 1.01 | 0.98 |
| Q40 | 0.150 / 0.013 | 0.171 / 0.019 | 0.140 / 0.015 | 0.151 / 0.016 | 0.142 / 0.016 | 0.99 | 1.24 | 1.05 |
| Q41 | 0.162 / 0.010 | 0.152 / 0.018 | 0.131 / 0.016 | 0.144 / 0.017 | 0.156 / 0.015 | 0.92 | 1.55 ↑ | 1.03 |
| Q42 | 0.155 / 0.009 | 0.178 / 0.019 | 0.144 / 0.015 | 0.135 / 0.015 | 0.144 / 0.015 | 1.05 | 1.62 ↑ | 0.97 |

↓ / ↑: faster / slower in every round by more than 5% and more than 5 ms.

## Per-case comparison

### Code change: main + #21138 + #21144 vs the v1.2.967 commit (same build parameters)

Hot geomean 0.969, hot sum 23.61 → 22.47 s, peak geomean 0.831. 9 queries consistently faster, 0 consistently slower, 34 within noise.

| faster | official-967 hot | official-main-prs hot | ratio | official-967 peak | official-main-prs peak |
|---|---:|---:|---:|---:|---:|
| Q20 | 0.978 | 0.710 | 0.73 | 2.5 GiB | 1.4 GiB |
| Q24 | 0.060 | 0.051 | 0.85 | 1.4 GiB | 1.2 GiB |
| Q11 | 0.188 | 0.161 | 0.86 | 1.4 GiB | 1.4 GiB |
| Q10 | 0.173 | 0.150 | 0.86 | 1.4 GiB | 1.4 GiB |
| Q26 | 0.077 | 0.067 | 0.88 | 1.5 GiB | 1.2 GiB |
| Q03 | 0.071 | 0.064 | 0.90 | 1.1 GiB | 0.9 GiB |
| Q21 | 1.006 | 0.928 | 0.92 | 2.7 GiB | 2.2 GiB |
| Q31 | 0.722 | 0.670 | 0.93 | 3.7 GiB | 4.3 GiB |
| Q27 | 1.119 | 1.050 | 0.94 | 2.5 GiB | 2.2 GiB |

### Build reproduction: 967 built locally with the CI parameters vs the official package

Hot geomean 1.029, hot sum 23.43 → 23.61 s, peak geomean 1.061. 0 queries consistently faster, 0 consistently slower, 43 within noise.

### Latest code vs v0.9.53, the release ClickBench runs

Hot geomean 1.189, hot sum 30.36 → 22.47 s, peak geomean 0.997. 14 queries consistently faster, 22 consistently slower, 7 within noise.

| faster | v0.9.53 hot | official-main-prs hot | ratio | v0.9.53 peak | official-main-prs peak |
|---|---:|---:|---:|---:|---:|
| Q17 | 0.764 | 0.043 | 0.06 | 8.5 GiB | 1.1 GiB |
| Q32 | 3.810 | 1.609 | 0.42 | 13.1 GiB | 13.3 GiB |
| Q16 | 1.541 | 0.702 | 0.46 | 8.0 GiB | 6.2 GiB |
| Q28 | 9.255 | 4.240 | 0.46 | 2.5 GiB | 3.6 GiB |
| Q18 | 2.535 | 1.348 | 0.53 | 8.2 GiB | 9.6 GiB |
| Q35 | 0.401 | 0.285 | 0.71 | 3.0 GiB | 2.4 GiB |
| Q15 | 0.450 | 0.331 | 0.74 | 3.0 GiB | 3.3 GiB |
| Q04 | 0.351 | 0.277 | 0.79 | 3.1 GiB | 2.5 GiB |
| Q34 | 2.390 | 2.002 | 0.84 | 12.2 GiB | 9.2 GiB |
| Q05 | 0.410 | 0.345 | 0.84 | 4.4 GiB | 2.6 GiB |
| Q33 | 2.368 | 1.999 | 0.84 | 11.8 GiB | 9.0 GiB |
| Q14 | 0.532 | 0.463 | 0.87 | 4.4 GiB | 2.9 GiB |
| Q08 | 0.395 | 0.345 | 0.87 | 1.9 GiB | 2.8 GiB |
| Q12 | 0.465 | 0.423 | 0.91 | 4.2 GiB | 2.6 GiB |

| slower | v0.9.53 hot | official-main-prs hot | ratio | v0.9.53 peak | official-main-prs peak |
|---|---:|---:|---:|---:|---:|
| Q10 | 0.133 | 0.150 | 1.13 | 1.1 GiB | 1.4 GiB |
| Q29 | 0.052 | 0.063 | 1.21 | 0.9 GiB | 0.8 GiB |
| Q36 | 0.033 | 0.041 | 1.24 | 1.0 GiB | 1.0 GiB |
| Q09 | 0.443 | 0.560 | 1.27 | 1.9 GiB | 3.0 GiB |
| Q11 | 0.122 | 0.161 | 1.31 | 1.0 GiB | 1.4 GiB |
| Q30 | 0.325 | 0.475 | 1.46 | 2.4 GiB | 3.0 GiB |
| Q41 | 0.010 | 0.015 | 1.55 | 0.9 GiB | 0.7 GiB |
| Q42 | 0.009 | 0.015 | 1.62 | 1.0 GiB | 0.6 GiB |
| Q07 | 0.021 | 0.034 | 1.64 | 0.9 GiB | 0.8 GiB |
| Q23 | 0.124 | 0.211 | 1.70 | 2.7 GiB | 3.5 GiB |
| Q38 | 0.013 | 0.022 | 1.72 | 0.9 GiB | 0.8 GiB |
| Q01 | 0.018 | 0.030 | 1.73 | 0.9 GiB | 0.8 GiB |
| Q02 | 0.028 | 0.054 | 1.93 | 0.9 GiB | 1.1 GiB |
| Q25 | 0.110 | 0.234 | 2.14 | 0.9 GiB | 1.3 GiB |
| Q22 | 0.741 | 1.613 | 2.18 | 1.0 GiB | 3.2 GiB |
| Q20 | 0.316 | 0.710 | 2.25 | 0.9 GiB | 1.4 GiB |
| Q03 | 0.028 | 0.064 | 2.28 | 0.9 GiB | 0.9 GiB |
| Q21 | 0.390 | 0.928 | 2.38 | 0.9 GiB | 2.2 GiB |
| Q24 | 0.016 | 0.051 | 3.29 | 0.9 GiB | 1.2 GiB |
| Q00 | 0.005 | 0.016 | 3.38 | 0.2 GiB | 0.4 GiB |
| Q27 | 0.300 | 1.050 | 3.50 | 0.9 GiB | 2.2 GiB |
| Q26 | 0.016 | 0.067 | 4.12 | 0.9 GiB | 1.2 GiB |

