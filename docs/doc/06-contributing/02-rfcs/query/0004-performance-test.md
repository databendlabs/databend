# Design of test infra for performance test

Currently, we have already supports to run performance tests locally in tests/perfs directory, and here we need to support performance testing in CI

Decoupling compute and storage allow databend to integrate into kubernetes easily.
With the support of kubernetes platform, databend benchmarking could achieve the following advantages:
1. Stable benchmarking results, with containerization and cgroup, easy testing instance could have idempotence computing resource
2. Elastic, can expand or shrink tests on demand and also supports to run test locally(minikube + DinD) or on the cloud
3. For following tests, we need to test on TPC benchmark and integrate databend-dfs storage layer to test infrastructure, thus far kubernetes can help instance scaling easily

## Goals

1. Fast CI speed is desired, By design one performance testing should not exceed two hours(including docker build time and performance testing running time)
2. Expandable: supports to deploy performance tests on scale, and also supports to deploy on a single machine for affordable CI
3. Cloud Native environment supports: Should be able to deploy whole platform on different cloud providers like GKE, EKS
4. High Availability: both webhook and runner should support to self-healing and do not have single point failure
5. Observability: whole process should be observable, should collect logs for performance running instances and collect compare report results

## Non Goals

1. Hybrid Cloud not supported for alpha version
2. Networking optimization for github action part(typically CI fail is caused by networking problem
3. Dashboard(Currently, prototype implemented, but priority here is low)

## Performance Test API

Support three semantics

```bash
/run-perf <branch-name>
```

Compare performance difference between current pull requests’ latest SHA build and given branch name

Branch-name supports:
1. Main branch: main (some repo is main)
2. Release tag branch: i,e v1.1.1-nightly
3. Latest tag: fetch the latest tag in the github repo

```bash
/rerun-perf <branch-name>
```

Similar to run-perf part, the ONLY difference is that it would bypass docker build part and assume performance docker build images are ready for test

Examples:
```bash
/run-perf main
```

It will compare performance between current PR’s latest commit and main branch
```bash
/run-perf v1.1.1-nightly
```

It will compare performance between current PR’s latest commit and release tag v1.1.1-nightly
```bash
/run-perf latest
```

It will compare performance between current PR’s latest commit and the latest release tag
```bash
/rerun-perf main
```

Do the same thing as `/run-perf main` did, but will skip docker image building steps

For more information please checkout [test-infra](https://github.com/datafuselabs/test-infra)