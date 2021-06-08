# Datafuse benchmarking chatbot (standalone) Design Doc
## Summary
Currently we have already supported to run standalone ![benchmarking](https://github.com/datafuselabs/datafuse/tree/master/tests/perfs), basically it will compile code in local machine, and run a suite of SQL to generate a benchmarking html result, and we also have a initial workflow file for perf test up and running in ![here](https://github.com/datafuselabs/datafuse/blob/master/.github/workflows/performance-tests-standalone.yml).
Here the current implementation could be improved by adopting chatbot and build runner follow consistent scripts.

## Goals

**Support the following commands under github PR review**

 `/fusebench-local current`: it will run performance testing for the latest commit under PR

 `/fusebench-local master`: it will run performance testing for master branch

 `/fusebench-local v0.4.1-nightly`: run performance testing for given release tag 

`/fusebench-local cancel`: cancel current benchmarking test

**Support basic member authorization**

Only repository members and owners has permission to run benchmarking test

**Support to show benchmarking results in PR comment**

## Non-Goal

Unified dashboard to show each benchmarking test results, similar to ![testgrid](https://testgrid.k8s.io/istio_release-1.10_common-files_postsubmit)

Slack/Jira notifications

Cluster mode performance testing for different cloud providers like GKE, EKS, AKS, kind etc.

## Implementation
given comment will trigger repository dispatch event in github action workflow(ref: https://docs.github.com/en/actions/reference/events-that-trigger-workflows), and webhook will process comment and start benchmarking workflow
