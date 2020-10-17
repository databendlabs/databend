[![Github Actions Status](https://github.com/datafusedev/fuse-query/workflows/FuseQuery%20Lint/badge.svg)](https://github.com/datafusedev/fuse-query/actions?query=workflow%3A%22FuseQuery+Lint%22)
[![Github Actions Status](https://github.com/datafusedev/fuse-query/workflows/FuseQuery%20Test/badge.svg)](https://github.com/datafusedev/fuse-query/actions?query=workflow%3A%22FuseQuery+Test%22)
[![codecov.io](https://codecov.io/gh/datafusedev/fuse-query/graphs/badge.svg)](https://codecov.io/gh/datafusedev/fuse-query/branch/master)
[![License](https://img.shields.io/badge/License-AGPL%203.0-blue.svg)](https://opensource.org/licenses/AGPL-3.0)

# FuseQuery

FuseQuery is a Distributed SQL Query Engine at scale.

## Features

* **High Performance**
* **High Scalability**
* **High Reliability**


## Architecture

| Crate     | Description |  Status |
|-----------|-------------|-------------|
| processors | Query execution pipeline | WIP |
| transforms | Query execution transform | WIP |
| planners | Distributed plan for queries and DML statements | WIP |
| optimizers | Optimizer for distributed plan | TODO |
| functions | Scalar and Aggregation functions | WIP |
| datablocks | Vectorized data processing unit | WIP |
| datastreams | Streaming iterator for DataBlock | WIP |
| datasources | Interface to the fuse-store server | WIP | 
| distributed | Distributed scheduler and executor for planner | TODO |

