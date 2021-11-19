---
id: development-roadmap
title: Roadmap 2021
---

Databend roadmap 2021.

!!! note "Notes"
    Sync from the [#476](https://github.com/datafuselabs/databend/issues/746)

# Main tasks

###  1. Query task

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [ Cloud Re-architected track #1408](https://github.com/datafuselabs/databend/issues/1408)| DONE  | v0.5    |   |
| [ HTTP API #2241](https://github.com/datafuselabs/databend/issues/2241)| DONE  | v0.5  |   |
| [ fuse table #1780](https://github.com/datafuselabs/databend/issues/1780)| PROGRESS  |  v0.5 |   |


###  2. Distributed Query task

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [Query cluster track #747](https://github.com/datafuselabs/databend/issues/747) | PROGRESS  |  v0.6 | @zhang2014 |
| [Functions track #758](https://github.com/datafuselabs/databend/issues/758)| PROGRESS  |   | @sundy-li   |
|[Queries track #765](https://github.com/datafuselabs/databend/issues/765/)|PROGRESS| | @zhang2014|


### 3. Observability task
| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [Observability track #795](https://github.com/datafuselabs/databend/issues/795) | PROGRESS  |  v0.6 | @BohuTANG  |

### 4. Test infra task

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [Test Infra track #796](https://github.com/datafuselabs/databend/issues/796) | DONE  |  v0.4 | @ZhiHanZ  |

### 5. RBAC task

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [RBAC track #894](https://github.com/datafuselabs/databend/issues/894) | PROGRESS  | v0.6  |  Access Control and Account Management |

### 6. Optimizer task

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [Cost based optimizer(CBO) track #915](https://github.com/datafuselabs/databend/issues/915) | PLANNING  |   |  Table statistics and CBO |
| [ Refactor SQL Parser #1218](https://github.com/datafuselabs/databend/issues/1218)| PROGRESS  |     | |

### 7. Deployment task

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [ databend cli #938](https://github.com/datafuselabs/databend/issues/938) | PROGRESS  | v0.5   |  All-in-one tool for setting up, managing with Databend |
| online playground  | PROGRESS  |   | User can try the demo on the databend.rs website |

# Experimental and interns tasks

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [Hash method in ClickHouse way #754](https://github.com/datafuselabs/databend/issues/754) | DONE  |   |  |
| [Join #559](https://github.com/datafuselabs/databend/pull/559) |  PLANNING |   | @leiysky  |
| Window functions | PLANNING  |   |  |
| Limited support for transactions | PLANNING  |   |  |
| Tuple functions | PLANNING  |   | Reference: https://clickhouse.tech/docs/en/sql-reference/functions/tuple-functions/  |
| Array functions | PLANNING  |   |  Reference: https://clickhouse.tech/docs/en/sql-reference/functions/array-functions/ |
| Lambda functions | PLANNING  |   | Reference: https://clickhouse.tech/docs/en/sql-reference/functions/#higher-order-functions  |
| Compile aggregate functions(JIT) | PLANNING  |   | Reference: https://github.com/ClickHouse/ClickHouse/pull/24789  |
