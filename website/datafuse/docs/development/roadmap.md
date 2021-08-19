---
id: development-roadmap
title: Roadmap 2021
---

Datafuse roadmap 2021.

!!! note "Notes"
    Sync from the [#476](https://github.com/datafuselabs/datafuse/issues/746)

# Main tasks

## 1. Query/Store task

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [ Query/Store API track #745](https://github.com/datafuselabs/datafuse/issues/745)| PROGRESS  |  v0.5 |   |
| [ Cloud Re-architected track #1408](https://github.com/datafuselabs/datafuse/issues/1408)| PROGRESS  | v0.5    |   |
| [ Refactor SQL Parser #1218](https://github.com/datafuselabs/datafuse/issues/1218)| PROGRESS  | v0.5    | |
| [ Store Service track #1154](https://github.com/datafuselabs/datafuse/issues/1154)| PLANNING  |   |   |


## 2. Distributed Query task

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [Query cluster track #747](https://github.com/datafuselabs/datafuse/issues/747) | PROGRESS  |  v0.5 | @zhang2014 |
| [Functions track #758](https://github.com/datafuselabs/datafuse/issues/758)| PROGRESS  |   | @sundy-li   |
|[Queries track #765](https://github.com/datafuselabs/datafuse/issues/765/)|PROGRESS| | @zhang2014|

## 3. Distributed Store task

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [Store track #271](https://github.com/datafuselabs/datafuse/issues/271) | PROGRESS  |  v0.5 | |


## 4. Observability task
| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [Observability track #795](https://github.com/datafuselabs/datafuse/issues/795) | PROGRESS  |  v0.5 | @BohuTANG  |

## 5. Test infra task

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [Test Infra track #796](https://github.com/datafuselabs/datafuse/issues/796) | DONE  |  v0.4 | @ZhiHanZ  |

## 6. RBAC task

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [RBAC track #894](https://github.com/datafuselabs/datafuse/issues/894) | PROGRESS  | v0.5  |  Access Control and Account Management |

## 7. CBO task

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [Cost based optimizer(CBO) track #915](https://github.com/datafuselabs/datafuse/issues/915) | PLANNING  |   |  Table statistics and CBO |

## 8. Deployment task

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [ datafuse cli #938](https://github.com/datafuselabs/datafuse/issues/938) | PROGRESS  | v0.5   |  All-in-one tool for setting up, managing with Datafuse |

# Experimental and interns tasks

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [Hash method in ClickHouse way #754](https://github.com/datafuselabs/datafuse/issues/754) | DONE  |   |  |
| [Join #559](https://github.com/datafuselabs/datafuse/pull/559) |  PLANNING |   | @leiysky  |
| Online Playgroud  | PLANNING  |   | User can try the demo on the datafuse.rs website |
| Sharding |  PLANNING |   | Store supports partition sharding |
| Window functions | PLANNING  |   |  |
| Limited support for transactions | PLANNING  |   |  |
| Tuple functions | PLANNING  |   | Reference: https://clickhouse.tech/docs/en/sql-reference/functions/tuple-functions/  |
| Array functions | PLANNING  |   |  Reference: https://clickhouse.tech/docs/en/sql-reference/functions/array-functions/ |
| Lambda functions | PLANNING  |   | Reference: https://clickhouse.tech/docs/en/sql-reference/functions/#higher-order-functions  |
| Compile aggregate functions(JIT) | PLANNING  |   | Reference: https://github.com/ClickHouse/ClickHouse/pull/24789  |
