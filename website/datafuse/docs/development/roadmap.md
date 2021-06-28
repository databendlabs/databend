---
id: development-roadmap
title: Roadmap 2021
---

Datafuse roadmap 2021.

!!! note "Notes"
    Sync from the [#476](https://github.com/datafuselabs/datafuse/issues/746)


# Main tasks

###  1. Query/Store task

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [ Query/Store API track #745](https://github.com/datafuselabs/datafuse/issues/745)| PROGRESS  |  v0.5 | @dantengsky @drmingdrmer  |

###  2. Distributed Query task

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [Query cluster track #747](https://github.com/datafuselabs/datafuse/issues/747) | PROGRESS  |  v0.5 | @zhang2014 |
| [Functions track #758](https://github.com/datafuselabs/datafuse/issues/758)| PROGRESS  |   | @sundy-li   |
|[Queries track #765](https://github.com/datafuselabs/datafuse/issues/765/)|PROGRESS| | @zhang2014|

###  3. Distributed Store task

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [Store track #271](https://github.com/datafuselabs/datafuse/issues/271) | PROGRESS  |  v0.5 | |


### 4. Observability task
| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [Observability track #795](https://github.com/datafuselabs/datafuse/issues/795) | PROGRESS  |  v0.5 | @BohuTANG  |

### 5. Test infra task

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [Test Infra track #796](https://github.com/datafuselabs/datafuse/issues/796) | PROGRESS  |  v0.4 | @ZhiHanZ  |

### 6. RBAC task

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [RBAC track #894](https://github.com/datafuselabs/datafuse/issues/894) | PLANNING  | v0.5  |  Access Control and Account Management |

### 7. CBO task

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [Cost based optimizer(CBO) track #915](https://github.com/datafuselabs/datafuse/issues/915) | PLANNING  |   |  Table statistics and CBO |

# Experimental and interns tasks

| Task                                         | Status    | Release Target | Comments        | 
| ----------------------------------------------- | --------- | -------------- | --------------- | 
| [Join #559](https://github.com/datafuselabs/datafuse/pull/559) |  PROGRESS | v0.5  | @leiysky  |
| Online Playgroud  | PLANNING  |   | User can try the demo on the datafuse.rs website |
| Sharding |  PLANNING |   | Store supports partition sharding |
| Window functions | PLANNING  |   |  |
| Limited support for transactions | PLANNING  |   |  |
| [Hash method in ClickHouse way #754](https://github.com/datafuselabs/datafuse/issues/754) | PLANNING  |   |  |
| Tuple functions | PLANNING  |   | Reference: https://clickhouse.tech/docs/en/sql-reference/functions/tuple-functions/  |
| Array functions | PLANNING  |   |  Reference: https://clickhouse.tech/docs/en/sql-reference/functions/array-functions/ |
| Lambda functions | PLANNING  |   | Reference: https://clickhouse.tech/docs/en/sql-reference/functions/#higher-order-functions  |
| Compile aggregate functions(JIT) | PLANNING  |   | Reference: https://github.com/ClickHouse/ClickHouse/pull/24789  |

