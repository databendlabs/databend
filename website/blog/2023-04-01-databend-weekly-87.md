---
title: "This Week in Databend #87"
date: 2023-04-01
slug: 2023-04-01-databend-weekly
tags: [databend, weekly]
description: "Stay up to date with the latest weekly developments on Databend!"
contributors:
  - name: andylokandy
  - name: ariesdevil
  - name: b41sh
  - name: BohuTANG
  - name: dantengsky
  - name: Dousir9
  - name: drmingdrmer
  - name: everpcpc
  - name: jun0315
  - name: leiysky
  - name: lichuang
  - name: PsiACE
  - name: RinChanNOWWW
  - name: rkmdCodes
  - name: SkyFan2002
  - name: soyeric128
  - name: sundy-li
  - name: TCeason
  - name: Xuanwo
  - name: xudong963
  - name: youngsofun
  - name: zhang2014
  - name: zhyass
authors:
  - name: PsiACE
    url: https://github.com/psiace
    image_url: https://github.com/psiace.png
---

[Databend](https://github.com/datafuselabs/databend) is a modern cloud data warehouse, serving your massive-scale analytics needs at low cost and complexity. Open source alternative to Snowflake. Also available in the cloud: <https://app.databend.com> .

## What's On In Databend

Stay connected with the latest news about Databend.

### Window Function

Aggregate window functions can operate on a group of rows and return a single value for each row in the underlying query. The `OVER` clause specifies how to partition the rows in the result set. When used with `GROUP BY`, aggregate window functions do not collapse rows but instead return all rows in the result set.

```sql
-- use aggregate window function
SELECT date, AVG(amount) over (partition by date)
FROM BookSold

June 21|544.0
June 21|544.0
June 22|454.5
June 22|454.5
June 23|643.0
June 23|643.0
```

Databend supports all aggregate functions as aggregate window functions.

- [Doc | (draft) Aggregate Window Functions](https://databend.rs/doc/sql-functions/window-functions/aggregate-window-functions)
- [PR #10700 | feat(window): initial impl window function](https://github.com/datafuselabs/databend/pull/10700).

### Suggest Function Name on Typos

Databend has added an intelligent feature that automatically provides the closest matching item when you enter an incorrect function name.

```sql
#> select base64(1);
ERROR 1105 (HY000) at line 1: Code: 1008, displayText = error:
  --> SQL:1:8
  |
1 | select base64(1)
  |        ^^^^^^^^^ no function matches the given name: 'base64', do you mean 'to_base64'?
```

- [PR | feat(planner): suggest function name on typo](https://github.com/datafuselabs/databend/pull/10759)

## Code Corner

Discover some fascinating code snippets or projects that showcase our work or learning journey.

### Dump Running Async Task Stack

Databend now supports dumping the running async task stacks. Simply visit `http://<admin_api_address>/debug/async_tasks/dump` to capture it in your browser.

![Dump Running Async Task Stack](https://user-images.githubusercontent.com/8087042/228602725-a0440e39-3a65-4939-8826-3b92d381cb39.png)

Calling the `async_backtrace::taskdump_tree` function can obtain information about the asynchronous task tree (collected by `#[async_backtrace::framed]`).

```rust
    let tree =
        async_backtrace::taskdump_tree(req.map(|x| x.wait_for_running_tasks).unwrap_or(false));
```

Tasks are divided into regular tasks and polling tasks (marked with `[POLLING]`). Record the stack information for each task, and output it to a string sorted by stack depth.

```rust
    for mut tasks in [tasks, polling_tasks] {
        tasks.sort_by(|l, r| Ord::cmp(&l.stack_frames.len(), &r.stack_frames.len()));

        for item in tasks.into_iter().rev() {
            for frame in item.stack_frames {
                writeln!(output, "{}", frame).unwrap();
            }

            writeln!(output).unwrap();
        }
    }
```

To learn more about how it works, refer to the following link:

- [PR | feat(query): try support dump running async task stack](https://github.com/datafuselabs/databend/pull/10830)

### New Way to Integrate with Jupyter Notebook

As described in [Doc | Visualization Databend Data in Jupyter Notebook](https://databend.rs/doc/integrations/gui-tool/jupyter), we can use Jupyter Notebook to explore data in Databend.

![](https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/integration/integration-jupyter-databend.png)

However, through the trick of magic, [ipython-sql](https://github.com/catherinedevlin/ipython-sql) provides another interesting way to make SQL queries look like they are running in SQL cells and maintain seamless integration with Python.

**Install Dependencies**

```bash
pip install ipython-sql databend-sqlalchemy
```

**Work with Jupyter Notebook**

```sql
In [1]: %load_ext sql

In [2]: %%sql databend://{username}:{password}@{host_port_name}/{database_name}?secure=false
   ...: SHOW DATABASES;

In [3]: result = %%sql SELECT * FROM numbers(100);

In [4]: %matplotlib inline
   ...: df = result.DataFrame()
   ...: df.plot()
```

## Highlights

Here are some noteworthy items recorded here, perhaps you can find something that interests you.

- *[Connecting to Databend with DBeaver](https://databend.rs/blog/dbeaver)*, either by selecting the pre-configured MySQL driver or adding the Databend JDBC driver.
- Databend provides integration with Redash to help you gain better insights into your data. *[Doc | Integrations - Redash](https://databend.rs/doc/integrations/gui-tool/redash)*
- Master how to display information of columns in a given table. *[Doc | SHOW COLUMNS](https://databend.rs/doc/sql-commands/show/show-full-columns)*
- Databend now supports `generate_series` and `range` table functions.
- Databend now supports the `ai_embedding_vector` function, which returns a 1536-dimensional f32 vectors generated by the OpenAI Embeddings API.
- Databend added support for `[CREATE | DROP | SHOW] SHARE ENDPOINT` DDL.

## What's Up Next

We're always open to cutting-edge technologies and innovative ideas. You're more than welcome to join the community and bring them to Databend.

### Collect Metrics from Sled

[sled](https://github.com/spacejam/sled) is an embedded database. It has a metrics feature that exposed some metrics.

Databend Meta Service uses [sled](https://github.com/datafuse-extras/sled) as the underlying Key-Value storage. We expect to obtain more metrics about sled in order to further improve observability and help with optimization.

[Issue #7233 | make use of sled metrics feature for collect sled metrics](https://github.com/datafuselabs/databend/issues/7233)

Please let us know if you're interested in contributing to this issue, or pick up a good first issue at <https://link.databend.rs/i-m-feeling-lucky> to get started.

## New Contributors

We always open arms to everyone and can't wait to see how you'll help our community grow and thrive.

- [@rkmdCodes](https://github.com/rkmdCodes) made their first contribution in [#10269](https://github.com/datafuselabs/databend/pull/10269). This pull request improves the error string and makes the prompt clearer.

## Changelog

You can check the changelog of Databend Nightly for details about our latest developments.

**Full Changelog**: <https://github.com/datafuselabs/databend/compare/v1.0.33-nightly...v1.0.46-nightly>
