// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::Result;
use common_planners::PlanNode;
use criterion::Criterion;
use fuse_query::interpreters::SelectInterpreter;
use fuse_query::sessions::FuseQueryContext;
use fuse_query::sql::PlanParser;
use futures::StreamExt;

pub mod bench_aggregate_query_sql;

pub async fn select_executor(sql: &str) -> Result<()> {
    let ctx = FuseQueryContext::try_create()?;

    if let PlanNode::Select(plan) = PlanParser::create(ctx.clone()).build_from_sql(sql)? {
        let executor = SelectInterpreter::try_create(ctx, plan)?;
        let mut stream = executor.execute().await?;
        while let Some(_block) = stream.next().await {}
    } else {
        assert!(false)
    }
    Ok(())
}

pub fn criterion_benchmark_suite(c: &mut Criterion, sql: &str) {
    c.bench_function(sql, |b| {
        b.iter(|| {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(select_executor(sql))
        })
    });
}
