// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_base::tokio;
use common_exception::Result;
use common_planners::PlanNode;
use criterion::Criterion;
use databend_query::configs::Config;
use databend_query::interpreters::SelectInterpreter;
use databend_query::sessions::SessionManager;
use databend_query::sql::PlanParser;
use futures::StreamExt;

pub mod bench_aggregate_query_sql;
pub mod bench_filter_query_sql;
pub mod bench_limit_query_sql;
pub mod bench_sort_query_sql;

pub async fn select_executor(sql: &str) -> Result<()> {
    let sessions = SessionManager::from_conf(Config::default()).await?;
    let executor_session = sessions.create_session("Benches")?;
    let ctx = executor_session.create_query_context().await?;

    if let PlanNode::Select(plan) = PlanParser::parse(ctx.clone(), sql).await? {
        let executor = SelectInterpreter::try_create(ctx, plan)?;
        let mut stream = executor.execute(None).await?;
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
