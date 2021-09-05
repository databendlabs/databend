// Copyright 2020 Datafuse Labs.
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

use common_exception::Result;
use common_planners::PlanNode;
use common_runtime::tokio;
use criterion::Criterion;
<<<<<<< HEAD:query/benches/suites/mod.rs
use datafuse_query::interpreters::SelectInterpreter;
use datafuse_query::sql::PlanParser;
use datafuse_query::tests::try_create_session_mgr;
=======
use fuse_query::interpreters::SelectInterpreter;
use fuse_query::sessions::SessionManager;
use fuse_query::sql::PlanParser;
use fuse_query::tests::with_max_connections_sessions;
>>>>>>> cluster_manager:fusequery/query/benches/suites/mod.rs
use futures::StreamExt;

pub mod bench_aggregate_query_sql;
pub mod bench_filter_query_sql;
pub mod bench_limit_query_sql;
pub mod bench_sort_query_sql;

pub async fn select_executor(sql: &str) -> Result<()> {
<<<<<<< HEAD:query/benches/suites/mod.rs
    let session_manager = try_create_session_mgr(Some(1))?;
=======
    let session_manager = with_max_connections_sessions(1)?;
>>>>>>> cluster_manager:fusequery/query/benches/suites/mod.rs
    let executor_session = session_manager.create_session("Benches")?;
    let ctx = executor_session.create_context();

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
