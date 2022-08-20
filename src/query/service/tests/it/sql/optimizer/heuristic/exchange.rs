// Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_ast::Backtrace;
use common_ast::Dialect;
use common_base::base::tokio;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use databend_query::sessions::QueryContext;
use databend_query::sql::optimizer::HeuristicOptimizer;
use databend_query::sql::optimizer::RuleList;
use databend_query::sql::optimizer::DEFAULT_REWRITE_RULES;
use databend_query::sql::plans::Plan;
use databend_query::sql::Binder;
use databend_query::sql::Metadata;
use databend_query::sql::NameResolutionContext;
use goldenfile::Mint;
use parking_lot::RwLock;

use crate::sql::optimizer::heuristic::run_suites;
use crate::sql::optimizer::heuristic::Suite;
use crate::tests::create_query_context;

async fn run_cluster_test(ctx: Arc<QueryContext>, suite: &Suite) -> Result<String> {
    let tokens = tokenize_sql(&suite.query)?;
    let bt = Backtrace::new();
    let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL, &bt)?;
    let binder = Binder::new(
        ctx.clone(),
        ctx.get_catalog_manager()?,
        NameResolutionContext::default(),
        Arc::new(RwLock::new(Metadata::create())),
    );
    let plan = binder.bind(&stmt).await?;

    let result = match plan {
        Plan::Query {
            s_expr,
            metadata,
            bind_context,
            ..
        } => {
            let mut heuristic_opt = HeuristicOptimizer::new(
                ctx.clone(),
                bind_context,
                metadata.clone(),
                RuleList::create(suite.rules.clone())?,
                true,
            );
            let optimized = heuristic_opt.optimize(s_expr)?;
            optimized.to_format_tree(&metadata).format_indent()
        }
        _ => Err(ErrorCode::LogicalError("Unsupported non-query statement")),
    }?;

    Ok(result)
}

#[tokio::test]
pub async fn test_heuristic_optimizer_exchange() -> Result<()> {
    let mut mint = Mint::new("tests/it/sql/optimizer/heuristic/testdata/");
    let mut file = mint.new_goldenfile("exchange.test")?;

    let ctx = create_query_context().await?;

    let suites = vec![
        Suite {
            comment: "".to_string(),
            query: "select * from numbers(1) t, numbers(2) t1 where t.number = t1.number".to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        },
        Suite {
            comment: "# Result of t1 join t is distributed on t.number".to_string(),
            query: "select * from numbers(1) t, numbers(2) t1, numbers(3) t2 where t.number = t1.number and t.number = t2.number".to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        },
        Suite {
            comment: "".to_string(),
            query: "select * from (select number as a, number+1 as b from numbers(1)) t, numbers(2) t1, numbers(3) t2 where a = t1.number and b = t2.number".to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        },
        Suite {
            comment: "".to_string(),
            query: "select * from (select sum(number) as number from numbers(1) group by number) t, numbers(2) t1 where t.number = t1.number".to_string(),
            rules: DEFAULT_REWRITE_RULES.clone(),
        },
    ];

    run_suites(ctx, &mut file, &suites, run_cluster_test).await
}
