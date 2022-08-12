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

mod exchange;
mod join;
mod prune_columns;
mod select;
mod subquery;

use std::future::Future;
use std::io::Write;
use std::sync::Arc;

use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_ast::Backtrace;
use common_ast::Dialect;
use common_catalog::catalog::CatalogManager;
use common_exception::ErrorCode;
use common_exception::Result;
use databend_query::catalogs::CatalogManagerHelper;
use databend_query::sessions::QueryContext;
use databend_query::sql::optimizer::HeuristicOptimizer;
use databend_query::sql::optimizer::RuleID;
use databend_query::sql::optimizer::RuleList;
use databend_query::sql::plans::Plan;
use databend_query::sql::Binder;
use databend_query::sql::Metadata;
use databend_query::sql::NameResolutionContext;
use parking_lot::RwLock;

pub(super) struct Suite {
    pub comment: String,
    pub query: String,
    pub rules: Vec<RuleID>,
}

async fn run_test(ctx: Arc<QueryContext>, suite: &Suite) -> Result<String> {
    let tokens = tokenize_sql(&suite.query)?;
    let bt = Backtrace::new();
    let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL, &bt)?;
    let binder = Binder::new(
        ctx.clone(),
        CatalogManager::instance(),
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
                false,
            );
            let optimized = heuristic_opt.optimize(s_expr)?;
            optimized.to_format_tree(&metadata).format_indent()
        }
        _ => Err(ErrorCode::LogicalError("Unsupported non-query statement")),
    }?;

    Ok(result)
}

pub(super) async fn run_suites<'a, Fut: Future<Output = Result<String>>>(
    ctx: Arc<QueryContext>,
    file: &mut std::fs::File,
    suites: &'a [Suite],
    test_func: impl Fn(Arc<QueryContext>, &'a Suite) -> Fut,
) -> Result<()> {
    for suite in suites {
        let result = test_func(ctx.clone(), suite).await?;

        if !suite.comment.is_empty() {
            writeln!(file, "{}", &suite.comment)?;
        }
        writeln!(file, "{}", &suite.query)?;
        writeln!(file, "----")?;
        writeln!(file, "{result}")?;
        writeln!(file)?;
    }

    Ok(())
}
