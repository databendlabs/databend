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

mod join;
mod select;
mod subquery;

use std::io::Write;
use std::sync::Arc;

use common_ast::parser::error::Backtrace;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_base::infallible::RwLock;
use common_exception::ErrorCode;
use common_exception::Result;
use databend_query::sessions::QueryContext;
use databend_query::sql::optimizer::HeuristicOptimizer;
use databend_query::sql::optimizer::RuleID;
use databend_query::sql::optimizer::RuleList;
use databend_query::sql::plans::Plan;
use databend_query::sql::Binder;
use databend_query::sql::Metadata;

pub(super) struct Suite {
    pub comment: String,
    pub query: String,
    pub rules: Vec<RuleID>,
}

async fn run_test(ctx: Arc<QueryContext>, suite: &Suite) -> Result<String> {
    let tokens = tokenize_sql(&suite.query)?;
    let bt = Backtrace::new();
    let stmts = parse_sql(&tokens, &bt)?;
    if stmts.len() != 1 {
        return Err(ErrorCode::LogicalError("Unsupported statements number"));
    }
    let binder = Binder::new(
        ctx.clone(),
        ctx.get_catalogs(),
        Arc::new(RwLock::new(Metadata::create())),
    );

    let plan = binder.bind(&stmts[0]).await?;

    let result = match plan {
        Plan::Query {
            s_expr, metadata, ..
        } => {
            let mut heuristic_opt = HeuristicOptimizer::new(
                ctx.clone(),
                metadata.clone(),
                RuleList::create(suite.rules.clone())?,
            );
            let optimized = heuristic_opt.optimize(s_expr)?;
            optimized.to_format_tree(&metadata).format_indent()
        }
        _ => Err(ErrorCode::LogicalError("Unsupported non-query statement")),
    }?;

    Ok(result)
}

pub(super) async fn run_suites(
    ctx: Arc<QueryContext>,
    file: &mut std::fs::File,
    suites: &[Suite],
) -> Result<()> {
    for suite in suites {
        let result = run_test(ctx.clone(), suite).await?;

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
