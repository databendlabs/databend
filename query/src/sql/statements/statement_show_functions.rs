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

use std::sync::Arc;

use common_exception::Result;
use common_tracing::tracing;
use sqlparser::ast::Expr;
use sqlparser::ast::Ident;

use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::PlanParser;

#[derive(Debug, Clone, PartialEq)]
pub enum DfShowFunctions {
    All,
    Like(Ident),
    Where(Expr),
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfShowFunctions {
    #[tracing::instrument(level = "debug", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let rewritten_query = self.rewritten_query(ctx.clone());
        let rewritten_query_plan = PlanParser::parse(rewritten_query.as_str(), ctx);
        Ok(AnalyzedResult::SimpleQuery(Box::new(
            rewritten_query_plan.await?,
        )))
    }
}

const FUNCTIONS_TABLE: &str = "system.functions";

impl DfShowFunctions {
    fn show_all_functions(&self, _ctx: Arc<QueryContext>) -> String {
        format!("SELECT name FROM {} ORDER BY name", FUNCTIONS_TABLE,)
    }

    fn show_functions_with_like(&self, i: &Ident, _ctx: Arc<QueryContext>) -> String {
        format!(
            "SELECT name FROM {} where name LIKE {} ORDER BY name",
            FUNCTIONS_TABLE, i,
        )
    }

    fn show_functions_with_predicate(&self, e: &Expr, _ctx: Arc<QueryContext>) -> String {
        format!(
            "SELECT name FROM {} where ({}) ORDER BY name",
            FUNCTIONS_TABLE, e,
        )
    }

    fn rewritten_query(&self, ctx: Arc<QueryContext>) -> String {
        match self {
            DfShowFunctions::All => self.show_all_functions(ctx),
            DfShowFunctions::Like(i) => self.show_functions_with_like(i, ctx),
            DfShowFunctions::Where(e) => self.show_functions_with_predicate(e, ctx),
        }
    }
}
