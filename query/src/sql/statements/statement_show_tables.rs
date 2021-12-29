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
use sqlparser::ast::ObjectName;

use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::PlanParser;

#[derive(Debug, Clone, PartialEq)]
pub enum DfShowTables {
    All,
    Like(Ident),
    Where(Expr),
    FromOrIn(ObjectName),
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfShowTables {
    #[tracing::instrument(level = "debug", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let rewritten_query = self.rewritten_query(ctx.clone());
        let rewritten_query_plan = PlanParser::parse(rewritten_query.as_str(), ctx);
        Ok(AnalyzedResult::SimpleQuery(Box::new(
            rewritten_query_plan.await?,
        )))
    }
}

impl DfShowTables {
    fn show_all_tables(&self, ctx: Arc<QueryContext>) -> String {
        format!(
            "SELECT created_on, name FROM system.tables where database = '{}' ORDER BY database, name",
            ctx.get_current_database()
        )
    }

    fn show_tables_with_like(&self, i: &Ident, ctx: Arc<QueryContext>) -> String {
        format!(
            "SELECT created_on, name FROM system.tables where database = '{}' AND name LIKE {} ORDER BY database, name",
            ctx.get_current_database(), i,
        )
    }

    fn show_tables_with_predicate(&self, e: &Expr, ctx: Arc<QueryContext>) -> String {
        format!(
            "SELECT created_on, name FROM system.tables where database = '{}' AND ({}) ORDER BY database, name",
            ctx.get_current_database(),
            e,
        )
    }

    fn show_tables_from_db(name: &ObjectName) -> String {
        format!(
            "SELECT name FROM system.tables where database = '{}' ORDER BY database, name",
            name.0[0].value.clone()
        )
    }

    fn rewritten_query(&self, ctx: Arc<QueryContext>) -> String {
        match self {
            DfShowTables::All => self.show_all_tables(ctx),
            DfShowTables::Like(i) => self.show_tables_with_like(i, ctx),
            DfShowTables::Where(e) => self.show_tables_with_predicate(e, ctx),
            DfShowTables::FromOrIn(name) => DfShowTables::show_tables_from_db(name),
        }
    }
}
