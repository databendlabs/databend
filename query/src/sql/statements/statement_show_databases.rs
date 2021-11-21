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
use common_tracing::tracing;
use sqlparser::ast::Expr;

use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::PlanParser;

#[derive(Debug, Clone, PartialEq)]
pub struct DfShowDatabases {
    pub where_opt: Option<Expr>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfShowDatabases {
    #[tracing::instrument(level = "info", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let rewritten_query = self.rewritten_query();
        let rewritten_query_plan = PlanParser::parse(rewritten_query.as_str(), ctx);
        Ok(AnalyzedResult::SimpleQuery(rewritten_query_plan.await?))
    }
}

impl DfShowDatabases {
    fn show_all_databases() -> String {
        String::from("SELECT name AS Database FROM system.databases ORDER BY name")
    }

    fn show_databases_with_predicate(expr: &Expr) -> String {
        format!(
            "SELECT name AS Database FROM system.databases WHERE {} ORDER BY name",
            expr
        )
    }

    fn rewritten_query(&self) -> String {
        match &self.where_opt {
            None => Self::show_all_databases(),
            Some(expr) => Self::show_databases_with_predicate(expr),
        }
    }
}
