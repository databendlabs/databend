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

use common_exception::Result;
use common_planners::AlterViewPlan;
use common_planners::PlanNode;
use common_tracing::tracing;
use sqlparser::ast::ObjectName;

use crate::sessions::QueryContext;
use crate::sql::statements::resolve_table;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::statements::DfQueryStatement;

#[derive(Debug, Clone, PartialEq)]
pub struct DfAlterView {
    /// View Name
    pub name: ObjectName,
    /// Original SQL String, store in meta service
    pub subquery: String,
    /// Check and Analyze Select query
    pub query: DfQueryStatement,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfAlterView {
    #[tracing::instrument(level = "debug", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        // check whether query is valid
        let _ = self.query.analyze(ctx.clone()).await?;
        let subquery = self.subquery.clone();
        let tenant = ctx.get_tenant();
        let (catalog, db, viewname) = resolve_table(&ctx, &self.name, "ALTER VIEW")?;
        Ok(AnalyzedResult::SimpleQuery(Box::new(PlanNode::AlterView(
            AlterViewPlan {
                tenant,
                catalog,
                db,
                viewname,
                subquery,
            },
        ))))
    }
}
