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
//

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;
use common_tracing::tracing;
use sqlparser::ast::ObjectName;

use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::statements::DfQueryStatement;
use common_planners::CreateViewPlan;

#[derive(Debug, Clone, PartialEq)]
pub struct DfCreateView {
    pub if_not_exists: bool,
    /// View Name
    pub name: ObjectName,
    /// Original SQL String, store in meta service
    pub subquery: String,
    /// Check and Analyze Select query
    pub query: DfQueryStatement,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfCreateView {
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        // check if query is ok
        let _ = self.query.analyze(ctx.clone()).await?;
        // 
        let if_not_exists = self.if_not_exists;
        let subquery = self.subquery.clone();
        let tenant = ctx.get_tenant();
        let (db, viewname) = Self::resolve_viewname(ctx.clone(), &self.name)?;
        Ok(AnalyzedResult::SimpleQuery(Box::new(
            PlanNode::CreateView(CreateViewPlan {
                if_not_exists,
                tenant,
                db,
                viewname,
                subquery
            })
        )))
    }
}

impl DfCreateView {
    fn resolve_viewname(ctx: Arc<QueryContext>, table_name: &ObjectName) -> Result<(String, String)> {
        let idents = &table_name.0;
        match idents.len() {
            0 => Err(ErrorCode::SyntaxException("Create table name is empty")),
            1 => Ok((ctx.get_current_database(), idents[0].value.clone())),
            2 => Ok((idents[0].value.clone(), idents[1].value.clone())),
            _ => Err(ErrorCode::SyntaxException(
                "Create table name must be [`db`].`table`",
            )),
        }
    }
}
