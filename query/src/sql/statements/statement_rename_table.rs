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

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::DropTablePlan;
use common_planners::PlanNode;
use common_planners::RenameTablePlan;
use common_tracing::tracing;
use sqlparser::ast::ObjectName;

use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfRenameTable {
    pub name: ObjectName,
    pub new_name: ObjectName,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfRenameTable {
    #[tracing::instrument(level = "debug", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let tenant = ctx.get_tenant();
        let (db, table_name) = self.resolve_table(ctx.clone(), &self.name)?;
        let (new_db, new_table_name) = self.resolve_table(ctx.clone(), &self.new_name)?;

        Ok(AnalyzedResult::SimpleQuery(Box::new(
            PlanNode::RenameTable(RenameTablePlan {
                tenant,
                db,
                table_name,
                new_db,
                new_table_name,
            }),
        )))
    }
}

impl DfRenameTable {
    fn resolve_table(
        &self,
        ctx: Arc<QueryContext>,
        table_name: &ObjectName,
    ) -> Result<(String, String)> {
        let idents = &table_name.0;
        match idents.len() {
            0 => Err(ErrorCode::SyntaxException("Rename table name is empty")),
            1 => Ok((ctx.get_current_database(), idents[0].value.clone())),
            2 => Ok((idents[0].value.clone(), idents[1].value.clone())),
            _ => Err(ErrorCode::SyntaxException(
                "Rename table name must be [`db`].`table`",
            )),
        }
    }
}
