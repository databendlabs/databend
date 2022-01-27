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

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::AlterTableOperation as AlterTblOp;
use common_planners::AlterTablePlan;
use common_planners::PlanNode;
use common_tracing::tracing;
use sqlparser::ast::AlterTableOperation;
use sqlparser::ast::ObjectName;
use sqlparser::ast::Statement;
use sqlparser::parser::ParserError;

use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfAlterTable {
    pub name: ObjectName,
    pub operation: AlterTableOperation,
}

impl DfAlterTable {
    fn resolve_table(ctx: Arc<QueryContext>, table_name: &ObjectName) -> Result<(String, String)> {
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

#[async_trait::async_trait]
impl AnalyzableStatement for DfAlterTable {
    #[tracing::instrument(level = "debug", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let (db, tbl) = Self::resolve_table(ctx, &self.name)?;
        let op = match &self.operation {
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => Ok(AlterTblOp::RenameColumn {
                old_column_name: old_column_name.to_string(),
                new_column_name: new_column_name.to_string(),
            }),
            // TODO error message
            _ => Err(ErrorCode::UnImplement("not implemented yet")),
        }?;

        Ok(AnalyzedResult::SimpleQuery(Box::new(PlanNode::AlterTable(
            AlterTablePlan {
                database_name: db,
                table_name: tbl,
                operation: op,
            },
        ))))
    }
}

impl std::convert::TryFrom<Statement> for DfAlterTable {
    type Error = ParserError;

    fn try_from(stmt: Statement) -> std::result::Result<Self, Self::Error> {
        match stmt {
            Statement::AlterTable { name, operation } => Ok(Self { name, operation }),
            // TODO refine error message
            _s => Err(ParserError::ParserError("expects alter table".to_owned())),
        }
    }
}
