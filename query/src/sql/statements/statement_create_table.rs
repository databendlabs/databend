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

use std::collections::HashMap;
use std::sync::Arc;

use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableMeta;
use common_planners::CreateTablePlan;
use common_planners::PlanNode;
use common_tracing::tracing;
use sqlparser::ast::ColumnDef;
use sqlparser::ast::ObjectName;
use sqlparser::ast::SqlOption;

use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::SQLCommon;

#[derive(Debug, Clone, PartialEq)]
pub struct DfCreateTable {
    pub if_not_exists: bool,
    /// Table name
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub engine: String,
    pub options: Vec<SqlOption>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfCreateTable {
    #[tracing::instrument(level = "info", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let table_meta = self.table_meta()?;
        let if_not_exists = self.if_not_exists;
        let (db, table) = self.resolve_table(ctx)?;

        Ok(AnalyzedResult::SimpleQuery(Box::new(
            PlanNode::CreateTable(CreateTablePlan {
                if_not_exists,
                db,
                table,
                table_meta,
            }),
        )))
    }
}

impl DfCreateTable {
    fn resolve_table(&self, ctx: Arc<QueryContext>) -> Result<(String, String)> {
        let DfCreateTable {
            name: ObjectName(idents),
            ..
        } = self;
        match idents.len() {
            0 => Err(ErrorCode::SyntaxException("Create table name is empty")),
            1 => Ok((ctx.get_current_database(), idents[0].value.clone())),
            2 => Ok((idents[0].value.clone(), idents[1].value.clone())),
            _ => Err(ErrorCode::SyntaxException(
                "Create table name must be [`db`].`table`",
            )),
        }
    }

    fn table_options(&self) -> HashMap<String, String> {
        self.options
            .iter()
            .map(|option| {
                (
                    option.name.value.to_lowercase(),
                    option
                        .value
                        .to_string()
                        .trim_matches(|s| s == '\'' || s == '"')
                        .to_string(),
                )
            })
            .collect()
    }

    fn table_meta(&self) -> Result<TableMeta> {
        let engine = self.engine.clone();
        let schema = self.table_schema()?;
        let options = self.table_options();
        Ok(TableMeta {
            schema,
            engine,
            options,
        })
    }

    fn table_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRefExt::create(
            self.columns
                .iter()
                .map(|column| {
                    SQLCommon::make_data_type(&column.data_type)
                        .map(|data_type| DataField::new(&column.name.value, data_type, false))
                })
                .collect::<Result<Vec<DataField>>>()?,
        ))
    }
}
