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

use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::InsertIntoPlan;
use common_planners::PlanNode;
use common_tracing::tracing;
use sqlparser::ast::Expr;
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;
use sqlparser::ast::Query;
use sqlparser::ast::SetExpr;
use sqlparser::ast::SqliteOnConflict;
use sqlparser::ast::Values;

use crate::catalogs::Table;
use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::statements::DfQueryStatement;
use crate::sql::DfStatement;
use crate::sql::PlanParser;

#[derive(Debug, Clone, PartialEq)]
pub struct DfInsertStatement {
    pub or: Option<SqliteOnConflict>,
    /// TABLE
    pub table_name: ObjectName,
    /// COLUMNS
    pub columns: Vec<Ident>,
    /// Overwrite (Hive)
    pub overwrite: bool,
    /// A SQL query that specifies what to insert
    pub source: Option<Box<Query>>,
    /// partitioned insert (Hive)
    pub partitioned: Option<Vec<Expr>>,
    /// format name
    pub format: Option<String>,
    /// Columns defined after PARTITION
    pub after_columns: Vec<Ident>,
    /// whether the insert has the table keyword (Hive)
    pub table: bool,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfInsertStatement {
    #[tracing::instrument(level = "info", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        self.is_supported()?;

        match &self.source {
            None => self.analyze_insert_without_source(&ctx).await,
            Some(source) => match &source.body {
                SetExpr::Values(v) => self.analyze_insert_values(&ctx, v).await,
                SetExpr::Select(_) => self.analyze_insert_select(&ctx, source).await,
                _ => Err(ErrorCode::SyntaxException(
                    "Insert must be have values or select.",
                )),
            },
        }
    }
}

impl DfInsertStatement {
    fn resolve_table(&self, ctx: &QueryContext) -> Result<(String, String)> {
        match self.table_name.0.len() {
            0 => Err(ErrorCode::SyntaxException("Insert table name is empty")),
            1 => Ok((
                ctx.get_current_database(),
                self.table_name.0[0].value.clone(),
            )),
            2 => Ok((
                self.table_name.0[0].value.clone(),
                self.table_name.0[1].value.clone(),
            )),
            _ => Err(ErrorCode::SyntaxException(
                "Insert table name must be [`db`].`table`",
            )),
        }
    }

    fn is_supported(&self) -> Result<()> {
        if self.overwrite {
            return Err(ErrorCode::SyntaxException(
                "Unsupport insert overwrite statement.",
            ));
        }

        if self.partitioned.is_some() {
            return Err(ErrorCode::SyntaxException(
                "Unsupport insert ... partition statement.",
            ));
        }

        if !self.after_columns.is_empty() {
            return Err(ErrorCode::SyntaxException(
                "Unsupport specify columns after partitions.",
            ));
        }

        Ok(())
    }

    async fn analyze_insert_values(
        &self,
        ctx: &QueryContext,
        values: &Values,
    ) -> Result<AnalyzedResult> {
        tracing::debug!("{:?}", values);

        let (db, table) = self.resolve_table(ctx)?;
        let write_table = ctx.get_table(&db, &table).await?;
        let table_meta_id = write_table.get_id();
        let schema = self.insert_schema(write_table)?;

        let values = format!("{}", values);
        let values_data = (values["VALUES ".len()..]).to_string();
        Ok(AnalyzedResult::SimpleQuery(PlanNode::InsertInto(
            InsertIntoPlan::insert_values(db, table, table_meta_id, schema, values_data),
        )))
    }

    async fn analyze_insert_without_source(
        &self,
        ctx: &Arc<QueryContext>,
    ) -> Result<AnalyzedResult> {
        let (db, table) = self.resolve_table(ctx)?;
        let write_table = ctx.get_table(&db, &table).await?;
        let table_meta_id = write_table.get_id();
        let table_schema = self.insert_schema(write_table)?;

        Ok(AnalyzedResult::SimpleQuery(PlanNode::InsertInto(
            InsertIntoPlan::insert_without_source(db, table, table_meta_id, table_schema),
        )))
    }

    async fn analyze_insert_select(
        &self,
        ctx: &Arc<QueryContext>,
        source: &Query,
    ) -> Result<AnalyzedResult> {
        let (db, table) = self.resolve_table(ctx)?;
        let write_table = ctx.get_table(&db, &table).await?;
        let table_meta_id = write_table.get_id();
        let table_schema = self.insert_schema(write_table)?;

        let statement = DfQueryStatement::try_from(source.clone())?;
        let select_plan =
            PlanParser::build_plan(vec![DfStatement::Query(statement)], ctx.clone()).await?;
        Ok(AnalyzedResult::SimpleQuery(PlanNode::InsertInto(
            InsertIntoPlan::insert_select(db, table, table_meta_id, table_schema, select_plan),
        )))
    }

    fn insert_schema(&self, read_table: Arc<dyn Table>) -> Result<DataSchemaRef> {
        match self.columns.is_empty() {
            true => Ok(read_table.schema()),
            false => {
                let schema = read_table.schema();
                let fields = self
                    .columns
                    .iter()
                    .map(|ident| schema.field_with_name(&ident.value).map(|v| v.clone()))
                    .collect::<Result<Vec<_>>>()?;

                Ok(DataSchemaRefExt::create(fields))
            }
        }
    }
}
