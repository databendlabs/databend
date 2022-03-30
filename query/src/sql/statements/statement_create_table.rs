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
use sqlparser::ast::ColumnOption;
use sqlparser::ast::ObjectName;

use super::analyzer_expr::ExpressionAnalyzer;
use crate::catalogs::Catalog;
use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::statements::DfQueryStatement;
use crate::sql::DfStatement;
use crate::sql::PlanParser;
use crate::sql::SQLCommon;
use crate::storages::fuse::is_fuse_reserved_opt_key;
use crate::storages::fuse::FUSE_OPT_KEY_DATABASE_ID;

#[derive(Debug, Clone, PartialEq)]
pub struct DfCreateTable {
    pub if_not_exists: bool,
    /// Table name
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub engine: String,
    pub options: HashMap<String, String>,

    // The table name after "create .. like" statement.
    pub like: Option<ObjectName>,

    // The query of "create table .. as select" statement.
    pub query: Option<Box<DfQueryStatement>>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfCreateTable {
    #[tracing::instrument(level = "debug", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let (db, table) = Self::resolve_table(ctx.clone(), &self.name)?;
        let mut table_meta = self.table_meta(ctx.clone(), db.as_str()).await?;
        let if_not_exists = self.if_not_exists;
        let tenant = ctx.get_tenant();
        let as_select_plan_node = match &self.query {
            // CTAS
            Some(query_statement) => {
                let statements = vec![DfStatement::Query(query_statement.clone())];
                let select_plan = PlanParser::build_plan(statements, ctx).await?;

                // The schema contains two parts: create table (if specified) and select.
                let mut fields = table_meta.schema.fields().to_vec();
                let fields_map = fields
                    .iter()
                    .map(|f| (f.name().clone(), f.clone()))
                    .collect::<HashMap<_, _>>();
                for field in select_plan.schema().fields() {
                    if fields_map.get(field.name()).is_none() {
                        fields.push(field.clone());
                    }
                }
                table_meta.schema = DataSchemaRefExt::create(fields);
                Some(Box::new(select_plan))
            }
            // Query doesn't contain 'As Select' statement
            None => None,
        };

        Ok(AnalyzedResult::SimpleQuery(Box::new(
            PlanNode::CreateTable(CreateTablePlan {
                if_not_exists,
                tenant,
                db,
                table,
                table_meta,
                as_select: as_select_plan_node,
            }),
        )))
    }
}

impl DfCreateTable {
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

    async fn table_meta(&self, ctx: Arc<QueryContext>, db_name: &str) -> Result<TableMeta> {
        let engine = self.engine.clone();
        let schema = self.table_schema(ctx.clone()).await?;
        let meta = TableMeta {
            schema,
            engine,
            options: self.options.clone(),
            ..Default::default()
        };
        self.validate_table_options()?;

        self.plan_with_db_id(ctx.as_ref(), db_name, meta).await
    }

    async fn table_schema(&self, ctx: Arc<QueryContext>) -> Result<DataSchemaRef> {
        match &self.like {
            // For create table like statement, for example 'CREATE TABLE test2 LIKE db1.test1',
            // we use the original table's schema.
            Some(like_table_name) => {
                // resolve database and table name from 'like statement'
                let (origin_db_name, origin_table_name) =
                    Self::resolve_table(ctx.clone(), like_table_name)?;

                // use the origin table's schema for the table to create
                let origin_table = ctx.get_table(&origin_db_name, &origin_table_name).await?;
                Ok(origin_table.schema())
            }
            None => {
                let expr_analyzer = ExpressionAnalyzer::create(ctx);
                let mut fields = Vec::with_capacity(self.columns.len());

                for column in &self.columns {
                    //  Defaults to not nullable, if you want to use nullable, you should add `null` into table options
                    // For example: `CREATE TABLE test (id INT NOT NULL, name String NULL)`
                    // Equals to: `CREATE TABLE test (id INT, name String NULL)`
                    let mut nullable = false;
                    let mut default_expr = None;
                    for opt in &column.options {
                        match &opt.option {
                            ColumnOption::Null => {
                                nullable = true;
                            }
                            ColumnOption::Default(expr) => {
                                let expr = expr_analyzer.analyze(expr).await?;
                                default_expr = Some(serde_json::to_vec(&expr)?);
                            }
                            _ => {}
                        }
                    }
                    let field = SQLCommon::make_data_type(&column.data_type).map(|data_type| {
                        if nullable {
                            DataField::new_nullable(&column.name.value, data_type)
                                .with_default_expr(default_expr)
                        } else {
                            DataField::new(&column.name.value, data_type)
                                .with_default_expr(default_expr)
                        }
                    })?;
                    fields.push(field);
                }
                Ok(DataSchemaRefExt::create(fields))
            }
        }
    }

    async fn plan_with_db_id(
        &self,
        ctx: &QueryContext,
        database_name: &str,
        mut meta: TableMeta,
    ) -> Result<TableMeta> {
        if self.engine.to_uppercase().as_str() == "FUSE" {
            // Currently, [Table] can not accesses its database id yet, thus
            // here we keep the db id as an entry of `table_meta.options`.
            //
            // To make the unit/stateless test cases (`show create ..`) easier,
            // here we care about the FUSE engine only.
            //
            // Later, when database id is kept, let say in `TableInfo`, we can
            // safely eliminate this "FUSE" constant and the table meta option entry.
            let catalog = ctx.get_catalog();
            let db = catalog
                .get_database(ctx.get_tenant().as_str(), database_name)
                .await?;
            let db_id = db.get_db_info().database_id;
            meta.options
                .insert(FUSE_OPT_KEY_DATABASE_ID.to_owned(), db_id.to_string());
        }
        Ok(meta)
    }

    fn validate_table_options(&self) -> Result<()> {
        let reserved = self
            .options
            .keys()
            .filter_map(|k| {
                if is_fuse_reserved_opt_key(k) {
                    Some(k.as_str())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if !reserved.is_empty() {
            Err(ErrorCode::BadOption(format!("the following table options are reserved, please do not specify them in the CREATE TABLE statement: {}",
                        reserved.join(",")
                        )))
        } else {
            Ok(())
        }
    }
}
