// Copyright 2021 Datafuse Labs
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

use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_sql::binder::ConstraintExprBinder;
use databend_common_users::Object;
use futures::future::try_join_all;
use log::warn;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;
use crate::util::extract_leveled_strings;

const POINT_GET_TABLE_LIMIT: usize = 20;

pub struct ConstraintsTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for ConstraintsTable {
    const NAME: &'static str = "system.constraints";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let mut filtered_db_names = vec![];
        let mut filtered_table_names = vec![];
        let func_ctx = ctx.get_function_context()?;

        if let Some(filters) = push_downs.and_then(|info| info.filters) {
            let expr = filters.filter.as_expr(&BUILTIN_FUNCTIONS);
            (filtered_db_names, filtered_table_names) =
                extract_leveled_strings(&expr, &["database", "table"], &func_ctx)?;
        }

        let filtered_db_names = if filtered_db_names.is_empty() {
            None
        } else {
            Some(filtered_db_names)
        };

        let filtered_table_names = if filtered_table_names.is_empty() {
            None
        } else {
            Some(filtered_table_names)
        };

        let table_constraint_tables = self
            .list_table_constraints_tables(
                ctx.clone(),
                filtered_db_names.as_deref(),
                filtered_table_names.as_deref(),
            )
            .await?;

        let len = table_constraint_tables.len();
        let mut names = Vec::with_capacity(len);
        let mut types = Vec::with_capacity(len);
        let mut exprs = Vec::with_capacity(len);
        let mut constraint_column_indexes = Vec::with_capacity(len);
        let mut constraint_column_names = Vec::with_capacity(len);
        let mut databases = Vec::with_capacity(len);
        let mut tables = Vec::with_capacity(len);
        let mut created_on = Vec::with_capacity(len);
        let mut updated_on = Vec::with_capacity(len);

        for table in table_constraint_tables {
            let table_schema = table.schema();
            let mut binder =
                ConstraintExprBinder::try_new(ctx.clone(), Arc::new(table_schema.clone().into()))?;

            for (name, constraint) in &table.meta.constraints {
                let expr = constraint.expr();

                names.push(name.clone());
                types.push(constraint.type_name());
                databases.push(table.database_name()?.to_string());
                tables.push(Some(table.name.to_string()));

                let scalar_expr = binder.parse_and_bind(name, &expr)?;
                let columns = scalar_expr
                    .used_columns()
                    .iter()
                    .map(|i| binder.bind_context.columns[*i].column_name.clone())
                    .collect::<Vec<_>>();

                let mut indexes = String::new();

                for (index, column_name) in columns.iter().enumerate() {
                    if index > 0 {
                        indexes.push_str(", ");
                    }
                    indexes.push_str(table_schema.index_of(column_name)?.to_string().as_str());
                }
                constraint_column_indexes.push(indexes);
                constraint_column_names.push(columns.join(", "));
                created_on.push(table.meta.created_on.timestamp_micros());
                updated_on.push(None);
                exprs.push(expr);
            }
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(types),
            StringType::from_data(exprs),
            StringType::from_data(constraint_column_indexes),
            StringType::from_data(constraint_column_names),
            StringType::from_data(databases),
            StringType::from_opt_data(tables),
            TimestampType::from_data(created_on),
            TimestampType::from_opt_data(updated_on),
        ]))
    }
}

impl ConstraintsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new("type", TableDataType::String),
            TableField::new("expressions", TableDataType::String),
            TableField::new("constraint_column_indexes", TableDataType::String),
            TableField::new("constraint_column_names", TableDataType::String),
            TableField::new("database", TableDataType::String),
            TableField::new(
                "table",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new("created_on", TableDataType::Timestamp),
            TableField::new(
                "updated_on",
                TableDataType::Nullable(Box::new(TableDataType::Timestamp)),
            ),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'constraints'".to_string(),
            name: "constraints".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemConstraints".to_string(),

                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(Self { table_info })
    }

    async fn list_table_constraints_tables(
        &self,
        ctx: Arc<dyn TableContext>,
        database_names: Option<&[String]>,
        table_names: Option<&[String]>,
    ) -> Result<Vec<TableInfo>> {
        let tenant = ctx.get_tenant();
        let visibility_checker = ctx.get_visibility_checker(false, Object::All).await?;
        let catalog = ctx.get_catalog(CATALOG_DEFAULT).await?;

        let ctl_name = catalog.name();
        let dbs = match catalog.list_databases(&tenant).await {
            Ok(dbs) => dbs
                .into_iter()
                .filter(|db| {
                    visibility_checker.check_database_visibility(
                        &ctl_name,
                        db.name(),
                        db.get_db_info().database_id.db_id,
                    )
                })
                .collect::<Vec<_>>(),
            Err(err) => {
                let msg = format!("List databases failed on catalog {}: {}", ctl_name, err);
                warn!("{}", msg);
                ctx.push_warning(msg);

                vec![]
            }
        };

        let mut constraint_tables = Vec::new();
        for db in dbs {
            let db_id = db.get_db_info().database_id.db_id;
            let db_name = db.name();

            if let Some(database_names) = database_names {
                if !database_names.iter().any(|name| name == db_name) {
                    continue;
                }
            }
            let tables = match (
                table_names,
                table_names.iter().len() <= POINT_GET_TABLE_LIMIT,
            ) {
                (Some(table_names), true) => {
                    match try_join_all(table_names.iter().map(|table_name| async {
                        db.get_table(table_name)
                            .await
                            .map_err(|err| (table_name.to_string(), err))
                    }))
                    .await
                    {
                        Ok(tables) => tables,
                        Err((table_name, err)) => {
                            let msg = format!(
                                "Failed to get table: {} in database: {}, {}",
                                table_name, db_name, err
                            );
                            warn!("{}", msg);
                            ctx.push_warning(msg);
                            continue;
                        }
                    }
                }
                _ => match catalog.list_tables(&tenant, db_name).await {
                    Ok(tables) => tables,
                    Err(err) => {
                        let msg =
                            format!("Failed to list tables in database: {}, {}", db_name, err);
                        warn!("{}", msg);
                        ctx.push_warning(msg);
                        continue;
                    }
                },
            };
            for table in tables {
                let table_info = table.get_table_info();
                if table_info.meta.constraints.is_empty() {
                    continue;
                }
                if let Some(table_names) = table_names {
                    if !table_names.contains(&table_info.name) {
                        continue;
                    }
                }
                if visibility_checker.check_table_visibility(
                    &ctl_name,
                    db_name,
                    table.name(),
                    db_id,
                    table.get_id(),
                ) {
                    constraint_tables.push(table_info.clone());
                }
            }
        }
        Ok(constraint_tables)
    }
}
