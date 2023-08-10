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

use std::collections::HashSet;
use std::sync::Arc;

use common_catalog::catalog_kind::CATALOG_DEFAULT;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::infer_table_schema;
use common_expression::types::StringType;
use common_expression::utils::FromData;
use common_expression::DataBlock;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_functions::BUILTIN_FUNCTIONS;
use common_meta_app::principal::GrantObject;
use common_meta_app::principal::RoleInfo;
use common_meta_app::principal::UserGrantSet;
use common_meta_app::principal::UserInfo;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_sql::Planner;
use common_storages_view::view_table::QUERY;
use common_storages_view::view_table::VIEW_ENGINE;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;
use crate::util::find_eq_filter;

pub struct ColumnsTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for ColumnsTable {
    const NAME: &'static str = "system.columns";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let rows = self.dump_table_columns(ctx, push_downs).await?;
        let mut names: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut tables: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut databases: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut types: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut data_types: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut default_kinds: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut default_exprs: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut is_nullables: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        let mut comments: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
        for (database_name, table_name, field) in rows.into_iter() {
            names.push(field.name().clone().into_bytes());
            tables.push(table_name.into_bytes());
            databases.push(database_name.into_bytes());
            types.push(field.data_type().wrapped_display().into_bytes());
            let data_type = field.data_type().remove_recursive_nullable().sql_name();
            data_types.push(data_type.into_bytes());

            let mut default_kind = "".to_string();
            let mut default_expr = "".to_string();
            if let Some(expr) = field.default_expr() {
                default_kind = "DEFAULT".to_string();
                default_expr = expr.to_string();
            }
            default_kinds.push(default_kind.into_bytes());
            default_exprs.push(default_expr.into_bytes());
            if field.is_nullable() {
                is_nullables.push("YES".to_string().into_bytes());
            } else {
                is_nullables.push("NO".to_string().into_bytes());
            }

            comments.push("".to_string().into_bytes());
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(databases),
            StringType::from_data(tables),
            StringType::from_data(types),
            StringType::from_data(data_types),
            StringType::from_data(default_kinds),
            StringType::from_data(default_exprs),
            StringType::from_data(is_nullables),
            StringType::from_data(comments),
        ]))
    }
}

impl ColumnsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new("database", TableDataType::String),
            TableField::new("table", TableDataType::String),
            // inner wrapped display style
            TableField::new("type", TableDataType::String),
            // mysql display style for 3rd party tools
            TableField::new("data_type", TableDataType::String),
            TableField::new("default_kind", TableDataType::String),
            TableField::new("default_expression", TableDataType::String),
            TableField::new("is_nullable", TableDataType::String),
            TableField::new("comment", TableDataType::String),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'columns'".to_string(),
            name: "columns".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemColumns".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(ColumnsTable { table_info })
    }

    #[async_backtrace::framed]
    async fn dump_table_columns(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<Vec<(String, String, TableField)>> {
        let tenant = ctx.get_tenant();
        let catalog = ctx.get_catalog(CATALOG_DEFAULT).await?;

        let mut tables = Vec::new();
        let mut databases = Vec::new();

        if let Some(push_downs) = push_downs {
            if let Some(filter) = push_downs.filter {
                let expr = filter.as_expr(&BUILTIN_FUNCTIONS);
                find_eq_filter(&expr, &mut |col_name, scalar| {
                    if col_name == "database" {
                        if let Scalar::String(s) = scalar {
                            if let Ok(database) = String::from_utf8(s.clone()) {
                                databases.push(database);
                            }
                        }
                    } else if col_name == "table" {
                        if let Scalar::String(s) = scalar {
                            if let Ok(table) = String::from_utf8(s.clone()) {
                                tables.push(table);
                            }
                        }
                    }
                });
            }
        }

        if databases.is_empty() {
            let all_databases = catalog.list_databases(tenant.as_str()).await?;
            for db in all_databases {
                databases.push(db.name().to_string());
            }
        }

        let tenant = ctx.get_tenant();
        let user = ctx.get_current_user()?;
        let roles = ctx.get_current_available_roles().await?;
        let visibility_checker = GrantObjectVisibilityChecker::new(&user, &roles);

        let final_dbs: Vec<String> = databases
            .iter()
            .filter(|db| visibility_checker.check_database_visibility(CATALOG_DEFAULT, db))
            .cloned()
            .collect();

        let mut rows: Vec<(String, String, TableField)> = vec![];
        for database in final_dbs {
            let tables = if tables.is_empty() {
                if let Ok(table) = catalog.list_tables(tenant.as_str(), &database).await {
                    table
                } else {
                    vec![]
                }
            } else {
                let mut res = Vec::new();
                for table in &tables {
                    if let Ok(table) = catalog.get_table(tenant.as_str(), &database, table).await {
                        res.push(table);
                    }
                }
                res
            };

            for table in tables {
                if visibility_checker.check_table_visibility(
                    CATALOG_DEFAULT,
                    &database,
                    table.name(),
                ) {
                    let fields = generate_fields(&ctx, &table).await?;
                    for field in fields {
                        rows.push((database.clone(), table.name().into(), field.clone()))
                    }
                }
            }
        }

        Ok(rows)
    }
}

async fn generate_fields(
    ctx: &Arc<dyn TableContext>,
    table: &Arc<dyn Table>,
) -> Result<Vec<TableField>> {
    let fields = if table.engine() == VIEW_ENGINE {
        if let Some(query) = table.options().get(QUERY) {
            let mut planner = Planner::new(ctx.clone());
            let (plan, _) = planner.plan_sql(query).await?;
            let schema = infer_table_schema(&plan.schema())?;
            schema.fields().clone()
        } else {
            return Err(ErrorCode::Internal(
                "Logical error, View Table must have a SelectQuery inside.",
            ));
        }
    } else {
        table.schema().fields().clone()
    };
    Ok(fields)
}

/// GrantObjectVisibilityChecker is used to check whether a user has the privilege to access a
/// database or table.
/// It is used in `SHOW DATABASES` and `SHOW TABLES` statements.
pub struct GrantObjectVisibilityChecker {
    granted_global: bool,
    granted_databases: HashSet<(String, String)>,
    granted_tables: HashSet<(String, String, String)>,
    extra_databases: HashSet<(String, String)>,
}

impl GrantObjectVisibilityChecker {
    pub fn new(user: &UserInfo, available_roles: &Vec<RoleInfo>) -> Self {
        let mut granted_global = false;
        let mut granted_databases = HashSet::new();
        let mut granted_tables = HashSet::new();
        let mut extra_databases = HashSet::new();

        let mut grant_sets: Vec<&UserGrantSet> = vec![&user.grants];
        for role in available_roles {
            grant_sets.push(&role.grants);
        }

        for grant_set in grant_sets {
            for ent in grant_set.entries() {
                match ent.object() {
                    GrantObject::Global => {
                        granted_global = true;
                    }
                    GrantObject::Database(catalog, db) => {
                        granted_databases.insert((catalog.to_string(), db.to_string()));
                    }
                    GrantObject::Table(catalog, db, table) => {
                        granted_tables.insert((
                            catalog.to_string(),
                            db.to_string(),
                            table.to_string(),
                        ));
                        // if table is visible, the table's database is also treated as visible
                        extra_databases.insert((catalog.to_string(), db.to_string()));
                    }
                }
            }
        }

        Self {
            granted_global,
            granted_databases,
            granted_tables,
            extra_databases,
        }
    }

    pub fn check_database_visibility(&self, catalog: &str, db: &str) -> bool {
        if self.granted_global {
            return true;
        }

        if self
            .granted_databases
            .contains(&(catalog.to_string(), db.to_string()))
        {
            return true;
        }

        // if one of the tables in the database is granted, the database is also visible
        if self
            .extra_databases
            .contains(&(catalog.to_string(), db.to_string()))
        {
            return true;
        }

        false
    }

    pub fn check_table_visibility(&self, catalog: &str, database: &str, table: &str) -> bool {
        if self.granted_global {
            return true;
        }

        // if database is granted, all the tables in it are visible
        if self
            .granted_databases
            .contains(&(catalog.to_string(), database.to_string()))
        {
            return true;
        }

        if self.granted_tables.contains(&(
            catalog.to_string(),
            database.to_string(),
            table.to_string(),
        )) {
            return true;
        }

        false
    }
}
