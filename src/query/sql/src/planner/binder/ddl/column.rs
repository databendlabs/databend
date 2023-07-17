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

use common_ast::ast::ShowColumnsStmt;
use common_ast::ast::ShowLimit;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::GrantObject;
use common_meta_app::principal::UserGrantSet;
use common_users::RoleCacheManager;
use tracing::debug;

use crate::normalize_identifier;
use crate::plans::Plan;
use crate::plans::RewriteKind;
use crate::BindContext;
use crate::Binder;
use crate::SelectBuilder;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_columns(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowColumnsStmt,
    ) -> Result<Plan> {
        let ShowColumnsStmt {
            catalog,
            database,
            table,
            full,
            limit,
        } = stmt;

        let catalog_name = match catalog {
            None => self.ctx.get_current_catalog(),
            Some(ident) => {
                let catalog = normalize_identifier(ident, &self.name_resolution_ctx).name;
                self.ctx.get_catalog(&catalog)?;
                catalog
            }
        };
        let catalog = self.ctx.get_catalog(&catalog_name)?;
        let database = match database {
            None => self.ctx.get_current_database(),
            Some(ident) => {
                let database = normalize_identifier(ident, &self.name_resolution_ctx).name;
                catalog
                    .get_database(&self.ctx.get_tenant(), &database)
                    .await?;
                database
            }
        };

        let table = {
            let table = normalize_identifier(table, &self.name_resolution_ctx).name;
            catalog
                .get_table(&self.ctx.get_tenant(), database.as_str(), &table)
                .await?;
            table
        };

        let tenant = self.ctx.get_tenant();
        let user = self.ctx.get_current_user()?;
        let (identity, grant_set) = (user.identity().to_string(), user.grants);

        let (unique_tables, has_object_priv) = generate_unique_object(
            Some(database.clone()),
            Some(table.clone()),
            &tenant,
            grant_set,
        )
        .await?;

        if unique_tables.is_empty() && !has_object_priv {
            return Err(ErrorCode::PermissionDenied(format!(
                "Permission denied, user {} don't have privilege for table {}.{}",
                identity, database, table
            )));
        }

        let mut select_builder = SelectBuilder::from("information_schema.columns");

        select_builder
            .with_column("column_name AS `Field`")
            .with_column("column_type AS `Type`")
            .with_column("is_nullable AS `Null`")
            .with_column("default AS `Default`")
            .with_column("extra AS `Extra`")
            .with_column("column_key AS `Key`");

        if *full {
            select_builder
                .with_column("collation_name AS `Collation`")
                .with_column("privileges AS `Privileges`")
                .with_column("column_comment AS `Comment`");
        }

        select_builder
            .with_order_by("table_schema")
            .with_order_by("table_name")
            .with_order_by("column_name");

        select_builder
            .with_filter(format!("table_schema = '{database}'"))
            .with_filter(format!("table_name = '{table}'"));

        let query = match limit {
            None => select_builder.build(),
            Some(ShowLimit::Like { pattern }) => {
                select_builder.with_filter(format!("column_name LIKE '{pattern}'"));
                select_builder.build()
            }
            Some(ShowLimit::Where { selection }) => {
                select_builder.with_filter(format!("({selection})"));
                select_builder.build()
            }
        };
        debug!("show columns rewrite to: {:?}", query);
        self.bind_rewrite_to_query(bind_context, query.as_str(), RewriteKind::ShowColumns)
            .await
    }
}

pub(crate) async fn generate_unique_object(
    database: Option<String>,
    table: Option<String>,
    tenant: &str,
    grant_set: UserGrantSet,
) -> Result<(HashSet<String>, bool)> {
    let mut unique_object: HashSet<String> = HashSet::new();
    let has_object_priv = RoleCacheManager::instance()
        .find_related_roles(tenant, &grant_set.roles())
        .await?
        .into_iter()
        .map(|role| role.grants)
        .fold(grant_set, |a, b| a | b)
        .entries()
        .iter()
        .any(|e| {
            let object = e.object();
            match object {
                GrantObject::Global => true,
                GrantObject::Database(_, ldb) => {
                    if let Some(database) = &database {
                        // show columns from table from db
                        // show tables from db
                        ldb == database
                    } else {
                        // show databases
                        unique_object.insert(format!("'{}'", ldb));
                        false
                    }
                }
                GrantObject::Table(_, ldb, ltab) => match (&database, &table) {
                    // show columns from tab from db;
                    (Some(database), Some(table)) => ldb == database && ltab == table,
                    // show tables from db;
                    (Some(database), None) => {
                        if ldb == database {
                            unique_object.insert(format!("'{}'", ltab));
                        }
                        false
                    }
                    // show databases
                    (None, None) => {
                        unique_object.insert(format!("'{}'", ldb));
                        false
                    }
                    _ => unreachable!(),
                },
            }
        });
    Ok((unique_object, has_object_priv))
}
