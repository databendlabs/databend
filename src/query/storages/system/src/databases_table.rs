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

use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::database::Database;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::utils::FromData;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::Object;
use databend_common_users::UserApiProvider;
use databend_common_users::check_database_visibility_with_roles;
use log::warn;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;
use crate::util::extract_leveled_strings;
use crate::util::generate_default_catalog_meta;

pub type DatabasesTableWithHistory = DatabasesTable<true>;
pub type DatabasesTableWithoutHistory = DatabasesTable<false>;

pub struct DatabasesTable<const WITH_HISTORY: bool> {
    table_info: TableInfo,
}

#[async_trait::async_trait]
pub trait HistoryAware {
    const TABLE_NAME: &'static str;
    async fn list_databases(
        catalog: &Arc<dyn Catalog>,
        tenant: &Tenant,
        with_history: bool,
    ) -> Result<Vec<Arc<dyn Database>>>;
}

macro_rules! impl_history_aware {
    ($with_history:expr, $table_name:expr) => {
        #[async_trait::async_trait]
        impl HistoryAware for DatabasesTable<$with_history> {
            const TABLE_NAME: &'static str = $table_name;

            #[async_backtrace::framed]
            async fn list_databases(
                catalog: &Arc<dyn Catalog>,
                tenant: &Tenant,
                with_history: bool,
            ) -> Result<Vec<Arc<dyn Database>>> {
                if with_history {
                    catalog.list_databases_history(tenant).await
                } else {
                    catalog.list_databases(tenant).await
                }
            }
        }
    };
}

impl_history_aware!(true, "databases_with_history");
impl_history_aware!(false, "databases");

#[async_trait::async_trait]
impl<const WITH_HISTORY: bool> AsyncSystemTable for DatabasesTable<WITH_HISTORY>
where DatabasesTable<WITH_HISTORY>: HistoryAware
{
    const NAME: &'static str = Self::TABLE_NAME;

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();

        // Threshold for using optimized path without visibility_checker
        const OPTIMIZED_PATH_THRESHOLD: usize = 20;

        // Extract filters (catalog name and database name)
        let mut filter_catalog_names: Vec<String> = vec![];
        let mut filter_db_names: Vec<String> = vec![];

        if let Some(push_downs) = &push_downs {
            if let Some(filter) = push_downs.filters.as_ref().map(|f| &f.filter) {
                let expr = filter.as_expr(&BUILTIN_FUNCTIONS);
                let func_ctx = ctx.get_function_context()?;
                let (catalog_names, db_names) =
                    extract_leveled_strings(&expr, &["catalog", "name"], &func_ctx)?;
                filter_catalog_names = catalog_names;
                filter_db_names = db_names;
            }
        }

        let ctl = ctx.get_catalog(self.get_table_info().catalog()).await?;
        let current_catalog_name = self.get_table_info().catalog();

        // Determine if current catalog should be included
        let include_current_catalog = filter_catalog_names.is_empty()
            || filter_catalog_names.iter().any(|name| name == current_catalog_name);

        let catalogs = if include_current_catalog {
            vec![(ctl.name(), ctl.clone())]
        } else {
            vec![]
        };

        let user_api = UserApiProvider::instance();
        let mut catalog_names = vec![];
        let mut db_names = vec![];
        let mut db_ids = vec![];
        let mut owners: Vec<Option<String>> = vec![];
        let mut dropped_on: Vec<Option<i64>> = vec![];

        // Optimized path: when filter specifies limited databases,
        // use lightweight permission check without loading all ownerships
        let use_optimized_path = !filter_db_names.is_empty()
            && filter_db_names.len() <= OPTIMIZED_PATH_THRESHOLD
            && !WITH_HISTORY
            && !ctl.is_external();

        if use_optimized_path {
            // Get effective roles once for permission checking
            let current_user = ctx.get_current_user()?;
            let effective_roles = ctx.get_all_effective_roles().await?;

            for (ctl_name, catalog) in catalogs.iter() {
                for db_name in &filter_db_names {
                    match catalog.get_database(&tenant, db_name).await {
                        Ok(db) => {
                            let db_id = db.get_db_info().database_id.db_id;

                            // Get ownership for this specific database
                            let ownership = user_api
                                .get_ownership(&tenant, &OwnershipObject::Database {
                                    catalog_name: ctl_name.to_string(),
                                    db_id,
                                })
                                .await
                                .ok()
                                .flatten();
                            let owner_role = ownership.map(|o| o.role.clone());

                            // Check if user is owner
                            let is_owner = owner_role.as_ref().is_some_and(|role| {
                                effective_roles.iter().any(|r| r.name == *role)
                            });

                            // Check visibility through grants (lightweight check)
                            let is_visible = is_owner
                                || check_database_visibility_with_roles(
                                    &current_user,
                                    &effective_roles,
                                    ctl_name,
                                    db_name,
                                    db_id,
                                );

                            if is_visible {
                                catalog_names.push(ctl_name.to_string());
                                db_names.push(db_name.clone());
                                db_ids.push(db_id);
                                owners.push(owner_role);
                                dropped_on.push(
                                    db.get_db_info().meta.drop_on.map(|v| v.timestamp_micros()),
                                );
                            }
                        }
                        Err(err) => {
                            let msg = format!("Failed to get database: {}, {}", db_name, err);
                            warn!("{}", msg);
                        }
                    }
                }
            }
        } else {
            // Slow path: need full visibility checker
            let visibility_checker = if ctl.is_external() {
                None
            } else {
                Some(ctx.get_visibility_checker(false, Object::All).await?)
            };
            let catalog_dbs = visibility_checker
                .as_ref()
                .and_then(|c| c.get_visibility_database());

            // None means has global level privileges
            if let Some(catalog_dbs) = catalog_dbs {
                if WITH_HISTORY {
                    for (ctl_name, dbs) in catalog_dbs {
                        let catalog = ctx.get_catalog(ctl_name).await?;
                        let dbs_history = catalog.list_databases_history(&tenant).await?;
                        for db_history in dbs_history {
                            let db_name = db_history
                                .get_db_info()
                                .name_ident
                                .database_name()
                                .to_string();
                            let id = db_history.get_db_info().database_id.db_id;
                            if db_ids.contains(&id) {
                                continue;
                            }
                            if dbs.contains(&(None, Some(&id)))
                                || db_name.to_lowercase() == "information_schema"
                                || db_name.to_lowercase() == "system"
                            {
                                catalog_names.push(ctl_name.to_string());
                                db_names.push(db_name);
                                db_ids.push(id);
                                owners.push(
                                    user_api
                                        .get_ownership(&tenant, &OwnershipObject::Database {
                                            catalog_name: ctl_name.to_string(),
                                            db_id: id,
                                        })
                                        .await
                                        .ok()
                                        .and_then(|ownership| ownership.map(|o| o.role.clone())),
                                );
                                dropped_on.push(
                                    db_history
                                        .get_db_info()
                                        .meta
                                        .drop_on
                                        .map(|v| v.timestamp_micros()),
                                );
                            }
                        }
                    }
                } else {
                    for (catalog, dbs) in catalog_dbs {
                        let mut catalog_db_ids = vec![];
                        let mut catalog_db_names = vec![];
                        let ctl = ctx.get_catalog(catalog).await?;
                        catalog_db_names.extend(
                            dbs.iter()
                                .filter_map(|(db_name, _)| *db_name)
                                .map(|db_name| db_name.to_string()),
                        );
                        catalog_db_ids.extend(dbs.iter().filter_map(|(_, db_id)| *db_id));

                        if let Ok(databases) = ctl
                            .mget_database_names_by_ids(&tenant, &catalog_db_ids)
                            .await
                        {
                            catalog_db_names.extend(databases.into_iter().flatten());
                        } else {
                            let msg = format!("Failed to get database name by id: {}", ctl.name());
                            warn!("{}", msg);
                        }
                        let db_idents = catalog_db_names
                            .iter()
                            .map(|name| DatabaseNameIdent::new(&tenant, name))
                            .collect::<Vec<DatabaseNameIdent>>();
                        let dbs = ctl.mget_databases(&tenant, &db_idents).await?;

                        for db in dbs {
                            let db_id = db.get_db_info().database_id.db_id;
                            if db_ids.contains(&db_id) {
                                continue;
                            }
                            catalog_names.push(catalog.to_string());
                            db_names.push(db.get_db_info().name_ident.database_name().to_string());
                            db_ids.push(db_id);
                            owners.push(
                                user_api
                                    .get_ownership(&tenant, &OwnershipObject::Database {
                                        catalog_name: catalog.to_string(),
                                        db_id,
                                    })
                                    .await
                                    .ok()
                                    .and_then(|ownership| ownership.map(|o| o.role.clone())),
                            );
                            dropped_on
                                .push(db.get_db_info().meta.drop_on.map(|v| v.timestamp_micros()));
                        }
                    }
                }
            } else {
                for (ctl_name, catalog) in catalogs.into_iter() {
                    let databases = Self::list_databases(&catalog, &tenant, WITH_HISTORY).await?;
                    let final_dbs = databases
                        .into_iter()
                        .filter(|db| {
                            visibility_checker
                                .as_ref()
                                .map(|c| {
                                    c.check_database_visibility(
                                        &ctl_name,
                                        db.name(),
                                        db.get_db_info().database_id.db_id,
                                    )
                                })
                                .unwrap_or(true)
                        })
                        .collect::<Vec<_>>();

                    for db in final_dbs {
                        catalog_names.push(ctl_name.clone());
                        let db_name = db.name().to_string();
                        db_names.push(db_name);
                        let id = db.get_db_info().database_id.db_id;
                        db_ids.push(id);
                        owners.push(
                            user_api
                                .get_ownership(&tenant, &OwnershipObject::Database {
                                    catalog_name: ctl_name.to_string(),
                                    db_id: id,
                                })
                                .await
                                .ok()
                                .and_then(|ownership| ownership.map(|o| o.role.clone())),
                        );
                        dropped_on.push(db.get_db_info().meta.drop_on.map(|v| v.timestamp_micros()));
                    }
                }
            }
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(catalog_names),
            StringType::from_data(db_names),
            UInt64Type::from_data(db_ids),
            StringType::from_opt_data(owners),
            TimestampType::from_opt_data(dropped_on),
        ]))
    }
}

impl<const WITH_HISTORY: bool> DatabasesTable<WITH_HISTORY>
where DatabasesTable<WITH_HISTORY>: HistoryAware
{
    pub fn create(table_id: u64, ctl_name: &str) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("catalog", TableDataType::String),
            TableField::new("name", TableDataType::String),
            TableField::new("database_id", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "owner",
                TableDataType::Nullable(Box::from(TableDataType::String)),
            ),
            TableField::new(
                "dropped_on",
                TableDataType::Nullable(Box::new(TableDataType::Timestamp)),
            ),
        ]);

        let name = Self::TABLE_NAME;
        let table_info = TableInfo {
            desc: format!("'system'.'{name}'"),
            name: Self::NAME.to_owned(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemDatabases".to_string(),
                ..Default::default()
            },
            catalog_info: Arc::new(CatalogInfo {
                name_ident: CatalogNameIdent::new(Tenant::new_literal("dummy"), ctl_name).into(),
                meta: generate_default_catalog_meta(),
                ..Default::default()
            }),
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(DatabasesTable { table_info })
    }
}
