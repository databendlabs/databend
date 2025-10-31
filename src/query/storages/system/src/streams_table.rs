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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_base::base::tokio::sync::Semaphore;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::utils::FromData;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_fuse::operations::acquire_task_permit;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_stream::stream_table::StreamTable;
use databend_common_users::Object;
use databend_common_users::UserApiProvider;
use log::warn;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;
use crate::util::extract_leveled_strings;
use crate::util::generate_catalog_meta;

pub type FullStreamsTable = StreamsTable<true>;
pub type TerseStreamsTable = StreamsTable<false>;

pub struct StreamsTable<const FULL: bool> {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl<const T: bool> AsyncSystemTable for StreamsTable<T> {
    const NAME: &'static str = "system.streams";

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

        let catalog_mgr = CatalogManager::instance();
        let ctl = catalog_mgr
            .get_catalog(
                tenant.tenant_name(),
                self.table_info.catalog(),
                ctx.session_state()?,
            )
            .await?;
        let ctl_name = ctl.name();

        let visibility_checker = ctx.get_visibility_checker(false, Object::All).await?;
        let user_api = UserApiProvider::instance();

        let mut catalogs = vec![];
        let mut databases = vec![];
        let mut owner = vec![];
        let mut comment = vec![];
        let mut table_id = vec![];
        let mut table_name = vec![];
        let mut invalid_reason = vec![];
        let mut mode = vec![];
        let mut names = vec![];
        let mut stream_id = vec![];
        let mut created_on = vec![];
        let mut updated_on = vec![];
        let mut table_version = vec![];
        let mut snapshot_location = vec![];

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let io_request_semaphore = Arc::new(Semaphore::new(max_threads));
        let runtime = GlobalIORuntime::instance();

        let mut dbs = Vec::new();
        if let Some(push_downs) = &push_downs {
            if let Some(filter) = push_downs.filters.as_ref().map(|f| &f.filter) {
                let expr = filter.as_expr(&BUILTIN_FUNCTIONS);
                let func_ctx = ctx.get_function_context()?;
                let (db_name, _) = extract_leveled_strings(&expr, &["database"], &func_ctx)?;
                for db in db_name {
                    match ctl.get_database(&tenant, db.as_str()).await {
                        Ok(database) => dbs.push(database),
                        Err(err) => {
                            let msg = format!("Failed to get database: {}, {}", db, err);
                            warn!("{}", msg);
                            ctx.push_warning(msg);
                        }
                    }
                }
            }
        }

        if dbs.is_empty() {
            dbs = match ctl.list_databases(&tenant).await {
                Ok(dbs) => dbs,
                Err(err) => {
                    let msg = format!("List databases failed on catalog {}: {}", ctl.name(), err);
                    warn!("{}", msg);
                    ctx.push_warning(msg);

                    vec![]
                }
            }
        }

        let final_dbs = dbs
            .into_iter()
            .filter(|db| {
                visibility_checker.check_database_visibility(
                    &ctl_name,
                    db.name(),
                    db.get_db_info().database_id.db_id,
                )
            })
            .collect::<Vec<_>>();

        let ownership = if T {
            user_api.list_ownerships(&tenant).await.unwrap_or_default()
        } else {
            HashMap::new()
        };

        let mut source_db_id_set = HashSet::new();
        let mut source_tb_id_set = HashSet::new();
        let mut source_db_tb_ids = vec![];
        for db in final_dbs {
            let db_id = db.get_db_info().database_id.db_id;
            let db_name = db.name();
            let tables = match ctl.list_tables(&tenant, db_name).await {
                Ok(tables) => tables,
                Err(err) => {
                    // Swallow the errors related with sharing. Listing tables in a shared database
                    // is easy to get errors with invalid configs, but system.streams is better not
                    // to be affected by it.
                    let msg = format!("Failed to list tables in database: {}, {}", db_name, err);
                    warn!("{}", msg);
                    ctx.push_warning(msg);

                    continue;
                }
            };

            let mut handlers = Vec::new();
            for table in tables {
                // If db1 is visible, do not mean db1.table1 is visible. A user may have a grant about db1.table2, so db1 is visible
                // for her, but db1.table1 may be not visible. So we need an extra check about table here after db visibility check.
                let t_id = table.get_id();
                if visibility_checker.check_table_visibility(
                    &ctl_name,
                    db.name(),
                    table.name(),
                    db_id,
                    t_id,
                ) && table.is_stream()
                {
                    let stream_info = table.get_table_info();
                    let stream_table = StreamTable::try_from_table(table.as_ref())?;

                    let source_db_id = stream_table.source_database_id(ctl.as_ref()).await.ok();
                    if let Some(source_db_id) = source_db_id {
                        source_db_id_set.insert(source_db_id);
                    }
                    let source_tb_id = stream_table.source_table_id().ok();
                    if let Some(source_tb_id) = source_tb_id {
                        source_tb_id_set.insert(source_tb_id);
                    }
                    match (source_db_id, source_tb_id) {
                        (Some(source_db_id), Some(source_tb_id)) => {
                            source_db_tb_ids.push(Some((source_db_id, source_tb_id)));
                        }
                        (_, _) => {
                            source_db_tb_ids.push(None);
                        }
                    }
                    catalogs.push(ctl_name.as_str());
                    databases.push(db_name.to_owned());
                    names.push(stream_table.name().to_string());
                    mode.push(stream_table.mode().to_string());

                    if T {
                        stream_id.push(stream_info.ident.table_id);
                        created_on.push(stream_info.meta.created_on.timestamp_micros());
                        updated_on.push(stream_info.meta.updated_on.timestamp_micros());

                        if ownership.is_empty() {
                            owner.push(None);
                        } else {
                            owner.push(
                                ownership
                                    .get(&OwnershipObject::Table {
                                        catalog_name: ctl_name.to_string(),
                                        db_id,
                                        table_id: t_id,
                                    })
                                    .map(|role| role.to_string()),
                            );
                        }
                        comment.push(stream_info.meta.comment.clone());

                        table_version.push(stream_table.offset().ok());
                        table_id.push(source_tb_id);
                        snapshot_location.push(stream_table.snapshot_loc());

                        let permit = acquire_task_permit(io_request_semaphore.clone()).await?;
                        let ctx = ctx.clone();
                        let table = table.clone();
                        let handler = runtime.spawn(async move {
                            let mut reason = "".to_string();
                            // safe unwrap.
                            let stream_table = StreamTable::try_from_table(table.as_ref()).unwrap();
                            match stream_table.source_table(ctx).await {
                                Ok(source) => {
                                    // safe unwrap, has been checked in source_table.
                                    let fuse_table =
                                        FuseTable::try_from_table(source.as_ref()).unwrap();
                                    if let Some(location) = stream_table.snapshot_loc() {
                                        reason = fuse_table
                                            .changes_read_offset_snapshot(&location)
                                            .await
                                            .err()
                                            .map_or("".to_string(), |e| e.display_text());
                                    }
                                }
                                Err(e) => {
                                    reason = e.display_text();
                                }
                            }
                            drop(permit);
                            reason
                        });
                        handlers.push(handler);
                    }
                }
            }

            let mut joint = futures::future::try_join_all(handlers)
                .await
                .unwrap_or_default();
            invalid_reason.append(&mut joint);
        }

        let mut source_db_ids = source_db_id_set.into_iter().collect::<Vec<u64>>();
        source_db_ids.sort();
        let source_db_names = ctl
            .mget_database_names_by_ids(&tenant, &source_db_ids)
            .await?;
        let source_db_map = source_db_ids
            .into_iter()
            .zip(source_db_names.into_iter())
            .filter(|(_, db_name)| db_name.is_some())
            .map(|(db_id, db_name)| (db_id, db_name.unwrap()))
            .collect::<HashMap<_, _>>();

        let mut source_tb_ids = source_tb_id_set.into_iter().collect::<Vec<u64>>();
        source_tb_ids.sort();
        let source_tb_names = ctl
            .mget_table_names_by_ids(&tenant, &source_tb_ids, false)
            .await?;
        let source_tb_map = source_tb_ids
            .into_iter()
            .zip(source_tb_names.into_iter())
            .filter(|(_, tb_name)| tb_name.is_some())
            .map(|(tb_id, tb_name)| (tb_id, tb_name.unwrap()))
            .collect::<HashMap<_, _>>();

        for source_db_tb_id in source_db_tb_ids.into_iter() {
            if let Some((db_id, tb_id)) = source_db_tb_id {
                if let Some(db) = source_db_map.get(&db_id) {
                    if let Some(tb) = source_tb_map.get(&tb_id) {
                        table_name.push(Some(format!("{db}.{tb}")));
                        continue;
                    }
                }
            }
            table_name.push(None);
        }

        if T {
            Ok(DataBlock::new_from_columns(vec![
                StringType::from_data(catalogs),
                StringType::from_data(databases),
                StringType::from_data(names),
                UInt64Type::from_data(stream_id),
                TimestampType::from_data(created_on),
                TimestampType::from_data(updated_on),
                StringType::from_data(mode),
                StringType::from_data(comment),
                StringType::from_opt_data(table_name),
                UInt64Type::from_opt_data(table_id),
                UInt64Type::from_opt_data(table_version),
                StringType::from_opt_data(snapshot_location),
                StringType::from_data(invalid_reason),
                StringType::from_opt_data(owner),
            ]))
        } else {
            Ok(DataBlock::new_from_columns(vec![
                StringType::from_data(catalogs),
                StringType::from_data(databases),
                StringType::from_data(names),
                StringType::from_data(mode),
                StringType::from_opt_data(table_name),
            ]))
        }
    }
}

impl<const T: bool> StreamsTable<T> {
    pub fn schema() -> TableSchemaRef {
        if T {
            TableSchemaRefExt::create(vec![
                TableField::new("catalog", TableDataType::String),
                TableField::new("database", TableDataType::String),
                TableField::new("name", TableDataType::String),
                TableField::new("stream_id", TableDataType::Number(NumberDataType::UInt64)),
                TableField::new("created_on", TableDataType::Timestamp),
                TableField::new("updated_on", TableDataType::Timestamp),
                TableField::new("mode", TableDataType::String),
                TableField::new("comment", TableDataType::String),
                TableField::new(
                    "table_name",
                    TableDataType::Nullable(Box::new(TableDataType::String)),
                ),
                TableField::new(
                    "table_id",
                    TableDataType::Nullable(Box::new(TableDataType::Number(
                        NumberDataType::UInt64,
                    ))),
                ),
                TableField::new(
                    "table_version",
                    TableDataType::Nullable(Box::new(TableDataType::Number(
                        NumberDataType::UInt64,
                    ))),
                ),
                TableField::new(
                    "snapshot_location",
                    TableDataType::Nullable(Box::new(TableDataType::String)),
                ),
                TableField::new("invalid_reason", TableDataType::String),
                TableField::new(
                    "owner",
                    TableDataType::Nullable(Box::new(TableDataType::String)),
                ),
            ])
        } else {
            TableSchemaRefExt::create(vec![
                TableField::new("catalog", TableDataType::String),
                TableField::new("database", TableDataType::String),
                TableField::new("name", TableDataType::String),
                TableField::new("mode", TableDataType::String),
                TableField::new(
                    "table_name",
                    TableDataType::Nullable(Box::new(TableDataType::String)),
                ),
            ])
        }
    }

    pub fn create(table_id: u64, ctl_name: &str) -> Arc<dyn Table> {
        let name = if T { "streams" } else { "streams_terse" };
        let table_info = TableInfo {
            desc: format!("'system'.'{name}'"),
            name: name.to_owned(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema: StreamsTable::<T>::schema(),
                engine: "SystemStreams".to_string(),
                ..Default::default()
            },
            catalog_info: Arc::new(CatalogInfo {
                name_ident: CatalogNameIdent::new(Tenant::new_literal("dummy"), ctl_name).into(),
                meta: generate_catalog_meta(ctl_name),
                ..Default::default()
            }),
            ..Default::default()
        };
        AsyncOneBlockSystemTable::create(StreamsTable::<T> { table_info })
    }
}
