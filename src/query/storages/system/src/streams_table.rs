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
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_storages_fuse::io::SnapshotsIO;
use databend_common_storages_fuse::operations::acquire_task_permit;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_stream::stream_table::StreamTable;
use databend_common_users::UserApiProvider;
use log::warn;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;
use crate::util::find_eq_filter;

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
        let ctls = catalog_mgr
            .list_catalogs(&tenant, ctx.txn_mgr())
            .await?
            .iter()
            .map(|e| (e.name(), e.clone()))
            .collect::<Vec<_>>();
        let visibility_checker = ctx.get_visibility_checker().await?;
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

        for (ctl_name, ctl) in ctls.iter() {
            let mut dbs = Vec::new();
            if let Some(push_downs) = &push_downs {
                let mut db_name = Vec::new();
                if let Some(filter) = push_downs.filters.as_ref().map(|f| &f.filter) {
                    let expr = filter.as_expr(&BUILTIN_FUNCTIONS);
                    find_eq_filter(&expr, &mut |col_name, scalar| {
                        if col_name == "database" {
                            if let Scalar::String(database) = scalar {
                                if !db_name.contains(database) {
                                    db_name.push(database.clone());
                                }
                            }
                        }
                        Ok(())
                    });
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
                        let msg =
                            format!("List databases failed on catalog {}: {}", ctl.name(), err);
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
                        ctl_name,
                        db.name(),
                        db.get_db_info().database_id.db_id,
                    )
                })
                .collect::<Vec<_>>();

            let ownership = if T {
                user_api
                    .get_ownerships(
                        &tenant,
                        ctx.get_settings().get_enable_upgrade_meta_data_to_pb()?,
                    )
                    .await
                    .unwrap_or_default()
            } else {
                HashMap::new()
            };

            let mut source_db_ids = vec![];
            let mut source_tb_ids = vec![];
            for db in final_dbs {
                let db_id = db.get_db_info().database_id.db_id;
                let db_name = db.name();
                let tables = match ctl.list_tables(&tenant, db_name).await {
                    Ok(tables) => tables,
                    Err(err) => {
                        // Swallow the errors related with sharing. Listing tables in a shared database
                        // is easy to get errors with invalid configs, but system.streams is better not
                        // to be affected by it.
                        let msg =
                            format!("Failed to list tables in database: {}, {}", db_name, err);
                        warn!("{}", msg);
                        ctx.push_warning(msg);

                        continue;
                    }
                };

                let mut handlers = Vec::new();
                for table in tables {
                    // If db1 is visible, do not means db1.table1 is visible. An user may have a grant about db1.table2, so db1 is visible
                    // for her, but db1.table1 may be not visible. So we need an extra check about table here after db visibility check.
                    let t_id = table.get_id();
                    if visibility_checker.check_table_visibility(
                        ctl_name,
                        db.name(),
                        table.name(),
                        db_id,
                        t_id,
                    ) && table.engine() == "STREAM"
                    {
                        let stream_info = table.get_table_info();
                        let stream_table = StreamTable::try_from_table(table.as_ref())?;

                        source_db_ids.push(
                            stream_table
                                .source_database_id(ctl.as_ref())
                                .await
                                .unwrap_or(0),
                        );
                        let source_tb_id = stream_table.source_table_id().ok();
                        source_tb_ids.push(source_tb_id.unwrap_or(0));

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
                                let stream_table =
                                    StreamTable::try_from_table(table.as_ref()).unwrap();
                                match stream_table.source_table(ctx).await {
                                    Ok(source) => {
                                        // safe unwrap, has been checked in source_table.
                                        let fuse_table =
                                            FuseTable::try_from_table(source.as_ref()).unwrap();
                                        if let Some(location) = stream_table.snapshot_loc() {
                                            reason = SnapshotsIO::read_snapshot(
                                                location,
                                                fuse_table.get_operator(),
                                            )
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

            let source_db_names = ctl
                .mget_database_names_by_ids(&tenant, &source_db_ids)
                .await?;
            let source_table_names = ctl.mget_table_names_by_ids(&tenant, &source_tb_ids).await?;
            for (db, tb) in source_db_names
                .into_iter()
                .zip(source_table_names.into_iter())
            {
                let desc = match (db, tb) {
                    (Some(db), Some(tb)) => Some(format!("{db}.{tb}")),
                    _ => None,
                };
                table_name.push(desc);
            }
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

    pub fn create(table_id: u64) -> Arc<dyn Table> {
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
            ..Default::default()
        };
        AsyncOneBlockSystemTable::create(StreamsTable::<T> { table_info })
    }
}
