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
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_stream::stream_table::StreamTable;
use databend_common_users::UserApiProvider;
use log::warn;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;
use crate::util::find_eq_filter;

pub struct StreamsTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for StreamsTable {
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
        let ctls: Vec<(String, Arc<dyn Catalog>)> = catalog_mgr
            .list_catalogs(&tenant, ctx.txn_mgr())
            .await?
            .iter()
            .map(|e| (e.name(), e.clone()))
            .collect();

        let user_api = UserApiProvider::instance();

        let mut catalogs = vec![];
        let mut databases = vec![];
        let mut owner = vec![];
        let mut comment = vec![];
        let mut table_id = vec![];
        let mut table_name = vec![];
        let mut invalid_reason = vec![];
        let mut mode = vec![];
        let mut names: Vec<String> = vec![];
        let mut stream_id = vec![];
        let mut created_on = vec![];
        let mut updated_on = vec![];
        let mut table_version = vec![];
        let mut snapshot_location = vec![];

        let visibility_checker = ctx.get_visibility_checker().await?;

        for (ctl_name, ctl) in ctls.into_iter() {
            let mut dbs = Vec::new();
            if let Some(push_downs) = &push_downs {
                let mut db_name: Vec<String> = Vec::new();
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
                    });
                    for db in db_name {
                        if let Ok(database) = ctl.get_database(&tenant, db.as_str()).await {
                            dbs.push(database);
                        }
                    }
                }
            }

            if dbs.is_empty() {
                dbs = ctl.list_databases(&tenant).await?;
            }
            let ctl_name: &str = Box::leak(ctl_name.into_boxed_str());

            let final_dbs = dbs
                .into_iter()
                .filter(|db| {
                    visibility_checker.check_database_visibility(
                        ctl_name,
                        db.name(),
                        db.get_db_info().ident.db_id,
                    )
                })
                .collect::<Vec<_>>();
            for db in final_dbs {
                let name = db.name().to_string().into_boxed_str();
                let name: &str = Box::leak(name);
                let tables = match ctl.list_tables(&tenant, name).await {
                    Ok(tables) => tables,
                    Err(err) => {
                        // Swallow the errors related with sharing. Listing tables in a shared database
                        // is easy to get errors with invalid configs, but system.tables is better not
                        // to be affected by it.
                        if db.get_db_info().meta.from_share.is_some() {
                            warn!("list tables failed on sharing db {}: {}", db.name(), err);
                            continue;
                        }
                        return Err(err);
                    }
                };

                let db_id = db.get_db_info().ident.db_id;

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
                        catalogs.push(ctl_name);
                        databases.push(name);

                        let stream_info = table.get_table_info();
                        names.push(table.name().to_string());
                        stream_id.push(stream_info.ident.table_id);
                        created_on.push(stream_info.meta.created_on.timestamp_micros());
                        updated_on.push(stream_info.meta.updated_on.timestamp_micros());
                        owner.push(
                            user_api
                                .get_ownership(&tenant, &OwnershipObject::Table {
                                    catalog_name: ctl_name.to_string(),
                                    db_id,
                                    table_id: t_id,
                                })
                                .await
                                .ok()
                                .and_then(|ownership| ownership.map(|o| o.role.clone())),
                        );
                        comment.push(stream_info.meta.comment.clone());

                        let stream_table = StreamTable::try_from_table(table.as_ref())?;
                        table_name.push(format!(
                            "{}.{}",
                            stream_table.source_table_database(),
                            stream_table.source_table_name()
                        ));
                        mode.push(stream_table.mode().to_string());
                        table_version.push(stream_table.offset());
                        table_id.push(stream_table.source_table_id());
                        snapshot_location.push(stream_table.snapshot_loc());

                        let mut reason = "".to_string();
                        match stream_table.source_table(ctx.clone()).await {
                            Ok(source) => {
                                let fuse_table = FuseTable::try_from_table(source.as_ref())?;
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
                        invalid_reason.push(reason);
                    }
                }
            }
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(catalogs),
            StringType::from_data(databases),
            StringType::from_data(names),
            UInt64Type::from_data(stream_id),
            TimestampType::from_data(created_on),
            TimestampType::from_data(updated_on),
            StringType::from_data(mode),
            StringType::from_data(comment),
            StringType::from_data(table_name),
            UInt64Type::from_data(table_id),
            UInt64Type::from_data(table_version),
            StringType::from_opt_data(snapshot_location),
            StringType::from_data(invalid_reason),
            StringType::from_opt_data(owner),
        ]))
    }
}

impl StreamsTable {
    pub fn schema() -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new("catalog", TableDataType::String),
            TableField::new("database", TableDataType::String),
            TableField::new("name", TableDataType::String),
            TableField::new("stream_id", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("created_on", TableDataType::Timestamp),
            TableField::new("updated_on", TableDataType::Timestamp),
            TableField::new("mode", TableDataType::String),
            TableField::new("comment", TableDataType::String),
            TableField::new("table_name", TableDataType::String),
            TableField::new("table_id", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "table_version",
                TableDataType::Number(NumberDataType::UInt64),
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
    }

    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let table_info = TableInfo {
            desc: "'system'.'streams'".to_string(),
            name: "streams".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema: StreamsTable::schema(),
                engine: "SystemStreams".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        AsyncOneBlockSystemTable::create(StreamsTable { table_info })
    }
}
