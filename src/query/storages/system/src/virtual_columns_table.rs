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

use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::ListVirtualColumnsReq;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::VirtualColumnMeta;
use databend_common_meta_types::MetaId;
use databend_common_storages_fuse::TableContext;

use crate::columns_table::dump_tables;
use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct VirtualColumnsTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for VirtualColumnsTable {
    const NAME: &'static str = "system.virtual_columns";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let catalog = ctx.get_catalog(CATALOG_DEFAULT).await?;
        let req = ListVirtualColumnsReq::new(tenant, None);
        let virtual_column_metas = catalog.list_virtual_columns(req).await?;

        let mut database_names = Vec::with_capacity(virtual_column_metas.len());
        let mut table_names: Vec<String> = Vec::with_capacity(virtual_column_metas.len());
        let mut virtual_columns = Vec::with_capacity(virtual_column_metas.len());
        let mut created_on_columns = Vec::with_capacity(virtual_column_metas.len());
        let mut updated_on_columns = Vec::with_capacity(virtual_column_metas.len());
        if !virtual_column_metas.is_empty() {
            let mut virtual_column_meta_map: HashMap<MetaId, VirtualColumnMeta> =
                virtual_column_metas
                    .into_iter()
                    .map(|v| (v.table_id, v))
                    .collect();

            let database_and_tables = dump_tables(&ctx, push_downs).await?;
            for (database, tables) in database_and_tables {
                for table in tables {
                    let table_id = table.get_id();
                    if let Some(virtual_column_meta) = virtual_column_meta_map.remove(&table_id) {
                        database_names.push(database.clone());
                        table_names.push(table.name().to_string());
                        virtual_columns.push(virtual_column_meta.virtual_columns.join(", "));
                        created_on_columns.push(virtual_column_meta.created_on.timestamp_micros());
                        updated_on_columns
                            .push(virtual_column_meta.updated_on.map(|u| u.timestamp_micros()));
                    }
                }
            }
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(database_names),
            StringType::from_data(table_names),
            StringType::from_data(virtual_columns),
            TimestampType::from_data(created_on_columns),
            TimestampType::from_opt_data(updated_on_columns),
        ]))
    }
}

impl VirtualColumnsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("database", TableDataType::String),
            TableField::new("table", TableDataType::String),
            TableField::new("virtual_columns", TableDataType::String),
            TableField::new("created_on", TableDataType::Timestamp),
            TableField::new(
                "updated_on",
                TableDataType::Nullable(Box::new(TableDataType::Timestamp)),
            ),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'virtual_columns'".to_string(),
            name: "virtual_columns".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemVirtualColumns".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(Self { table_info })
    }
}
