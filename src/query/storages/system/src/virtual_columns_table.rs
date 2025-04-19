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

use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_storages_fuse::TableContext;
use itertools::Itertools;

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
        _push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let session_state = ctx.session_state();

        let catalog_mgr = CatalogManager::instance();
        let catalog = catalog_mgr.get_default_catalog(session_state)?;

        let mut database_names = Vec::new();
        let mut table_names = Vec::new();
        let mut source_column_names = Vec::new();
        let mut virtual_column_ids = Vec::new();
        let mut virtual_column_names = Vec::new();
        let mut virtual_column_types = Vec::new();

        let dbs = catalog.list_databases(&tenant).await?;
        for db in dbs {
            let tables = catalog.list_tables(&tenant, db.name()).await?;
            for table in tables {
                if !table.support_virtual_columns() {
                    continue;
                }
                let table_info = table.get_table_info();
                let Some(ref virtual_schema) = table_info.meta.virtual_schema else {
                    continue;
                };
                let table_schema = table.schema();
                for virtual_field in &virtual_schema.fields {
                    let Ok(source_field) =
                        table_schema.field_of_column_id(virtual_field.source_column_id)
                    else {
                        continue;
                    };
                    database_names.push(db.name().to_owned());
                    table_names.push(table.name().to_owned());
                    source_column_names.push(source_field.name().clone());
                    virtual_column_ids.push(virtual_field.column_id);
                    virtual_column_names.push(virtual_field.name.clone());

                    let data_types_str = virtual_field
                        .data_types
                        .iter()
                        .map(|ty| format!("{}", ty))
                        .join(", ");
                    virtual_column_types.push(data_types_str);
                }
            }
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(database_names),
            StringType::from_data(table_names),
            StringType::from_data(source_column_names),
            UInt32Type::from_data(virtual_column_ids),
            StringType::from_data(virtual_column_names),
            StringType::from_data(virtual_column_types),
        ]))
    }
}

impl VirtualColumnsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("database", TableDataType::String),
            TableField::new("table", TableDataType::String),
            TableField::new("source_column", TableDataType::String),
            TableField::new(
                "virtual_column_id",
                TableDataType::Number(NumberDataType::UInt32),
            ),
            TableField::new("virtual_column_name", TableDataType::String),
            TableField::new("virtual_column_type", TableDataType::String),
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
