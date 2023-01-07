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

use std::sync::Arc;

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::types::StringType;
use common_expression::utils::FromData;
use common_expression::DataBlock;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_storages_system::SyncOneBlockSystemTable;
use common_storages_system::SyncSystemTable;

build_info::build_info!(pub fn build_info);

pub struct BuildOptionsTable {
    table_info: TableInfo,
}

impl SyncSystemTable for BuildOptionsTable {
    const NAME: &'static str = "system.build_options";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, _: Arc<dyn TableContext>) -> Result<DataBlock> {
        let crate_info = &build_info().crate_info;

        let available_features: Vec<Vec<u8>> = crate_info
            .available_features
            .iter()
            .map(|x| x.as_bytes().to_vec())
            .collect();

        let mut enabled = vec![false; available_features.len()];

        for i in 0..crate_info.enabled_features.len() {
            for j in 0..crate_info.available_features.len() {
                if crate_info.enabled_features[i] == crate_info.available_features[j] {
                    enabled[j] = true;
                }
            }
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(available_features),
            BooleanType::from_data(enabled),
        ]))
    }
}

impl BuildOptionsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("available_features", TableDataType::String),
            TableField::new("enabled", TableDataType::Boolean),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'build_options'".to_string(),
            name: "build_options".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemBuildOptions".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(BuildOptionsTable { table_info })
    }
}
