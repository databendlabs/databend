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

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::StringType;
use databend_common_expression::utils::FromData;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

pub struct BuildOptionsTable {
    table_info: TableInfo,
    data: DataBlock,
}

impl SyncSystemTable for BuildOptionsTable {
    const NAME: &'static str = "system.build_options";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, _: Arc<dyn TableContext>) -> Result<DataBlock> {
        Ok(self.data.clone())
    }
}

impl BuildOptionsTable {
    pub fn create(
        table_id: u64,
        cargo_features: Option<&str>,
        target_features: &str,
        build_profile: &str,
        opt_level: &str,
    ) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("category", TableDataType::String),
            TableField::new("name", TableDataType::String),
            TableField::new("value", TableDataType::String),
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

        let cargo_features = cargo_features
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .unwrap_or("not available");

        let target_features = target_features.trim();
        let target_features = if target_features.is_empty() {
            "not available"
        } else {
            target_features
        };

        let mut category = Vec::with_capacity(4);
        let mut name = Vec::with_capacity(4);
        let mut value = Vec::with_capacity(4);

        category.push("cargo".to_string());
        name.push("features".to_string());
        value.push(cargo_features.to_string());

        category.push("target".to_string());
        name.push("features".to_string());
        value.push(target_features.to_string());

        category.push("cargo".to_string());
        name.push("build_profile".to_string());
        value.push(build_profile.to_string());

        category.push("cargo".to_string());
        name.push("opt_level".to_string());
        value.push(opt_level.to_string());

        let data = DataBlock::new_from_columns(vec![
            StringType::from_data(category),
            StringType::from_data(name),
            StringType::from_data(value),
        ]);

        SyncOneBlockSystemTable::create(BuildOptionsTable { table_info, data })
    }
}
