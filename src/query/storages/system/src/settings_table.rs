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
use databend_common_expression::types::StringType;
use databend_common_expression::utils::FromData;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::principal::UserSettingValue;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use snailquote::escape;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

pub struct SettingsTable {
    table_info: TableInfo,
}

impl SyncSystemTable for SettingsTable {
    const NAME: &'static str = "system.settings";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let settings = ctx.get_settings();

        let mut names: Vec<String> = vec![];
        let mut values: Vec<String> = vec![];
        let mut defaults: Vec<String> = vec![];
        let mut ranges: Vec<String> = vec![];
        let mut levels: Vec<String> = vec![];
        let mut descs: Vec<String> = vec![];
        let mut types: Vec<String> = vec![];
        for item in settings.into_iter() {
            // Name.
            names.push(item.name);

            // Value.
            values.push(escape(format!("{:?}", item.user_value).as_str()).to_string());

            // Default Value.
            defaults.push(escape(format!("{:?}", item.default_value).as_str()).to_string());

            // Range Value.
            match item.range {
                Some(range) => ranges.push(format!("{}", range)),
                None => ranges.push("None".to_string()),
            }

            // Scope level.
            levels.push(format!("{:?}", item.level));

            // Desc.
            descs.push(item.desc.to_string());

            let typename = match item.user_value {
                UserSettingValue::UInt64(_) => "UInt64",
                UserSettingValue::String(_) => "String",
            };
            // Types.
            types.push(typename.to_string());
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(values),
            StringType::from_data(defaults),
            StringType::from_data(ranges),
            StringType::from_data(levels),
            StringType::from_data(descs),
            StringType::from_data(types),
        ]))
    }
}

impl SettingsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new("value", TableDataType::String),
            TableField::new("default", TableDataType::String),
            TableField::new("range", TableDataType::String),
            TableField::new("level", TableDataType::String),
            TableField::new("description", TableDataType::String),
            TableField::new("type", TableDataType::String),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'settings'".to_string(),
            name: "settings".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemSettings".to_string(),

                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(SettingsTable { table_info })
    }
}
