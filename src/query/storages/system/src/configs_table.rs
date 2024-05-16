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
use databend_common_config::Config;
use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::utils::FromData;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use itertools::Itertools;
use serde_json::Value as JsonValue;
use serde_json::Value;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

pub struct ConfigsTable {
    table_info: TableInfo,
}

impl SyncSystemTable for ConfigsTable {
    const NAME: &'static str = "system.config";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, _ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let config = GlobalConfig::instance()
            .as_ref()
            .clone()
            .into_config()
            .with_mask();
        let mut names: Vec<String> = vec![];
        let mut values: Vec<String> = vec![];
        let mut groups: Vec<String> = vec![];
        let mut descs: Vec<String> = vec![];

        let query_config = config.query;

        // Obsolete.
        let query_config_value = Self::remove_obsolete_configs(serde_json::to_value(query_config)?);

        ConfigsTable::extract_config(
            &mut names,
            &mut values,
            &mut groups,
            &mut descs,
            "query".to_string(),
            query_config_value,
        );

        let log_config = config.log;
        let log_config_value = serde_json::to_value(log_config)?;
        ConfigsTable::extract_config(
            &mut names,
            &mut values,
            &mut groups,
            &mut descs,
            "log".to_string(),
            log_config_value,
        );

        let meta_config = config.meta;
        let meta_config_value = serde_json::to_value(meta_config)?;
        ConfigsTable::extract_config(
            &mut names,
            &mut values,
            &mut groups,
            &mut descs,
            "meta".to_string(),
            meta_config_value,
        );

        let cache_config = config.cache;
        let cache_config_value = serde_json::to_value(cache_config)?;
        ConfigsTable::extract_config(
            &mut names,
            &mut values,
            &mut groups,
            &mut descs,
            "cache".to_string(),
            cache_config_value,
        );

        let storage_config = config.storage;
        let storage_config_value = serde_json::to_value(storage_config)?;
        ConfigsTable::extract_config(
            &mut names,
            &mut values,
            &mut groups,
            &mut descs,
            "storage".to_string(),
            storage_config_value,
        );

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(groups),
            StringType::from_data(names),
            StringType::from_data(values),
            StringType::from_data(descs),
        ]))
    }
}

impl ConfigsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("group", TableDataType::String),
            TableField::new("name", TableDataType::String),
            TableField::new("value", TableDataType::String),
            TableField::new("description", TableDataType::String),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'configs'".to_string(),
            name: "configs".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemConfigs".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(ConfigsTable { table_info })
    }

    fn extract_config(
        names: &mut Vec<String>,
        values: &mut Vec<String>,
        groups: &mut Vec<String>,
        descs: &mut Vec<String>,
        group: String,
        config_value: JsonValue,
    ) {
        ConfigsTable::extract_config_with_name_prefix(
            names,
            values,
            groups,
            descs,
            group,
            config_value,
            None,
        );
    }

    fn extract_config_with_name_prefix(
        names: &mut Vec<String>,
        values: &mut Vec<String>,
        groups: &mut Vec<String>,
        descs: &mut Vec<String>,
        group: String,
        config_value: JsonValue,
        name_prefix: Option<String>,
    ) {
        for (k, v) in config_value.as_object().unwrap().into_iter() {
            match v {
                JsonValue::String(s) => ConfigsTable::push_config(
                    names,
                    values,
                    groups,
                    descs,
                    k.to_string(),
                    s.to_string(),
                    group.clone(),
                    "".to_string(),
                    name_prefix.clone(),
                ),
                JsonValue::Number(n) => ConfigsTable::push_config(
                    names,
                    values,
                    groups,
                    descs,
                    k.to_string(),
                    n.to_string(),
                    group.clone(),
                    "".to_string(),
                    name_prefix.clone(),
                ),
                JsonValue::Bool(b) => ConfigsTable::push_config(
                    names,
                    values,
                    groups,
                    descs,
                    k.to_string(),
                    b.to_string(),
                    group.clone(),
                    "".to_string(),
                    name_prefix.clone(),
                ),
                JsonValue::Array(v) => ConfigsTable::push_config(
                    names,
                    values,
                    groups,
                    descs,
                    k.to_string(),
                    v.iter().join(","),
                    group.clone(),
                    "".to_string(),
                    name_prefix.clone(),
                ),
                JsonValue::Object(_) => ConfigsTable::extract_config_with_name_prefix(
                    names,
                    values,
                    groups,
                    descs,
                    group.clone(),
                    v.clone(),
                    if let Some(prefix) = &name_prefix {
                        Some(format!("{prefix}.{k}"))
                    } else {
                        Some(k.to_string())
                    },
                ),
                JsonValue::Null => ConfigsTable::push_config(
                    names,
                    values,
                    groups,
                    descs,
                    k.to_string(),
                    "null".to_string(),
                    group.clone(),
                    "".to_string(),
                    name_prefix.clone(),
                ),
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn push_config(
        names: &mut Vec<String>,
        values: &mut Vec<String>,
        groups: &mut Vec<String>,
        descs: &mut Vec<String>,
        name: String,
        value: String,
        group: String,
        desc: String,
        name_prefix: Option<String>,
    ) {
        if let Some(prefix) = name_prefix {
            names.push(format!("{}.{}", prefix, name));
        } else {
            names.push(name);
        }
        values.push(value);
        groups.push(group);
        descs.push(desc);
    }

    fn remove_obsolete_configs(config_json: JsonValue) -> JsonValue {
        match config_json {
            Value::Object(mut config_json_obj) => {
                for key in Config::obsoleted_option_keys().iter() {
                    config_json_obj.remove(*key);
                }
                JsonValue::Object(config_json_obj)
            }
            _ => config_json,
        }
    }
}
