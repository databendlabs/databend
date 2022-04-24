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

use common_base::mask_string;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use serde_json::Value;

use crate::sessions::QueryContext;
use crate::storages::system::table::SyncOneBlockSystemTable;
use crate::storages::system::table::SyncSystemTable;
use crate::storages::Table;

pub struct ConfigsTable {
    table_info: TableInfo,
}

impl SyncSystemTable for ConfigsTable {
    const NAME: &'static str = "system.config";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, ctx: Arc<QueryContext>) -> Result<DataBlock> {
        let config = ctx.get_config();

        let mut names: Vec<String> = vec![];
        let mut values: Vec<String> = vec![];
        let mut groups: Vec<String> = vec![];
        let mut descs: Vec<String> = vec![];

        let query_config = config.query;
        let query_config_value = serde_json::to_value(query_config)?;
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

        // Clone storage config to avoid change it's value.
        //
        // TODO(xuanwo):
        // Refactor into config so that config can  decide which value needs mask.
        let mut storage_config = config.storage;
        storage_config.s3.access_key_id = mask_string(&storage_config.s3.access_key_id, 3);
        storage_config.s3.secret_access_key = mask_string(&storage_config.s3.secret_access_key, 3);
        storage_config.azblob.account_name = mask_string(&storage_config.azblob.account_name, 3);
        storage_config.azblob.account_key = mask_string(&storage_config.azblob.account_key, 3);
        let storage_config_value = serde_json::to_value(storage_config)?;
        ConfigsTable::extract_config(
            &mut names,
            &mut values,
            &mut groups,
            &mut descs,
            "storage".to_string(),
            storage_config_value,
        );

        let names: Vec<&str> = names.iter().map(|x| x.as_str()).collect();
        let values: Vec<&str> = values.iter().map(|x| x.as_str()).collect();
        let groups: Vec<&str> = groups.iter().map(|x| x.as_str()).collect();
        let descs: Vec<&str> = descs.iter().map(|x| x.as_str()).collect();
        Ok(DataBlock::create(self.table_info.schema(), vec![
            Series::from_data(groups),
            Series::from_data(names),
            Series::from_data(values),
            Series::from_data(descs),
        ]))
    }
}

impl ConfigsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("group", Vu8::to_data_type()),
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("value", Vu8::to_data_type()),
            DataField::new("description", Vu8::to_data_type()),
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
        };

        SyncOneBlockSystemTable::create(ConfigsTable { table_info })
    }

    fn extract_config(
        names: &mut Vec<String>,
        values: &mut Vec<String>,
        groups: &mut Vec<String>,
        descs: &mut Vec<String>,
        group: String,
        config_value: Value,
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
        config_value: Value,
        name_prefix: Option<String>,
    ) {
        for (k, v) in config_value.as_object().unwrap().into_iter() {
            match v {
                Value::String(s) => ConfigsTable::push_config(
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
                Value::Number(n) => ConfigsTable::push_config(
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
                Value::Bool(b) => ConfigsTable::push_config(
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
                Value::Object(_) => ConfigsTable::extract_config_with_name_prefix(
                    names,
                    values,
                    groups,
                    descs,
                    group.clone(),
                    v.clone(),
                    Some(k.to_string()),
                ),
                _ => unimplemented!(),
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
}
