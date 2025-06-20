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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::HistoryConfig;

const TABLES_TOML: &str = include_str!("./history_tables.toml");

#[derive(Debug)]
pub struct HistoryTable {
    pub name: String,
    pub create: String,
    pub transform: String,
    pub delete: String,
}

impl HistoryTable {
    pub fn create(predefined: PredefinedTable, retention: u64) -> Self {
        HistoryTable {
            name: predefined.name,
            create: predefined.create,
            transform: predefined.transform,
            delete: predefined
                .delete
                .replace("{retention_hours}", &retention.to_string()),
        }
    }

    pub fn assemble_log_history_transform(&self, stage_name: &str, batch_number: u64) -> String {
        let mut transform = self.transform.clone();
        transform = transform.replace("{stage_name}", stage_name);
        transform = transform.replace("{batch_number}", &batch_number.to_string());
        transform
    }

    pub fn assemble_normal_transform(&self, begin: u64, end: u64) -> String {
        let mut transform = self.transform.clone();
        transform = transform.replace("{batch_begin}", &begin.to_string());
        transform = transform.replace("{batch_end}", &end.to_string());
        transform
    }
}

#[derive(serde::Deserialize)]
pub struct PredefinedTables {
    pub tables: Vec<PredefinedTable>,
}

#[derive(serde::Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct PredefinedTable {
    pub name: String,
    pub target: String,
    pub create: String,
    pub transform: String,
    pub delete: String,
}

pub fn init_history_tables(cfg: &HistoryConfig) -> Result<Vec<Arc<HistoryTable>>> {
    let predefined_tables: PredefinedTables =
        toml::from_str(TABLES_TOML).expect("Failed to parse toml");

    let mut predefined_map: BTreeMap<String, PredefinedTable> = BTreeMap::from_iter(
        predefined_tables
            .tables
            .into_iter()
            .map(|table| (table.name.clone(), table)),
    );

    let mut history_tables = Vec::with_capacity(cfg.tables.len());
    // log_history is the source table, it is always included
    // if user defined log_history, we will use the user defined retention
    // if user did not define log_history, we will use the default retention of 24*7 hours
    let mut user_defined_log_history = false;
    for enable_table in cfg.tables.iter() {
        if enable_table.table_name == "log_history" {
            user_defined_log_history = true;
        }
        if let Some(predefined_table) = predefined_map.remove(&enable_table.table_name) {
            let retention = enable_table.retention;
            history_tables.push(Arc::new(HistoryTable::create(
                predefined_table,
                retention as u64,
            )));
        } else {
            return Err(ErrorCode::InvalidConfig(format!(
                "Invalid history table name {}",
                enable_table.table_name
            )));
        }
    }
    if !user_defined_log_history {
        history_tables.push(Arc::new(HistoryTable::create(
            predefined_map.remove("log_history").unwrap(),
            24 * 7,
        )));
    }
    Ok(history_tables)
}

pub fn table_to_target() -> HashMap<String, String> {
    let predefined_tables: PredefinedTables =
        toml::from_str(TABLES_TOML).expect("Failed to parse toml");
    let mut table_to_target = HashMap::new();
    for table in predefined_tables.tables {
        if table.name != "log_history" {
            table_to_target.insert(table.name, table.target);
        }
    }
    table_to_target
}

pub fn get_all_history_table_names() -> Vec<String> {
    let predefined_tables: PredefinedTables =
        toml::from_str(TABLES_TOML).expect("Failed to parse toml");
    predefined_tables
        .tables
        .into_iter()
        .map(|t| t.name)
        .collect()
}
