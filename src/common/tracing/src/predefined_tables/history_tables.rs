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
const DEFAULT_RETENTION_HOURS: u64 = 24 * 7;
#[cfg(test)]
const LINEAGE_HISTORY_TABLE: &str = "lineage_history";

#[derive(Debug)]
pub struct HistoryTable {
    pub name: String,
    pub create: String,
    pub transforms: Vec<String>,
    pub delete: Option<String>,
}

impl HistoryTable {
    pub fn create(predefined: PredefinedTable, retention: Option<u64>) -> Self {
        let transforms = predefined.transforms();
        let retention = retention
            .or_else(|| (!predefined.permanent_by_default).then_some(DEFAULT_RETENTION_HOURS));
        HistoryTable {
            name: predefined.name,
            create: predefined.create,
            transforms,
            delete: retention.map(|retention| {
                predefined
                    .delete
                    .replace("{retention_hours}", &retention.to_string())
            }),
        }
    }

    pub fn assemble_log_history_transforms(
        &self,
        stage_name: &str,
        batch_number: u64,
    ) -> Vec<String> {
        self.transforms
            .iter()
            .map(|transform| {
                transform
                    .replace("{stage_name}", stage_name)
                    .replace("{batch_number}", &batch_number.to_string())
            })
            .collect()
    }

    pub fn assemble_normal_transforms(&self, begin: u64, end: u64) -> Vec<String> {
        self.transforms
            .iter()
            .map(|transform| {
                transform
                    .replace("{batch_begin}", &begin.to_string())
                    .replace("{batch_end}", &end.to_string())
            })
            .collect()
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
    #[serde(default)]
    pub additional_transforms: Vec<String>,
    #[serde(default)]
    pub permanent_by_default: bool,
    pub delete: String,
}

impl PredefinedTable {
    fn transforms(&self) -> Vec<String> {
        std::iter::once(self.transform.clone())
            .chain(self.additional_transforms.iter().cloned())
            .collect()
    }
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
            history_tables.push(Arc::new(HistoryTable::create(
                predefined_table,
                enable_table.retention.map(|retention| retention as u64),
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
            None,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lineage_history_uses_hash_pattern_merge() {
        let tables: PredefinedTables = toml::from_str(TABLES_TOML).unwrap();
        let lineage = tables
            .tables
            .iter()
            .find(|table| table.name == "lineage_history")
            .unwrap();

        assert!(lineage.permanent_by_default);
        assert!(
            lineage
                .create
                .contains("column_lineage_hash STRING NOT NULL")
        );
        for field in [
            "updated_on TIMESTAMP",
            "user_name STRING",
            "query_parameterized_hash STRING",
            "query_info VARIANT",
            "column_lineage VARIANT",
        ] {
            assert!(lineage.create.contains(field), "missing field {field}");
        }
        for removed in [
            "query_kind STRING",
            "query_text STRING",
            "source_to_target_columns MAP",
            "target_to_source_columns MAP",
        ] {
            assert!(
                !lineage.create.contains(removed),
                "obsolete field {removed}"
            );
        }
        assert!(
            lineage
                .transform
                .contains("MERGE INTO system_history.lineage_history")
        );
        assert!(lineage.create.contains("source_catalog_type STRING"));
        assert!(lineage.create.contains("target_catalog_type STRING"));
        assert!(lineage.transform.contains("AS source_catalog_type"));
        for field in [
            "AS updated_on",
            "AS user_name",
            "AS query_parameterized_hash",
            "AS query_info",
            "AS column_lineage",
        ] {
            assert!(
                lineage.transform.contains(field),
                "missing projection {field}"
            );
        }
        assert!(lineage.delete.contains("lineage_kind = 'DML'"));
        assert!(lineage.delete.contains("updated_on <"));
        assert!(
            lineage
                .transform
                .contains("target.column_lineage_hash = source.column_lineage_hash")
        );
        assert!(lineage.transform.contains(
            "PARTITION BY m['source']['lineage_key']::STRING, m['target']['lineage_key']::STRING, m['lineage_kind']::STRING, m['column_lineage_hash']::STRING"
        ));
        assert!(
            lineage
                .transform
                .contains("m['query_info']['query_id']::STRING DESC")
        );
        assert!(lineage.transform.contains(
            "target.source_lineage_key = source.source_lineage_key AND target.target_lineage_key = source.target_lineage_key AND target.lineage_kind = source.lineage_kind"
        ));
        assert!(
            lineage
                .transform
                .contains("WHEN MATCHED THEN UPDATE * WHEN NOT MATCHED THEN INSERT *")
        );
        assert!(
            lineage
                .transform
                .contains("coalesce(m['operation']::STRING, 'UPSERT_EDGE') = 'UPSERT_EDGE'")
        );
        assert_eq!(lineage.additional_transforms.len(), 1);
        assert!(lineage.additional_transforms[0].contains("DELETE_OBJECT"));
        assert!(lineage.additional_transforms[0].contains("source_id IN"));
        assert!(lineage.additional_transforms[0].contains("target_id IN"));
    }

    #[test]
    fn test_lineage_same_batch_tombstone_replay_order() {
        let tables: PredefinedTables = toml::from_str(TABLES_TOML).unwrap();
        let lineage = tables
            .tables
            .into_iter()
            .find(|table| table.name == LINEAGE_HISTORY_TABLE)
            .unwrap();
        let history = HistoryTable::create(lineage, Some(DEFAULT_RETENTION_HOURS));

        // A same-batch tombstone must run after edge upserts. Replaying a batch repeats the same
        // order, so the MERGE cannot resurrect an edge removed by the tombstone phase.
        let first_attempt = history.assemble_normal_transforms(17, 23);
        let replay = history.assemble_normal_transforms(17, 23);
        assert_eq!(first_attempt, replay);
        assert_eq!(first_attempt.len(), 2);
        assert!(first_attempt[0].contains("MERGE INTO system_history.lineage_history"));
        assert!(first_attempt[0].contains("= 'UPSERT_EDGE'"));
        assert!(first_attempt[1].starts_with("DELETE FROM system_history.lineage_history"));
        assert!(first_attempt[1].contains("= 'DELETE_OBJECT'"));
        for phase in first_attempt {
            assert!(phase.contains("batch_number >= 17"));
            assert!(phase.contains("batch_number < 23"));
        }
    }

    #[test]
    fn test_lineage_retention_defaults_to_permanent() {
        let config = |table_name: &str, retention| HistoryConfig {
            tables: vec![crate::HistoryTableConfig {
                table_name: table_name.to_string(),
                retention,
                invisible: false,
            }],
            ..Default::default()
        };

        let lineage = init_history_tables(&config(LINEAGE_HISTORY_TABLE, None)).unwrap();
        let default_delete = &lineage
            .iter()
            .find(|table| table.name == LINEAGE_HISTORY_TABLE)
            .unwrap()
            .delete;
        assert!(default_delete.is_none());

        let lineage = init_history_tables(&config(LINEAGE_HISTORY_TABLE, Some(24))).unwrap();
        let explicit_delete = &lineage
            .iter()
            .find(|table| table.name == LINEAGE_HISTORY_TABLE)
            .unwrap()
            .delete
            .as_deref()
            .unwrap();
        assert!(explicit_delete.contains("lineage_kind = 'DML'"));
        assert!(explicit_delete.contains("subtract_hours(NOW(), 24)"));

        let zero_retention = init_history_tables(&config(LINEAGE_HISTORY_TABLE, Some(0))).unwrap();
        assert!(
            zero_retention
                .iter()
                .find(|table| table.name == LINEAGE_HISTORY_TABLE)
                .unwrap()
                .delete
                .as_deref()
                .unwrap()
                .contains("subtract_hours(NOW(), 0)")
        );

        let query = init_history_tables(&config("query_history", None)).unwrap();
        assert!(
            query
                .iter()
                .find(|table| table.name == "query_history")
                .unwrap()
                .delete
                .as_deref()
                .unwrap()
                .contains("subtract_hours(NOW(), 168)")
        );

        let explicit_query = init_history_tables(&config("query_history", Some(48))).unwrap();
        assert!(
            explicit_query
                .iter()
                .find(|table| table.name == "query_history")
                .unwrap()
                .delete
                .as_deref()
                .unwrap()
                .contains("subtract_hours(NOW(), 48)")
        );

        let log_history = init_history_tables(&HistoryConfig::default()).unwrap();
        assert!(
            log_history
                .iter()
                .find(|table| table.name == "log_history")
                .unwrap()
                .delete
                .as_deref()
                .unwrap()
                .contains("subtract_hours(NOW(), 168)")
        );
    }
}
