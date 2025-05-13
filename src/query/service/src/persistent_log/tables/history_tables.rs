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
use std::sync::Arc;

use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;

use crate::persistent_log::tables::log_history::log_history;

pub struct HistoryTables {
    pub tables: BTreeMap<String, Arc<HistoryTable>>,
}

impl HistoryTables {
    fn get_table(table_name: &str) -> Result<Arc<HistoryTable>> {
        match table_name {
            "log_history" => Ok(log_history()),
            _ => Err(ErrorCode::InvalidConfig("Unknown log history table name")),
        }
    }

    pub fn init(cfg: &InnerConfig) -> Result<HistoryTables> {
        let mut tables = BTreeMap::new();
        tables.insert("log_history".to_string(), log_history());
        for table in cfg.log.history.tables.iter() {
            tables.insert(
                table.table_name.clone(),
                HistoryTables::get_table(&table.table_name)?,
            );
        }
        Ok(HistoryTables { tables })
    }
}

pub struct HistoryTable {
    pub name: String,
    pub schema: TableSchemaRef,
    pub cluster_by: Vec<String>,
    pub transform_sql: String,
}

impl HistoryTable {
    pub fn create_sql(&self) -> String {
        let mut fields = vec![];
        for field in self.schema.fields().iter() {
            let field_name = field.name();
            let field_type = field.data_type().sql_name();
            fields.push(format!("{} {}", field_name, field_type))
        }
        let fields = fields.join(", ");
        let cluster_by = self.cluster_by.join(", ");
        format!(
            "CREATE TABLE IF NOT EXISTS system_history.{} ({}) CLUSTER BY ({})",
            self.name, fields, cluster_by
        )
    }
}
