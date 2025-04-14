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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_tracing::PERSISTENT_LOG_SCHEMA_VERSION;
use itertools::Itertools;

pub trait PersistentLogTable: Send + Sync + 'static {
    fn table_name(&self) -> String;
    fn enable_version_suffix(&self);
    fn schema(&self) -> TableSchemaRef;
    fn cluster_by(&self) -> Vec<String>;
    fn create_table_sql(&self) -> String {
        let table_name = self.table_name();
        let schema = self.schema();
        let fields = schema
            .fields()
            .iter()
            .map(|f| format!("{} {}", f.name(), f.data_type().sql_name()))
            .collect::<Vec<_>>()
            .join(", ");
        let cluster_by = self.cluster_by().join(", ");
        format!(
            "CREATE TABLE IF NOT EXISTS persistent_system.{} ({}) CLUSTER BY ({})",
            table_name, fields, cluster_by
        )
    }

    fn copy_into_sql(&self, stage_name: &str, files: &[String]) -> String;
    fn schema_equal(&self, other: TableSchemaRef) -> bool {
        self.schema().fields().len() == other.fields().len()
            && self
                .schema()
                .fields()
                .iter()
                .zip(other.fields().iter())
                .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type())
    }
    fn clean_sql(&self, retention: usize) -> String;
}

pub struct QueryLogTable {
    need_version_suffix: AtomicBool,
}

impl QueryLogTable {
    pub fn new() -> Self {
        Self {
            need_version_suffix: AtomicBool::new(false),
        }
    }
}
impl Default for QueryLogTable {
    fn default() -> Self {
        Self::new()
    }
}

impl PersistentLogTable for QueryLogTable {
    fn table_name(&self) -> String {
        if self.need_version_suffix.load(Ordering::Relaxed) {
            format!("query_log_v{}", PERSISTENT_LOG_SCHEMA_VERSION)
        } else {
            "query_log".to_string()
        }
    }

    fn enable_version_suffix(&self) {
        self.need_version_suffix.store(true, Ordering::Relaxed);
    }

    fn schema(&self) -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new(
                "timestamp",
                TableDataType::Nullable(Box::new(TableDataType::Timestamp)),
            ),
            TableField::new(
                "path",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "target",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "log_level",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "cluster_id",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "node_id",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "warehouse_id",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "query_id",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "message",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "fields",
                TableDataType::Nullable(Box::new(TableDataType::Variant)),
            ),
        ])
    }

    fn cluster_by(&self) -> Vec<String> {
        vec!["timestamp".to_string(), "query_id".to_string()]
    }

    fn copy_into_sql(&self, stage_name: &str, files: &[String]) -> String {
        let file_names = files.iter().map(|f| format!("'{}'", f)).join(",");
        format!(
            "COPY INTO persistent_system.{}
             FROM @{} FILES=({}) file_format = (TYPE = PARQUET)
             PURGE = TRUE",
            self.table_name(),
            stage_name,
            file_names
        )
    }

    fn clean_sql(&self, retention: usize) -> String {
        let table_name = self.table_name();
        format!(
            "DELETE FROM persistent_system.{} WHERE timestamp < subtract_hours(NOW(), {})",
            table_name, retention
        )
    }
}

pub struct QueryDetailsTable {
    need_version_suffix: AtomicBool,
}

impl QueryDetailsTable {
    pub fn new() -> Self {
        Self {
            need_version_suffix: AtomicBool::new(false),
        }
    }
}
impl Default for QueryDetailsTable {
    fn default() -> Self {
        Self::new()
    }
}

impl PersistentLogTable for QueryDetailsTable {
    fn table_name(&self) -> String {
        if self.need_version_suffix.load(Ordering::Relaxed) {
            format!("query_details_v{}", PERSISTENT_LOG_SCHEMA_VERSION)
        } else {
            "query_details".to_string()
        }
    }

    fn enable_version_suffix(&self) {
        self.need_version_suffix.store(true, Ordering::Relaxed);
    }

    fn schema(&self) -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            // Type.
            TableField::new(
                "log_type",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int8))),
            ),
            TableField::new(
                "log_type_name",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "handler_type",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            // User.
            TableField::new(
                "tenant_id",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "cluster_id",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "node_id",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "sql_user",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "sql_user_quota",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "sql_user_privileges",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            // Query.
            TableField::new(
                "query_id",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "query_kind",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "query_text",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "query_hash",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "query_parameterized_hash",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "event_date",
                TableDataType::Nullable(Box::new(TableDataType::Date)),
            ),
            TableField::new(
                "event_time",
                TableDataType::Nullable(Box::new(TableDataType::Timestamp)),
            ),
            TableField::new(
                "query_start_time",
                TableDataType::Nullable(Box::new(TableDataType::Timestamp)),
            ),
            TableField::new(
                "query_duration_ms",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int64))),
            ),
            TableField::new(
                "query_queued_duration_ms",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int64))),
            ),
            // Schema.
            TableField::new(
                "current_database",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            // Stats.
            TableField::new(
                "written_rows",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "written_bytes",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "join_spilled_rows",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "join_spilled_bytes",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "agg_spilled_rows",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "agg_spilled_bytes",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "group_by_spilled_rows",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "group_by_spilled_bytes",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "written_io_bytes",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "written_io_bytes_cost_ms",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "scan_rows",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "scan_bytes",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "scan_io_bytes",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "scan_io_bytes_cost_ms",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "scan_partitions",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "total_partitions",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "result_rows",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "result_bytes",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "bytes_from_remote_disk",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "bytes_from_local_disk",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "bytes_from_memory",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            // Client.
            TableField::new(
                "client_address",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "user_agent",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            // Exception.
            TableField::new(
                "exception_code",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int32))),
            ),
            TableField::new(
                "exception_text",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            // Server.
            TableField::new(
                "server_version",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            // Session
            TableField::new(
                "query_tag",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "has_profile",
                TableDataType::Nullable(Box::new(TableDataType::Boolean)),
            ),
            TableField::new(
                "peek_memory_usage",
                TableDataType::Nullable(Box::new(TableDataType::Variant)),
            ),
        ])
    }

    fn cluster_by(&self) -> Vec<String> {
        vec!["event_time".to_string(), "query_id".to_string()]
    }

    fn copy_into_sql(&self, stage_name: &str, files: &[String]) -> String {
        let fields = self
            .schema()
            .fields()
            .iter()
            .map(|f| format!("m['{}']", f.name()))
            .collect::<Vec<_>>()
            .join(", ");
        let file_names = files.iter().map(|f| format!("'{}'", f)).join(",");
        format!(
            "INSERT INTO persistent_system.{} FROM (SELECT {} FROM (SELECT parse_json(message) as m FROM @{} (FILES=>({})) WHERE target='databend::log::query'))",
            self.table_name(),
            fields,
            stage_name,
            file_names
        )
    }

    fn clean_sql(&self, retention: usize) -> String {
        let table_name = self.table_name();
        format!(
            "DELETE FROM persistent_system.{} WHERE event_time < subtract_hours(NOW(), {})",
            table_name, retention
        )
    }
}

pub struct QueryProfileTable {
    need_version_suffix: AtomicBool,
}

impl QueryProfileTable {
    pub fn new() -> Self {
        Self {
            need_version_suffix: AtomicBool::new(false),
        }
    }
}
impl Default for QueryProfileTable {
    fn default() -> Self {
        Self::new()
    }
}

impl PersistentLogTable for QueryProfileTable {
    fn table_name(&self) -> String {
        if self.need_version_suffix.load(Ordering::Relaxed) {
            format!("query_profile_v{}", PERSISTENT_LOG_SCHEMA_VERSION)
        } else {
            "query_profile".to_string()
        }
    }

    fn enable_version_suffix(&self) {
        self.need_version_suffix.store(true, Ordering::Relaxed);
    }

    fn schema(&self) -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new(
                "timestamp",
                TableDataType::Nullable(Box::new(TableDataType::Timestamp)),
            ),
            TableField::new(
                "query_id",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new(
                "profiles",
                TableDataType::Nullable(Box::new(TableDataType::Variant)),
            ),
            TableField::new(
                "statistics_desc",
                TableDataType::Nullable(Box::new(TableDataType::Variant)),
            ),
        ])
    }

    fn cluster_by(&self) -> Vec<String> {
        vec!["timestamp".to_string(), "query_id".to_string()]
    }

    fn copy_into_sql(&self, stage_name: &str, files: &[String]) -> String {
        let fields = self
            .schema()
            .fields()
            .iter()
            .filter(|f| f.name() != "timestamp")
            .map(|f| format!("m['{}']", f.name()))
            .collect::<Vec<_>>()
            .join(", ");
        let file_names = files.iter().map(|f| format!("'{}'", f)).join(",");
        format!(
            "INSERT INTO persistent_system.{} FROM (SELECT timestamp, {} FROM (SELECT timestamp, parse_json(message) as m FROM @{} (FILES=>({})) WHERE target='databend::log::profile'))",
            self.table_name(),
            fields,
            stage_name,
            file_names
        )
    }

    fn clean_sql(&self, retention: usize) -> String {
        let table_name = self.table_name();
        format!(
            "DELETE FROM persistent_system.{} WHERE timestamp < subtract_hours(NOW(), {})",
            table_name, retention
        )
    }
}
