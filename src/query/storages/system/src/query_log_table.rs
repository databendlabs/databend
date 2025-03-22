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

use std::collections::HashMap;

use chrono::DateTime;
use databend_common_exception::Result;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use serde::Serialize;
use serde::Serializer;
use serde_repr::Serialize_repr;

use crate::SystemLogElement;
use crate::SystemLogQueue;
use crate::SystemLogTable;

#[derive(Clone, Copy, Serialize_repr)]
#[repr(u8)]
pub enum LogType {
    Start = 1,
    Finish = 2,
    Error = 3,

    /// canceled by another thread:
    /// 1. `kill <query_id>` statement
    /// 2. client_driver/ctx.cancel() -> /kill
    Aborted = 4,

    /// close early because client does not need more data:
    /// 1. explicit or implicit result_set.close() -> /final
    Closed = 5,
}

impl LogType {
    pub fn as_string(&self) -> String {
        match self {
            LogType::Start => "Start".to_string(),
            LogType::Finish => "Finish".to_string(),
            LogType::Error => "Error".to_string(),
            LogType::Aborted => "Aborted".to_string(),
            LogType::Closed => "Closed".to_string(),
        }
    }
}

impl std::fmt::Debug for LogType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.as_string())
    }
}

fn date_str<S>(dt: &i32, s: S) -> std::result::Result<S::Ok, S::Error>
where S: Serializer {
    let t = DateTime::from_timestamp(i64::from(*dt) * 24 * 3600, 0)
        .unwrap()
        .naive_utc();
    s.serialize_str(t.format("%Y-%m-%d").to_string().as_str())
}

fn datetime_str<S>(dt: &i64, s: S) -> std::result::Result<S::Ok, S::Error>
where S: Serializer {
    let t = DateTime::from_timestamp(
        dt / 1_000_000,
        TryFrom::try_from((dt % 1_000_000) * 1000).unwrap_or(0),
        // u32::try_from((dt % 1_000_000) * 1000).unwrap_or(0),
    )
    .unwrap()
    .naive_utc();
    s.serialize_str(t.format("%Y-%m-%d %H:%M:%S%.6f").to_string().as_str())
}

#[derive(Clone, Serialize)]
pub struct QueryLogElement {
    // Type.
    pub log_type: LogType,
    pub log_type_name: String,
    pub handler_type: String,

    // User.
    pub tenant_id: String,
    pub cluster_id: String,
    pub node_id: String,
    pub sql_user: String,

    #[serde(skip_serializing)]
    pub sql_user_quota: String,
    #[serde(skip_serializing)]
    pub sql_user_privileges: String,

    // Query.
    pub query_id: String,
    pub query_kind: String,
    pub query_text: String,
    pub query_hash: String,
    pub query_parameterized_hash: String,

    #[serde(serialize_with = "date_str")]
    pub event_date: i32,
    #[serde(serialize_with = "datetime_str")]
    pub event_time: i64,
    #[serde(serialize_with = "datetime_str")]
    pub query_start_time: i64,
    pub query_duration_ms: i64,
    pub query_queued_duration_ms: i64,

    // Schema.
    pub current_database: String,
    pub databases: String,
    pub tables: String,
    pub columns: String,
    pub projections: String,

    // Stats.
    pub written_rows: u64,
    pub written_bytes: u64,
    pub written_io_bytes: u64,
    pub written_io_bytes_cost_ms: u64,
    pub scan_rows: u64,
    pub scan_bytes: u64,
    pub scan_io_bytes: u64,
    pub scan_io_bytes_cost_ms: u64,
    pub scan_partitions: u64,
    pub total_partitions: u64,
    pub result_rows: u64,
    pub result_bytes: u64,
    pub cpu_usage: u32,
    pub memory_usage: u64,
    pub join_spilled_bytes: u64,
    pub join_spilled_rows: u64,
    pub agg_spilled_bytes: u64,
    pub agg_spilled_rows: u64,
    pub group_by_spilled_bytes: u64,
    pub group_by_spilled_rows: u64,
    pub bytes_from_remote_disk: u64,
    pub bytes_from_local_disk: u64,
    pub bytes_from_memory: u64,

    // Client.
    pub client_info: String,
    pub client_address: String,
    pub user_agent: String,

    // Exception.
    pub exception_code: i32,
    pub exception_text: String,
    pub stack_trace: String,

    // Server.
    pub server_version: String,

    // Session
    pub query_tag: String,
    #[serde(skip_serializing)]
    pub session_settings: String,

    // Extra.
    pub extra: String,

    pub has_profiles: bool,

    // Transaction
    pub txn_state: String,
    pub txn_id: String,
    pub peak_memory_usage: HashMap<String, usize>,
}

impl SystemLogElement for QueryLogElement {
    const TABLE_NAME: &'static str = "query_log";

    fn schema() -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            // Type.
            TableField::new("log_type", TableDataType::Number(NumberDataType::Int8)),
            TableField::new("log_type_name", TableDataType::String),
            TableField::new("handler_type", TableDataType::String),
            // User.
            TableField::new("tenant_id", TableDataType::String),
            TableField::new("cluster_id", TableDataType::String),
            TableField::new("node_id", TableDataType::String),
            TableField::new("sql_user", TableDataType::String),
            TableField::new("sql_user_quota", TableDataType::String),
            TableField::new("sql_user_privileges", TableDataType::String),
            // Query.
            TableField::new("query_id", TableDataType::String),
            TableField::new("query_kind", TableDataType::String),
            TableField::new("query_text", TableDataType::String),
            TableField::new("query_hash", TableDataType::String),
            TableField::new("query_parameterized_hash", TableDataType::String),
            TableField::new("event_date", TableDataType::Date),
            TableField::new("event_time", TableDataType::Timestamp),
            TableField::new("query_start_time", TableDataType::Timestamp),
            TableField::new(
                "query_duration_ms",
                TableDataType::Number(NumberDataType::Int64),
            ),
            TableField::new(
                "query_queued_duration_ms",
                TableDataType::Number(NumberDataType::Int64),
            ),
            // Schema.
            TableField::new("current_database", TableDataType::String),
            TableField::new("databases", TableDataType::String),
            TableField::new("tables", TableDataType::String),
            TableField::new("columns", TableDataType::String),
            TableField::new("projections", TableDataType::String),
            // Stats.
            TableField::new(
                "written_rows",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "written_bytes",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "join_spilled_rows",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "join_spilled_bytes",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "agg_spilled_rows",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "agg_spilled_bytes",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "group_by_spilled_rows",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "group_by_spilled_bytes",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "written_io_bytes",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "written_io_bytes_cost_ms",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("scan_rows", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("scan_bytes", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "scan_io_bytes",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "scan_io_bytes_cost_ms",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "scan_partitions",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "total_partitions",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("result_rows", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "result_bytes",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("cpu_usage", TableDataType::Number(NumberDataType::UInt32)),
            TableField::new(
                "memory_usage",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "bytes_from_remote_disk",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "bytes_from_local_disk",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "bytes_from_memory",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            // Client.
            TableField::new("client_info", TableDataType::String),
            TableField::new("client_address", TableDataType::String),
            TableField::new("user_agent", TableDataType::String),
            // Exception.
            TableField::new(
                "exception_code",
                TableDataType::Number(NumberDataType::Int32),
            ),
            TableField::new("exception_text", TableDataType::String),
            TableField::new("stack_trace", TableDataType::String),
            // Server.
            TableField::new("server_version", TableDataType::String),
            // Session
            TableField::new("query_tag", TableDataType::String),
            TableField::new("session_settings", TableDataType::String),
            // Extra.
            TableField::new("extra", TableDataType::String),
            TableField::new("has_profile", TableDataType::Boolean),
            TableField::new("peek_memory_usage", TableDataType::Variant),
        ])
    }

    fn fill_to_data_block(&self, columns: &mut Vec<ColumnBuilder>) -> Result<()> {
        let mut columns = columns.iter_mut();
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::Int8(self.log_type as i8)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.log_type_name.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.handler_type.clone()).as_ref());
        // User.
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.tenant_id.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.cluster_id.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.node_id.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.sql_user.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.sql_user_quota.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.sql_user_privileges.clone()).as_ref());
        // Query.
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.query_id.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.query_kind.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.query_text.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.query_hash.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.query_parameterized_hash.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Date(self.event_date).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Timestamp(self.event_time).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Timestamp(self.query_start_time).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::Int64(self.query_duration_ms)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::Int64(self.query_queued_duration_ms)).as_ref());
        // Schema.
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.current_database.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.databases.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.tables.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.columns.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.projections.clone()).as_ref());
        // Stats.
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.written_rows)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.written_bytes)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.join_spilled_rows)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.join_spilled_bytes)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.agg_spilled_rows)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.agg_spilled_bytes)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.group_by_spilled_rows)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.group_by_spilled_bytes)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.written_io_bytes)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.written_io_bytes_cost_ms)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.scan_rows)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.scan_bytes)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.scan_io_bytes)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.scan_io_bytes_cost_ms)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.scan_partitions)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.total_partitions)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.result_rows)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.result_bytes)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt32(self.cpu_usage)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.memory_usage)).as_ref());

        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.bytes_from_remote_disk)).as_ref());

        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.bytes_from_local_disk)).as_ref());

        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.bytes_from_memory)).as_ref());

        // Client.
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.client_info.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.client_address.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.user_agent.clone()).as_ref());
        // Exception.
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::Int32(self.exception_code)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.exception_text.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.stack_trace.clone()).as_ref());
        // Server.
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.server_version.clone()).as_ref());
        // Session
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.query_tag.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.session_settings.clone()).as_ref());
        // Extra.
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.extra.clone()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Boolean(self.has_profiles).as_ref());
        columns.next().unwrap().push(
            Scalar::Variant(
                jsonb::Value::from(jsonb::Object::from_iter(
                    self.peak_memory_usage
                        .iter()
                        .map(|(k, v)| (k.clone(), jsonb::Value::from(*v))),
                ))
                .to_vec(),
            )
            .as_ref(),
        );
        Ok(())
    }
}

pub type QueryLogQueue = SystemLogQueue<QueryLogElement>;
pub type QueryLogTable = SystemLogTable<QueryLogElement>;
