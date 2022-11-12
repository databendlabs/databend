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

use chrono::NaiveDateTime;
use common_exception::Result;
use common_expression::ColumnBuilder;
use common_expression::DataField;
use common_expression::DataSchemaRefExt;
use common_expression::NumberDataType;
use common_expression::NumberScalar;
use common_expression::Scalar;
use common_expression::SchemaDataType;
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
    Aborted = 4,
}

fn date_str<S>(dt: &i32, s: S) -> Result<S::Ok, S::Error>
where S: Serializer {
    let t = NaiveDateTime::from_timestamp(i64::from(*dt) * 24 * 3600, 0);
    s.serialize_str(t.format("%Y-%m-%d").to_string().as_str())
}

fn datetime_str<S>(dt: &i64, s: S) -> Result<S::Ok, S::Error>
where S: Serializer {
    let t = NaiveDateTime::from_timestamp(
        dt / 1_000_000,
        TryFrom::try_from((dt % 1_000_000) * 1000).unwrap_or(0),
        // u32::try_from((dt % 1_000_000) * 1000).unwrap_or(0),
    );
    s.serialize_str(t.format("%Y-%m-%d %H:%M:%S%.6f").to_string().as_str())
}

#[derive(Clone, Serialize)]
pub struct QueryLogElement {
    // Type.
    pub log_type: LogType,
    pub handler_type: String,

    // User.
    pub tenant_id: String,
    pub cluster_id: String,
    pub sql_user: String,

    #[serde(skip_serializing)]
    pub sql_user_quota: String,
    #[serde(skip_serializing)]
    pub sql_user_privileges: String,

    // Query.
    pub query_id: String,
    pub query_kind: String,
    pub query_text: String,

    #[serde(serialize_with = "date_str")]
    pub event_date: i32,
    #[serde(serialize_with = "datetime_str")]
    pub event_time: i64,
    #[serde(serialize_with = "datetime_str")]
    pub query_start_time: i64,
    pub query_duration_ms: i64,

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

    // Client.
    pub client_info: String,
    pub client_address: String,

    // Exception.
    pub exception_code: i32,
    pub exception_text: String,
    pub stack_trace: String,

    // Server.
    pub server_version: String,

    // Session settings
    #[serde(skip_serializing)]
    pub session_settings: String,

    // Extra.
    pub extra: String,
}

impl SystemLogElement for QueryLogElement {
    const TABLE_NAME: &'static str = "query_log";

    fn schema() -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            // Type.
            DataField::new("log_type", SchemaDataType::Number(NumberDataType::Int8)),
            DataField::new("handler_type", SchemaDataType::String),
            // User.
            DataField::new("tenant_id", SchemaDataType::String),
            DataField::new("cluster_id", SchemaDataType::String),
            DataField::new("sql_user", SchemaDataType::String),
            DataField::new("sql_user_quota", SchemaDataType::String),
            DataField::new("sql_user_privileges", SchemaDataType::String),
            // Query.
            DataField::new("query_id", SchemaDataType::String),
            DataField::new("query_kind", SchemaDataType::String),
            DataField::new("query_text", SchemaDataType::String),
            DataField::new("event_date", SchemaDataType::Date),
            DataField::new("event_time", SchemaDataType::Timestamp),
            DataField::new("query_start_time", SchemaDataType::Timestamp),
            DataField::new(
                "query_duration_ms",
                SchemaDataType::Number(NumberDataType::Int64),
            ),
            // Schema.
            DataField::new("current_database", SchemaDataType::String),
            DataField::new("databases", SchemaDataType::String),
            DataField::new("tables", SchemaDataType::String),
            DataField::new("columns", SchemaDataType::String),
            DataField::new("projections", SchemaDataType::String),
            // Stats.
            DataField::new(
                "written_rows",
                SchemaDataType::Number(NumberDataType::UInt64),
            ),
            DataField::new(
                "written_bytes",
                SchemaDataType::Number(NumberDataType::UInt64),
            ),
            DataField::new(
                "written_io_bytes",
                SchemaDataType::Number(NumberDataType::UInt64),
            ),
            DataField::new(
                "written_io_bytes_cost_ms",
                SchemaDataType::Number(NumberDataType::UInt64),
            ),
            DataField::new("scan_rows", SchemaDataType::Number(NumberDataType::UInt64)),
            DataField::new("scan_bytes", SchemaDataType::Number(NumberDataType::UInt64)),
            DataField::new(
                "scan_io_bytes",
                SchemaDataType::Number(NumberDataType::UInt64),
            ),
            DataField::new(
                "scan_io_bytes_cost_ms",
                SchemaDataType::Number(NumberDataType::UInt64),
            ),
            DataField::new(
                "scan_partitions",
                SchemaDataType::Number(NumberDataType::UInt64),
            ),
            DataField::new(
                "total_partitions",
                SchemaDataType::Number(NumberDataType::UInt64),
            ),
            DataField::new(
                "result_rows",
                SchemaDataType::Number(NumberDataType::UInt64),
            ),
            DataField::new(
                "result_bytes",
                SchemaDataType::Number(NumberDataType::UInt64),
            ),
            DataField::new("cpu_usage", SchemaDataType::Number(NumberDataType::UInt32)),
            DataField::new(
                "memory_usage",
                SchemaDataType::Number(NumberDataType::UInt64),
            ),
            // Client.
            DataField::new("client_info", SchemaDataType::String),
            DataField::new("client_address", SchemaDataType::String),
            // Exception.
            DataField::new(
                "exception_code",
                SchemaDataType::Number(NumberDataType::Int32),
            ),
            DataField::new("exception_text", SchemaDataType::String),
            DataField::new("stack_trace", SchemaDataType::String),
            // Server.
            DataField::new("server_version", SchemaDataType::String),
            // Session settings
            DataField::new("session_settings", SchemaDataType::String),
            // Extra.
            DataField::new("extra", SchemaDataType::String),
        ])
    }

    fn fill_to_data_block(&self, columns: &mut Vec<ColumnBuilder>) -> Result<()> {
        let mut columns = columns.iter_mut();
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::Int64(self.log_type as i64)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.handler_type.as_bytes().to_vec()).as_ref());
        // User.
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.tenant_id.as_bytes().to_vec()).as_ref());

        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.cluster_id.as_bytes().to_vec()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.sql_user.as_bytes().to_vec()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.sql_user_quota.as_bytes().to_vec()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.sql_user_privileges.as_bytes().to_vec()).as_ref());
        // Query.
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.query_id.as_bytes().to_vec()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.query_kind.as_bytes().to_vec()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.query_text.as_bytes().to_vec()).as_ref());
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
        // Schema.
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.current_database.as_bytes().to_vec()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.databases.as_bytes().to_vec()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.tables.as_bytes().to_vec()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.columns.as_bytes().to_vec()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.projections.as_bytes().to_vec()).as_ref());
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
            .push(Scalar::Number(NumberScalar::UInt64(self.cpu_usage as u64)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::UInt64(self.memory_usage)).as_ref());
        // Client.
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.client_info.as_bytes().to_vec()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.client_address.as_bytes().to_vec()).as_ref());
        // Exception.
        columns
            .next()
            .unwrap()
            .push(Scalar::Number(NumberScalar::Int64(self.exception_code as i64)).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.exception_text.as_bytes().to_vec()).as_ref());
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.stack_trace.as_bytes().to_vec()).as_ref());
        // Server.
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.server_version.as_bytes().to_vec()).as_ref());
        // Session settings
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.session_settings.as_bytes().to_vec()).as_ref());
        // Extra.
        columns
            .next()
            .unwrap()
            .push(Scalar::String(self.extra.as_bytes().to_vec()).as_ref());
        Ok(())
    }
}

pub type QueryLogQueue = SystemLogQueue<QueryLogElement>;
pub type QueryLogTable = SystemLogTable<QueryLogElement>;
