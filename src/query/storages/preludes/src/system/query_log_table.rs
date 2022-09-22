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
use common_datavalues::prelude::*;
use common_exception::Result;
use serde::Serialize;
use serde::Serializer;
use serde_repr::Serialize_repr;

use crate::system::log_queue::SystemLogElement;
use crate::system::SystemLogQueue;
use crate::system::SystemLogTable;

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
            DataField::new("log_type", i8::to_data_type()),
            DataField::new("handler_type", Vu8::to_data_type()),
            // User.
            DataField::new("tenant_id", Vu8::to_data_type()),
            DataField::new("cluster_id", Vu8::to_data_type()),
            DataField::new("sql_user", Vu8::to_data_type()),
            DataField::new("sql_user_quota", Vu8::to_data_type()),
            DataField::new("sql_user_privileges", Vu8::to_data_type()),
            // Query.
            DataField::new("query_id", Vu8::to_data_type()),
            DataField::new("query_kind", Vu8::to_data_type()),
            DataField::new("query_text", Vu8::to_data_type()),
            DataField::new("event_date", DateType::new_impl()),
            DataField::new("event_time", TimestampType::new_impl(3)),
            // Schema.
            DataField::new("current_database", Vu8::to_data_type()),
            DataField::new("databases", Vu8::to_data_type()),
            DataField::new("tables", Vu8::to_data_type()),
            DataField::new("columns", Vu8::to_data_type()),
            DataField::new("projections", Vu8::to_data_type()),
            // Stats.
            DataField::new("written_rows", u64::to_data_type()),
            DataField::new("written_bytes", u64::to_data_type()),
            DataField::new("written_io_bytes", u64::to_data_type()),
            DataField::new("written_io_bytes_cost_ms", u64::to_data_type()),
            DataField::new("scan_rows", u64::to_data_type()),
            DataField::new("scan_bytes", u64::to_data_type()),
            DataField::new("scan_io_bytes", u64::to_data_type()),
            DataField::new("scan_io_bytes_cost_ms", u64::to_data_type()),
            DataField::new("scan_partitions", u64::to_data_type()),
            DataField::new("total_partitions", u64::to_data_type()),
            DataField::new("result_rows", u64::to_data_type()),
            DataField::new("result_bytes", u64::to_data_type()),
            DataField::new("cpu_usage", u32::to_data_type()),
            DataField::new("memory_usage", u64::to_data_type()),
            // Client.
            DataField::new("client_info", Vu8::to_data_type()),
            DataField::new("client_address", Vu8::to_data_type()),
            // Exception.
            DataField::new("exception_code", i32::to_data_type()),
            DataField::new("exception_text", Vu8::to_data_type()),
            DataField::new("stack_trace", Vu8::to_data_type()),
            // Server.
            DataField::new("server_version", Vu8::to_data_type()),
            // Session settings
            DataField::new("session_settings", Vu8::to_data_type()),
            // Extra.
            DataField::new("extra", Vu8::to_data_type()),
        ])
    }

    fn fill_to_data_block(&self, columns: &mut Vec<Box<dyn MutableColumn>>) -> Result<()> {
        let mut columns = columns.iter_mut();
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::Int64(self.log_type as i64))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.handler_type.as_bytes().to_vec()))?;
        // User.
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.tenant_id.as_bytes().to_vec()))?;

        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.cluster_id.as_bytes().to_vec()))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.sql_user.as_bytes().to_vec()))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.sql_user_quota.as_bytes().to_vec()))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(
                self.sql_user_privileges.as_bytes().to_vec(),
            ))?;
        // Query.
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.query_id.as_bytes().to_vec()))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.query_kind.as_bytes().to_vec()))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.query_text.as_bytes().to_vec()))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::Int64(self.event_date as i64))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::Int64(self.event_time))?;
        // Schema.
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.current_database.as_bytes().to_vec()))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.databases.as_bytes().to_vec()))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.tables.as_bytes().to_vec()))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.columns.as_bytes().to_vec()))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.projections.as_bytes().to_vec()))?;
        // Stats.
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::UInt64(self.written_rows))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::UInt64(self.written_bytes))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::UInt64(self.written_io_bytes))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::UInt64(self.written_io_bytes_cost_ms))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::UInt64(self.scan_rows))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::UInt64(self.scan_bytes))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::UInt64(self.scan_io_bytes))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::UInt64(self.scan_io_bytes_cost_ms))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::UInt64(self.scan_partitions))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::UInt64(self.total_partitions))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::UInt64(self.result_rows))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::UInt64(self.result_bytes))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::UInt64(self.cpu_usage as u64))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::UInt64(self.memory_usage))?;
        // Client.
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.client_info.as_bytes().to_vec()))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.client_address.as_bytes().to_vec()))?;
        // Exception.
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::Int64(self.exception_code as i64))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.exception_text.as_bytes().to_vec()))?;
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.stack_trace.as_bytes().to_vec()))?;
        // Server.
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.server_version.as_bytes().to_vec()))?;
        // Session settings
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.session_settings.as_bytes().to_vec()))?;
        // Extra.
        columns
            .next()
            .unwrap()
            .append_data_value(DataValue::String(self.extra.as_bytes().to_vec()))?;

        Ok(())
    }
}

pub type QueryLogQueue = SystemLogQueue<QueryLogElement>;
pub type QueryLogTable = SystemLogTable<QueryLogElement>;
