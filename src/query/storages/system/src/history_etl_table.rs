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

use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_users::UserApiProvider;
use databend_meta_client::kvapi::KvApiExt;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::types::SeqV;
use serde::Deserialize;
use serde::Serialize;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

/// The latest reported ETL error, retained across successful batches and overwritten on failure.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistoryEtlError {
    pub message: String,
    /// UTC microseconds since the Unix epoch, captured when the error is reported.
    pub time: i64,
}

impl HistoryEtlError {
    pub fn new(message: &str, time: i64) -> Self {
        let message = message.trim();
        let mut end = message.len().min(1024);
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        Self {
            message: message[..end].trim_end().to_owned(),
            time,
        }
    }
}

#[derive(Default)]
struct HistoryEtlRow {
    heartbeat_node_id: Option<String>,
    batch_number: Option<u64>,
    last_success_time: Option<i64>,
    last_error: Option<HistoryEtlError>,
}

// This is the existing heartbeat JSON format; the error record is deliberately separate.
#[derive(Deserialize)]
struct HeartbeatMessage {
    from_node_id: String,
}

fn collect_rows(
    prefix: &str,
    values: impl IntoIterator<Item = (String, SeqV<Vec<u8>>)>,
) -> Result<BTreeMap<String, HistoryEtlRow>> {
    let mut rows = BTreeMap::<String, HistoryEtlRow>::new();
    for (key, value) in values {
        let Some((table_name, field)) = key.strip_prefix(prefix).and_then(|s| s.split_once('/'))
        else {
            continue;
        };
        if table_name.is_empty() || !matches!(field, "heartbeat" | "batch_number" | "last_error") {
            continue;
        }
        let row = rows.entry(table_name.to_owned()).or_default();
        match field {
            "heartbeat" => {
                let heartbeat: HeartbeatMessage = serde_json::from_slice(&value.data)?;
                row.heartbeat_node_id = Some(heartbeat.from_node_id);
            }
            "batch_number" => {
                row.batch_number = Some(serde_json::from_slice(&value.data)?);
                // A successful no-op does not write the checkpoint. This is the meta proposal
                // time of the latest checkpoint write, not the time of the latest poll.
                row.last_success_time = value
                    .meta
                    .and_then(|meta| meta.proposed_at_ms)
                    .and_then(|ms| i64::try_from(ms).ok())
                    .and_then(|ms| ms.checked_mul(1000));
            }
            "last_error" => row.last_error = Some(serde_json::from_slice(&value.data)?),
            _ => unreachable!(),
        }
    }
    Ok(rows)
}

pub struct HistoryEtlTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for HistoryEtlTable {
    const NAME: &'static str = "system.history_etl";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let meta_client = UserApiProvider::instance().get_meta_store_client();
        let prefix = format!("{}/history_log_transform/", ctx.get_tenant().tenant_name());
        let values = meta_client
            .list_kv_collect(ListOptions::unlimited(&prefix))
            .await
            .map_err(|e| ErrorCode::MetaServiceError(e.to_string()))?;
        let rows = collect_rows(&prefix, values)?;
        let mut table_names = Vec::with_capacity(rows.len());
        let mut heartbeat_node_ids = Vec::with_capacity(rows.len());
        let mut batch_numbers = Vec::with_capacity(rows.len());
        let mut last_success_times = Vec::with_capacity(rows.len());
        let mut last_error_times = Vec::with_capacity(rows.len());
        let mut last_errors = Vec::with_capacity(rows.len());
        for (table_name, row) in rows {
            table_names.push(table_name);
            heartbeat_node_ids.push(row.heartbeat_node_id);
            batch_numbers.push(row.batch_number);
            last_success_times.push(row.last_success_time);
            last_error_times.push(row.last_error.as_ref().map(|e| e.time));
            last_errors.push(row.last_error.map(|e| e.message));
        }
        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(table_names),
            StringType::from_opt_data(heartbeat_node_ids),
            UInt64Type::from_opt_data(batch_numbers),
            TimestampType::from_opt_data(last_success_times),
            TimestampType::from_opt_data(last_error_times),
            StringType::from_opt_data(last_errors),
        ]))
    }
}

impl HistoryEtlTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("table_name", TableDataType::String),
            TableField::new("heartbeat_node_id", TableDataType::String.wrap_nullable()),
            TableField::new(
                "batch_number",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
            TableField::new(
                "last_success_time",
                TableDataType::Timestamp.wrap_nullable(),
            ),
            TableField::new("last_error_time", TableDataType::Timestamp.wrap_nullable()),
            TableField::new("last_error", TableDataType::String.wrap_nullable()),
        ]);
        let table_info = TableInfo {
            desc: "'system'.'history_etl'".to_owned(),
            name: "history_etl".to_owned(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemHistoryEtl".to_owned(),
                ..Default::default()
            },
            ..Default::default()
        };
        AsyncOneBlockSystemTable::create(Self { table_info })
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_client::types::KVMeta;

    use super::*;

    #[test]
    fn error_message_is_bounded_on_utf8_boundary() {
        let message = format!("  {}错误  ", "x".repeat(1022));
        let error = HistoryEtlError::new(&message, 123);
        assert_eq!(error.message, "x".repeat(1022));
        assert_eq!(HistoryEtlError::new(" \n 故障 \n", 123).message, "故障");
        assert_eq!(
            HistoryEtlError::new(&"错".repeat(400), 123).message.len(),
            1023
        );
        let encoded = serde_json::to_vec(&error).unwrap();
        assert_eq!(
            serde_json::from_slice::<HistoryEtlError>(&encoded).unwrap(),
            error
        );
    }

    #[test]
    fn rows_include_inactive_tables_and_legacy_checkpoints() -> Result<()> {
        let prefix = "tenant/history_log_transform/";
        let checkpoint = |meta| SeqV {
            seq: 1,
            meta,
            data: b"42".to_vec(),
        };
        let rows = collect_rows(prefix, vec![
            (
                format!("{prefix}query_history/batch_number"),
                checkpoint(Some(KVMeta {
                    expire_at: None,
                    proposed_at_ms: Some(1788834023067),
                })),
            ),
            (
                format!("{prefix}log_history/batch_number"),
                checkpoint(None),
            ),
            (
                format!("{prefix}log_history/heartbeat"),
                SeqV::new(2, br#"{"from_node_id":"node-in-another-cluster"}"#.to_vec()),
            ),
            (
                format!("{prefix}query_history/last_error"),
                SeqV::new(
                    3,
                    br#"{"message":"previous failure","time":1788834000000000}"#.to_vec(),
                ),
            ),
            (
                format!("{prefix}login_history/last_error"),
                SeqV::new(
                    4,
                    br#"{"message":"first batch failed","time":1788834000000000}"#.to_vec(),
                ),
            ),
            (
                "other-tenant/history_log_transform/query_history/batch_number".into(),
                checkpoint(None),
            ),
            (
                format!("{prefix}ignored/future_field"),
                SeqV::new(5, b"{}".to_vec()),
            ),
        ])?;
        assert_eq!(rows.len(), 3);
        let query = &rows["query_history"];
        assert!(query.heartbeat_node_id.is_none());
        assert_eq!(query.batch_number, Some(42));
        assert_eq!(query.last_success_time, Some(1788834023067000));
        assert_eq!(
            query.last_error.as_ref().unwrap().message,
            "previous failure"
        );
        let log = &rows["log_history"];
        assert_eq!(
            log.heartbeat_node_id.as_deref(),
            Some("node-in-another-cluster")
        );
        assert!(log.last_success_time.is_none());
        assert!(log.last_error.is_none());
        let login = &rows["login_history"];
        assert!(login.batch_number.is_none());
        assert!(login.last_success_time.is_none());
        assert!(collect_rows(prefix, []).unwrap().is_empty());
        Ok(())
    }
}
