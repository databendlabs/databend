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

use std::fmt;

use databend_common_meta_api::kv_pb_api::decode_pb;
use databend_common_meta_app::data_mask::DatamaskMeta;
use databend_common_meta_app::data_mask::MaskPolicyTableId;
use databend_common_meta_app::data_mask::MaskpolicyTableIdList;
use databend_common_meta_app::principal::NetworkPolicy;
use databend_common_meta_app::principal::OwnershipInfo;
use databend_common_meta_app::principal::PasswordPolicy;
use databend_common_meta_app::principal::ProcedureIdentity;
use databend_common_meta_app::principal::ProcedureMeta;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::StageFile;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::Task;
use databend_common_meta_app::principal::TaskMessage;
use databend_common_meta_app::principal::UserDefinedConnection;
use databend_common_meta_app::principal::UserDefinedFileFormat;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::client_session::ClientSession;
use databend_common_meta_app::principal::user_token::QueryTokenInfo;
use databend_common_meta_app::row_access_policy::RowAccessPolicyMeta;
use databend_common_meta_app::row_access_policy::RowAccessPolicyTableId;
use databend_common_meta_app::schema::CatalogMeta;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::DbIdList;
use databend_common_meta_app::schema::DictionaryMeta;
use databend_common_meta_app::schema::EmptyProto;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::LeastVisibleTime;
use databend_common_meta_app::schema::LockMeta;
use databend_common_meta_app::schema::MarkedDeletedIndexMeta;
use databend_common_meta_app::schema::ObjectTagIdRefValue;
use databend_common_meta_app::schema::SequenceMeta;
use databend_common_meta_app::schema::TableCopiedFileInfo;
use databend_common_meta_app::schema::TableIdList;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TagMeta;
use databend_common_meta_app::schema::VacuumWatermark;
use databend_common_meta_app::tenant::TenantQuota;
use databend_common_proto_conv::FromToProto;
use databend_meta_types::Cmd;
use databend_meta_types::Operation;
use databend_meta_types::txn_op::Request;

fn decode_as<T>(bytes: &[u8]) -> Result<String, String>
where
    T: FromToProto + fmt::Debug,
    T::PB: prost::Message + Default,
{
    let val: T = decode_pb(bytes).map_err(|e| e.to_string())?;
    Ok(format!("{:?}", val))
}

macro_rules! try_decode {
    ($key:expr, $bytes:expr, [ $( $prefix:expr => $type:ty ),* $(,)? ]) => {{
        $(
            if $key.starts_with($prefix) {
                return match decode_as::<$type>($bytes) {
                    Ok(s) => s,
                    Err(e) => format!("decode-error({}): {}", $prefix, e),
                };
            }
        )*
        format!("unknown-prefix({} bytes)", $bytes.len())
    }};
}

/// Decode protobuf-encoded bytes to a human-readable string,
/// using the key prefix to determine the protobuf message type.
pub fn decode_pb_value(key: &str, bytes: &[u8]) -> String {
    // Match with trailing "/" to avoid prefix shadowing
    // (e.g., "__fd_sequence/" vs "__fd_sequence_storage/").
    try_decode!(key, bytes, [
        // schema - database
        "__fd_database_by_id/"      => DatabaseMeta,
        "__fd_db_id_list/"          => DbIdList,

        // schema - table
        "__fd_table_by_id/"         => TableMeta,
        "__fd_table_id_to_name/"    => DBIdTableName,
        "__fd_table_id_list/"       => TableIdList,
        "__fd_table_copied_files/"  => TableCopiedFileInfo,

        // schema - catalog
        "__fd_catalog_by_id/"       => CatalogMeta,

        // schema - index
        "__fd_index_by_id/"         => IndexMeta,
        "__fd_marked_deleted_table_index/" => MarkedDeletedIndexMeta,
        "__fd_marked_deleted_index/" => MarkedDeletedIndexMeta,

        // schema - table auxiliary
        "__fd_table_lvt/"           => LeastVisibleTime,
        "__fd_table_lock/"          => LockMeta,
        "__fd_vacuum_watermark_ts/" => VacuumWatermark,

        // schema - sequence
        "__fd_sequence/"            => SequenceMeta,

        // schema - dictionary
        "__fd_dictionary_by_id/"    => DictionaryMeta,

        // schema - tag
        "__fd_tag_by_id/"           => TagMeta,
        "__fd_object_tag_ref/"      => ObjectTagIdRefValue,
        "__fd_tag_object_ref/"      => EmptyProto,

        // principal - identity / access
        "__fd_users/"               => UserInfo,
        "__fd_roles/"               => RoleInfo,
        "__fd_stages/"              => StageInfo,
        "__fd_stage_files/"         => StageFile,
        "__fd_token/"               => QueryTokenInfo,
        "__fd_network_policies/"    => NetworkPolicy,
        "__fd_password_policies/"   => PasswordPolicy,
        "__fd_quotas/"              => TenantQuota,
        "__fd_object_owners/"       => OwnershipInfo,

        // principal - UDF / file format / connection
        "__fd_udfs/"                => UserDefinedFunction,
        "__fd_file_formats/"        => UserDefinedFileFormat,
        "__fd_connection/"          => UserDefinedConnection,

        // principal - procedure
        "__fd_procedure_by_id/"     => ProcedureMeta,
        "__fd_procedure_id_to_name/" => ProcedureIdentity,

        // principal - task / session
        "__fd_tasks/"               => Task,
        "__fd_task_messages/"       => TaskMessage,
        "__fd_session/"             => ClientSession,

        // data mask
        "__fd_datamask_by_id/"      => DatamaskMeta,
        "__fd_datamask_id_list/"    => MaskpolicyTableIdList,
        "__fd_mask_policy_apply_table_id/" => MaskPolicyTableId,

        // row access policy
        "__fd_row_access_policy_by_id/" => RowAccessPolicyMeta,
        "__fd_row_access_policy_apply_table_id/" => RowAccessPolicyTableId,
    ])
}

/// Format optional expire_at and ttl_ms fields as a suffix string.
fn format_put_meta(expire_at: Option<u64>, ttl_ms: Option<u64>) -> String {
    let mut parts = vec![];
    if let Some(ea) = expire_at {
        parts.push(format!("expire_at={}", ea));
    }
    if let Some(ttl) = ttl_ms {
        parts.push(format!("ttl_ms={}", ttl));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" ({})", parts.join(", "))
    }
}

/// Decode a single TxnOp, returning a formatted line if it contains a value.
fn decode_txn_op(prefix: &str, idx: usize, op: &databend_meta_types::TxnOp) -> Option<String> {
    match &op.request {
        Some(Request::Put(put)) => {
            let decoded = decode_pb_value(&put.key, &put.value);
            let meta = format_put_meta(put.expire_at, put.ttl_ms);
            Some(format!(
                "    {prefix}[{idx}].put {key}{meta}:\n      {decoded}",
                key = put.key,
            ))
        }
        Some(Request::PutSequential(ps)) => {
            let decoded = decode_pb_value(&ps.prefix, &ps.value);
            let meta = format_put_meta(ps.expires_at_ms, ps.ttl_ms);
            Some(format!(
                "    {prefix}[{idx}].put_sequential {key}{meta}:\n      {decoded}",
                key = ps.prefix,
            ))
        }
        _ => None,
    }
}

fn format_raw_bytes(bytes: &[u8]) -> String {
    format!(
        "[{}]",
        bytes
            .iter()
            .map(|b| b.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// Extract raw protobuf byte arrays from a Cmd, returning formatted lines.
pub fn raw_cmd_values(cmd: &Cmd) -> Vec<String> {
    match cmd {
        Cmd::UpsertKV(upsert) => match &upsert.value {
            Operation::Update(bytes) => {
                vec![format!(
                    "    raw({}): {}",
                    upsert.key,
                    format_raw_bytes(bytes)
                )]
            }
            _ => vec![],
        },
        Cmd::Transaction(txn) => {
            let mut lines = vec![];

            for (branch_idx, branch) in txn.operations.iter().enumerate() {
                for (op_idx, op) in branch.operations.iter().enumerate() {
                    let prefix = format!("txn.operations[{}].ops", branch_idx);
                    if let Some(line) = raw_txn_op(&prefix, op_idx, op) {
                        lines.push(line);
                    }
                }
            }

            for (i, op) in txn.if_then.iter().enumerate() {
                if let Some(line) = raw_txn_op("txn.if_then", i, op) {
                    lines.push(line);
                }
            }

            for (i, op) in txn.else_then.iter().enumerate() {
                if let Some(line) = raw_txn_op("txn.else_then", i, op) {
                    lines.push(line);
                }
            }

            lines
        }
        _ => vec![],
    }
}

/// Extract raw bytes from a single TxnOp, returning a formatted line if it contains a value.
fn raw_txn_op(prefix: &str, idx: usize, op: &databend_meta_types::TxnOp) -> Option<String> {
    match &op.request {
        Some(Request::Put(put)) => Some(format!(
            "    {prefix}[{idx}].put {key} raw:\n      {}",
            format_raw_bytes(&put.value),
            key = put.key,
        )),
        Some(Request::PutSequential(ps)) => Some(format!(
            "    {prefix}[{idx}].put_sequential {key} raw:\n      {}",
            format_raw_bytes(&ps.value),
            key = ps.prefix,
        )),
        _ => None,
    }
}

/// Decode all protobuf values found in a Cmd, returning formatted lines.
pub fn decode_cmd_values(cmd: &Cmd) -> Vec<String> {
    match cmd {
        Cmd::UpsertKV(upsert) => match &upsert.value {
            Operation::Update(bytes) => {
                let decoded = decode_pb_value(&upsert.key, bytes);
                vec![format!("    value({}): {}", upsert.key, decoded)]
            }
            _ => vec![],
        },
        Cmd::Transaction(txn) => {
            let mut lines = vec![];

            for (branch_idx, branch) in txn.operations.iter().enumerate() {
                for (op_idx, op) in branch.operations.iter().enumerate() {
                    let prefix = format!("txn.operations[{}].ops", branch_idx);
                    if let Some(line) = decode_txn_op(&prefix, op_idx, op) {
                        lines.push(line);
                    }
                }
            }

            for (i, op) in txn.if_then.iter().enumerate() {
                if let Some(line) = decode_txn_op("txn.if_then", i, op) {
                    lines.push(line);
                }
            }

            for (i, op) in txn.else_then.iter().enumerate() {
                if let Some(line) = decode_txn_op("txn.else_then", i, op) {
                    lines.push(line);
                }
            }

            lines
        }
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_api::kv_pb_api;
    use databend_common_proto_conv::FromToProto;
    use databend_meta_types::TxnOp;
    use databend_meta_types::TxnRequest;

    use super::*;

    fn normalize_timestamps(s: &str) -> String {
        let re = regex::Regex::new(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z").unwrap();
        re.replace_all(s, "<TS>").to_string()
    }

    fn encode_pb<T: FromToProto>(val: &T) -> Vec<u8> {
        kv_pb_api::encode_pb(val).unwrap()
    }

    // -- decode_pb_value tests --

    #[test]
    fn test_decode_pb_value_unknown_prefix() {
        assert_eq!(
            decode_pb_value("__unknown/key", &[1, 2, 3]),
            "unknown-prefix(3 bytes)"
        );
    }

    #[test]
    fn test_decode_pb_value_corrupted_bytes() {
        assert_eq!(
            decode_pb_value("__fd_database_by_id/123", &[0xff, 0xff]),
            "decode-error(__fd_database_by_id/): PbDecodeError: failed to decode Protobuf message: invalid varint"
        );
    }

    #[test]
    fn test_decode_pb_value_database_meta() {
        let buf = encode_pb(&DatabaseMeta::default());
        let got = normalize_timestamps(&decode_pb_value("__fd_database_by_id/1", &buf));
        assert_eq!(
            got,
            r#"DatabaseMeta { engine: "", engine_options: {}, options: {}, created_on: <TS>, updated_on: <TS>, comment: "", drop_on: None, gc_in_progress: false }"#
        );
    }

    #[test]
    fn test_decode_pb_value_table_meta() {
        let buf = encode_pb(&TableMeta::default());
        let got = normalize_timestamps(&decode_pb_value("__fd_table_by_id/1", &buf));
        assert_eq!(
            got,
            r#"TableMeta { schema: TableSchema { fields: [], metadata: {}, next_column_id: 0 }, engine: "FUSE", engine_options: {}, storage_params: None, part_prefix: "", options: {}, cluster_key: None, cluster_key_v2: None, cluster_key_seq: 0, created_on: <TS>, updated_on: <TS>, comment: "", field_comments: [], virtual_schema: None, drop_on: None, statistics: TableStatistics { number_of_rows: 0, data_bytes: 0, compressed_data_bytes: 0, index_data_bytes: 0, bloom_index_size: None, ngram_index_size: None, inverted_index_size: None, vector_index_size: None, virtual_column_size: None, number_of_segments: None, number_of_blocks: None }, shared_by: {}, column_mask_policy: None, column_mask_policy_columns_ids: {}, row_access_policy: None, row_access_policy_columns_ids: None, indexes: {}, constraints: {}, refs: {} }"#
        );
    }

    #[test]
    fn test_decode_pb_value_db_id_list() {
        let buf = encode_pb(&DbIdList::default());
        assert_eq!(
            decode_pb_value("__fd_db_id_list/1", &buf),
            "DbIdList { id_list: [] }"
        );
    }

    // -- decode_cmd_values: UpsertKV --

    #[test]
    fn test_cmd_upsert_kv_update() {
        let buf = encode_pb(&DatabaseMeta::default());
        let cmd = Cmd::UpsertKV(databend_meta_types::UpsertKV::update(
            "__fd_database_by_id/123",
            &buf,
        ));
        let lines = decode_cmd_values(&cmd);
        assert_eq!(lines.len(), 1);

        let got = normalize_timestamps(&lines[0]);
        assert_eq!(
            got,
            r#"    value(__fd_database_by_id/123): DatabaseMeta { engine: "", engine_options: {}, options: {}, created_on: <TS>, updated_on: <TS>, comment: "", drop_on: None, gc_in_progress: false }"#
        );
    }

    #[test]
    fn test_cmd_upsert_kv_delete() {
        let cmd = Cmd::UpsertKV(databend_meta_types::UpsertKV::delete(
            "__fd_database_by_id/1",
        ));
        assert_eq!(decode_cmd_values(&cmd), Vec::<String>::new());
    }

    #[test]
    fn test_cmd_upsert_kv_unknown_prefix() {
        let cmd = Cmd::UpsertKV(databend_meta_types::UpsertKV::update("__unknown/key", &[
            1, 2, 3,
        ]));
        let lines = decode_cmd_values(&cmd);
        assert_eq!(lines.len(), 1);
        assert_eq!(
            lines[0],
            "    value(__unknown/key): unknown-prefix(3 bytes)"
        );
    }

    #[test]
    fn test_cmd_upsert_kv_decode_error() {
        let cmd = Cmd::UpsertKV(databend_meta_types::UpsertKV::update(
            "__fd_database_by_id/bad",
            &[0xff, 0xff],
        ));
        let lines = decode_cmd_values(&cmd);
        assert_eq!(lines.len(), 1);
        assert_eq!(
            lines[0],
            "    value(__fd_database_by_id/bad): decode-error(__fd_database_by_id/): PbDecodeError: failed to decode Protobuf message: invalid varint"
        );
    }

    // -- decode_cmd_values: Transaction --

    #[test]
    fn test_cmd_txn_single_if_then_put() {
        let buf = encode_pb(&DatabaseMeta::default());
        let txn = TxnRequest::new(vec![], vec![TxnOp::put("__fd_database_by_id/1", buf)]);
        let lines = decode_cmd_values(&Cmd::Transaction(txn));
        assert_eq!(lines.len(), 1);

        let got = normalize_timestamps(&lines[0]);
        assert_eq!(
            got,
            concat!(
                "    txn.if_then[0].put __fd_database_by_id/1:\n",
                r#"      DatabaseMeta { engine: "", engine_options: {}, options: {}, created_on: <TS>, updated_on: <TS>, comment: "", drop_on: None, gc_in_progress: false }"#,
            )
        );
    }

    #[test]
    fn test_cmd_txn_single_else_then_put() {
        let buf = encode_pb(&DatabaseMeta::default());
        let txn = TxnRequest::new(vec![], vec![])
            .with_else(vec![TxnOp::put("__fd_database_by_id/2", buf)]);
        let lines = decode_cmd_values(&Cmd::Transaction(txn));
        assert_eq!(lines.len(), 1);

        let got = normalize_timestamps(&lines[0]);
        assert_eq!(
            got,
            concat!(
                "    txn.else_then[0].put __fd_database_by_id/2:\n",
                r#"      DatabaseMeta { engine: "", engine_options: {}, options: {}, created_on: <TS>, updated_on: <TS>, comment: "", drop_on: None, gc_in_progress: false }"#,
            )
        );
    }

    #[test]
    fn test_cmd_txn_multiple_if_then_puts() {
        let db_buf = encode_pb(&DatabaseMeta::default());
        let tbl_buf = encode_pb(&TableMeta::default());
        let txn = TxnRequest::new(vec![], vec![
            TxnOp::put("__fd_database_by_id/10", db_buf),
            TxnOp::put("__fd_table_by_id/20", tbl_buf),
        ]);
        let lines = decode_cmd_values(&Cmd::Transaction(txn));
        assert_eq!(lines.len(), 2);

        let got0 = normalize_timestamps(&lines[0]);
        assert_eq!(
            got0,
            concat!(
                "    txn.if_then[0].put __fd_database_by_id/10:\n",
                r#"      DatabaseMeta { engine: "", engine_options: {}, options: {}, created_on: <TS>, updated_on: <TS>, comment: "", drop_on: None, gc_in_progress: false }"#,
            )
        );
        let got1 = normalize_timestamps(&lines[1]);
        assert_eq!(
            got1,
            concat!(
                "    txn.if_then[1].put __fd_table_by_id/20:\n",
                r#"      TableMeta { schema: TableSchema { fields: [], metadata: {}, next_column_id: 0 }, engine: "FUSE", engine_options: {}, storage_params: None, part_prefix: "", options: {}, cluster_key: None, cluster_key_v2: None, cluster_key_seq: 0, created_on: <TS>, updated_on: <TS>, comment: "", field_comments: [], virtual_schema: None, drop_on: None, statistics: TableStatistics { number_of_rows: 0, data_bytes: 0, compressed_data_bytes: 0, index_data_bytes: 0, bloom_index_size: None, ngram_index_size: None, inverted_index_size: None, vector_index_size: None, virtual_column_size: None, number_of_segments: None, number_of_blocks: None }, shared_by: {}, column_mask_policy: None, column_mask_policy_columns_ids: {}, row_access_policy: None, row_access_policy_columns_ids: None, indexes: {}, constraints: {}, refs: {} }"#,
            )
        );
    }

    #[test]
    fn test_cmd_txn_both_if_then_and_else_then() {
        let db_buf = encode_pb(&DatabaseMeta::default());
        let list_buf = encode_pb(&DbIdList::default());

        let txn = TxnRequest::new(vec![], vec![TxnOp::put("__fd_database_by_id/1", db_buf)])
            .with_else(vec![TxnOp::put("__fd_db_id_list/2", list_buf)]);

        let lines = decode_cmd_values(&Cmd::Transaction(txn));
        assert_eq!(lines.len(), 2);

        let got0 = normalize_timestamps(&lines[0]);
        assert_eq!(
            got0,
            concat!(
                "    txn.if_then[0].put __fd_database_by_id/1:\n",
                r#"      DatabaseMeta { engine: "", engine_options: {}, options: {}, created_on: <TS>, updated_on: <TS>, comment: "", drop_on: None, gc_in_progress: false }"#,
            )
        );
        assert_eq!(
            lines[1],
            "    txn.else_then[0].put __fd_db_id_list/2:\n      DbIdList { id_list: [] }"
        );
    }

    #[test]
    fn test_cmd_txn_non_put_ops_are_skipped() {
        // get and delete ops should produce no output
        let txn = TxnRequest::new(vec![], vec![
            TxnOp::get("__fd_database_by_id/1"),
            TxnOp::delete("__fd_database_by_id/2"),
        ]);
        assert_eq!(
            decode_cmd_values(&Cmd::Transaction(txn)),
            Vec::<String>::new()
        );
    }

    #[test]
    fn test_cmd_txn_mixed_put_and_non_put() {
        let db_buf = encode_pb(&DatabaseMeta::default());
        let txn = TxnRequest::new(vec![], vec![
            TxnOp::get("__fd_database_by_id/1"),
            TxnOp::put("__fd_database_by_id/2", db_buf),
            TxnOp::delete("__fd_database_by_id/3"),
        ]);
        let lines = decode_cmd_values(&Cmd::Transaction(txn));
        // Only the put at index 1 produces output
        assert_eq!(lines.len(), 1);

        let got = normalize_timestamps(&lines[0]);
        assert_eq!(
            got,
            concat!(
                "    txn.if_then[1].put __fd_database_by_id/2:\n",
                r#"      DatabaseMeta { engine: "", engine_options: {}, options: {}, created_on: <TS>, updated_on: <TS>, comment: "", drop_on: None, gc_in_progress: false }"#,
            )
        );
    }

    #[test]
    fn test_cmd_txn_empty() {
        let txn = TxnRequest::new(vec![], vec![]);
        assert_eq!(
            decode_cmd_values(&Cmd::Transaction(txn)),
            Vec::<String>::new()
        );
    }

    #[test]
    fn test_cmd_txn_put_with_unknown_prefix() {
        let txn = TxnRequest::new(vec![], vec![TxnOp::put("__unknown/key", vec![1, 2])]);
        let lines = decode_cmd_values(&Cmd::Transaction(txn));
        assert_eq!(lines.len(), 1);
        assert_eq!(
            lines[0],
            "    txn.if_then[0].put __unknown/key:\n      unknown-prefix(2 bytes)"
        );
    }

    // -- decode_cmd_values: Transaction with expire_at/ttl_ms --

    #[test]
    fn test_cmd_txn_put_with_ttl() {
        use std::time::Duration;

        let buf = encode_pb(&DbIdList::default());
        let txn = TxnRequest::new(vec![], vec![TxnOp::put_with_ttl(
            "__fd_db_id_list/1",
            buf,
            Some(Duration::from_secs(60)),
        )]);
        let lines = decode_cmd_values(&Cmd::Transaction(txn));
        assert_eq!(lines.len(), 1);
        assert_eq!(
            lines[0],
            concat!(
                "    txn.if_then[0].put __fd_db_id_list/1 (ttl_ms=60000):\n",
                "      DbIdList { id_list: [] }",
            )
        );
    }

    #[test]
    fn test_cmd_txn_put_with_expire_at() {
        use databend_meta_types::protobuf::TxnPutRequest;

        let buf = encode_pb(&DbIdList::default());
        let op = databend_meta_types::TxnOp {
            request: Some(Request::Put(TxnPutRequest {
                key: "__fd_db_id_list/1".to_string(),
                value: buf,
                prev_value: true,
                expire_at: Some(1700000000),
                ttl_ms: None,
            })),
        };
        let txn = TxnRequest::new(vec![], vec![op]);
        let lines = decode_cmd_values(&Cmd::Transaction(txn));
        assert_eq!(lines.len(), 1);
        assert_eq!(
            lines[0],
            concat!(
                "    txn.if_then[0].put __fd_db_id_list/1 (expire_at=1700000000):\n",
                "      DbIdList { id_list: [] }",
            )
        );
    }

    #[test]
    fn test_cmd_txn_put_with_both_expire_at_and_ttl() {
        use databend_meta_types::protobuf::TxnPutRequest;

        let buf = encode_pb(&DbIdList::default());
        let op = databend_meta_types::TxnOp {
            request: Some(Request::Put(TxnPutRequest {
                key: "__fd_db_id_list/1".to_string(),
                value: buf,
                prev_value: true,
                expire_at: Some(1700000000),
                ttl_ms: Some(30000),
            })),
        };
        let txn = TxnRequest::new(vec![], vec![op]);
        let lines = decode_cmd_values(&Cmd::Transaction(txn));
        assert_eq!(lines.len(), 1);
        assert_eq!(
            lines[0],
            concat!(
                "    txn.if_then[0].put __fd_db_id_list/1 (expire_at=1700000000, ttl_ms=30000):\n",
                "      DbIdList { id_list: [] }",
            )
        );
    }

    // -- decode_cmd_values: Transaction with PutSequential --

    #[test]
    fn test_cmd_txn_put_sequential() {
        let buf = encode_pb(&DbIdList::default());
        let txn = TxnRequest::new(vec![], vec![TxnOp::put_sequential(
            "__fd_db_id_list/",
            "__seq/db",
            buf,
        )]);
        let lines = decode_cmd_values(&Cmd::Transaction(txn));
        assert_eq!(lines.len(), 1);
        assert_eq!(
            lines[0],
            concat!(
                "    txn.if_then[0].put_sequential __fd_db_id_list/:\n",
                "      DbIdList { id_list: [] }",
            )
        );
    }

    #[test]
    fn test_cmd_txn_put_sequential_with_ttl() {
        use databend_meta_types::protobuf::PutSequential;

        let buf = encode_pb(&DbIdList::default());
        let op = databend_meta_types::TxnOp {
            request: Some(Request::PutSequential(PutSequential {
                prefix: "__fd_db_id_list/".to_string(),
                sequence_key: "__seq/db".to_string(),
                value: buf,
                expires_at_ms: Some(9999),
                ttl_ms: Some(5000),
            })),
        };
        let txn = TxnRequest::new(vec![], vec![op]);
        let lines = decode_cmd_values(&Cmd::Transaction(txn));
        assert_eq!(lines.len(), 1);
        assert_eq!(
            lines[0],
            concat!(
                "    txn.if_then[0].put_sequential __fd_db_id_list/ (expire_at=9999, ttl_ms=5000):\n",
                "      DbIdList { id_list: [] }",
            )
        );
    }

    // -- decode_cmd_values: Transaction with operations (ConditionalOperation) --

    #[test]
    fn test_cmd_txn_operations_branch_with_put() {
        use databend_meta_types::protobuf::BooleanExpression;
        use databend_meta_types::protobuf::TxnCondition;

        let buf = encode_pb(&DbIdList::default());
        let txn = TxnRequest::new(vec![], vec![]).push_branch(
            Some(BooleanExpression::from_conditions_and([
                TxnCondition::eq_seq("k1", 1),
            ])),
            vec![TxnOp::put("__fd_db_id_list/5", buf)],
        );
        let lines = decode_cmd_values(&Cmd::Transaction(txn));
        assert_eq!(lines.len(), 1);
        assert_eq!(
            lines[0],
            concat!(
                "    txn.operations[0].ops[0].put __fd_db_id_list/5:\n",
                "      DbIdList { id_list: [] }",
            )
        );
    }

    #[test]
    fn test_cmd_txn_operations_multiple_branches() {
        use databend_meta_types::protobuf::BooleanExpression;
        use databend_meta_types::protobuf::TxnCondition;

        let db_buf = encode_pb(&DatabaseMeta::default());
        let list_buf = encode_pb(&DbIdList::default());

        let txn = TxnRequest::new(vec![], vec![])
            .push_branch(
                Some(BooleanExpression::from_conditions_and([
                    TxnCondition::eq_seq("k1", 1),
                ])),
                vec![TxnOp::put("__fd_db_id_list/1", list_buf)],
            )
            .push_branch(
                Some(BooleanExpression::from_conditions_and([
                    TxnCondition::eq_seq("k2", 2),
                ])),
                vec![
                    TxnOp::get("__fd_database_by_id/9"),
                    TxnOp::put("__fd_database_by_id/10", db_buf),
                ],
            );

        let lines = decode_cmd_values(&Cmd::Transaction(txn));
        assert_eq!(lines.len(), 2);

        assert_eq!(
            lines[0],
            concat!(
                "    txn.operations[0].ops[0].put __fd_db_id_list/1:\n",
                "      DbIdList { id_list: [] }",
            )
        );
        let got1 = normalize_timestamps(&lines[1]);
        assert_eq!(
            got1,
            concat!(
                "    txn.operations[1].ops[1].put __fd_database_by_id/10:\n",
                r#"      DatabaseMeta { engine: "", engine_options: {}, options: {}, created_on: <TS>, updated_on: <TS>, comment: "", drop_on: None, gc_in_progress: false }"#,
            )
        );
    }

    #[test]
    fn test_cmd_txn_operations_and_if_then_and_else_then() {
        use databend_meta_types::protobuf::BooleanExpression;
        use databend_meta_types::protobuf::TxnCondition;

        let buf1 = encode_pb(&DbIdList::default());
        let buf2 = encode_pb(&DbIdList::default());
        let buf3 = encode_pb(&DbIdList::default());

        let txn = TxnRequest::new(vec![TxnCondition::eq_seq("cond_key", 5)], vec![TxnOp::put(
            "__fd_db_id_list/if",
            buf2,
        )])
        .with_else(vec![TxnOp::put("__fd_db_id_list/else", buf3)])
        .push_branch(
            Some(BooleanExpression::from_conditions_and([
                TxnCondition::eq_seq("k1", 1),
            ])),
            vec![TxnOp::put("__fd_db_id_list/ops", buf1)],
        );

        let lines = decode_cmd_values(&Cmd::Transaction(txn));
        // operations first, then if_then, then else_then
        assert_eq!(lines.len(), 3);
        assert_eq!(
            lines[0],
            "    txn.operations[0].ops[0].put __fd_db_id_list/ops:\n      DbIdList { id_list: [] }"
        );
        assert_eq!(
            lines[1],
            "    txn.if_then[0].put __fd_db_id_list/if:\n      DbIdList { id_list: [] }"
        );
        assert_eq!(
            lines[2],
            "    txn.else_then[0].put __fd_db_id_list/else:\n      DbIdList { id_list: [] }"
        );
    }

    // -- decode_cmd_values: Transaction non-put ops that should be skipped --

    #[test]
    fn test_cmd_txn_delete_by_prefix_skipped() {
        use databend_meta_types::protobuf::TxnDeleteByPrefixRequest;

        let op = databend_meta_types::TxnOp {
            request: Some(Request::DeleteByPrefix(TxnDeleteByPrefixRequest {
                prefix: "__fd_database_by_id/".to_string(),
            })),
        };
        let txn = TxnRequest::new(vec![], vec![op]);
        assert_eq!(
            decode_cmd_values(&Cmd::Transaction(txn)),
            Vec::<String>::new()
        );
    }

    #[test]
    fn test_cmd_txn_fetch_increase_u64_skipped() {
        let op = TxnOp::fetch_add_u64("__fd_sequence/auto_inc", 1);
        let txn = TxnRequest::new(vec![], vec![op]);
        assert_eq!(
            decode_cmd_values(&Cmd::Transaction(txn)),
            Vec::<String>::new()
        );
    }

    // -- decode_cmd_values: other Cmd variants --

    #[test]
    fn test_cmd_other_variants_produce_no_output() {
        let cmd = Cmd::AddNode {
            node_id: 1,
            node: Default::default(),
            overriding: false,
        };
        assert_eq!(decode_cmd_values(&cmd), Vec::<String>::new());
    }

    // -- raw_cmd_values tests --

    #[test]
    fn test_raw_cmd_upsert_kv_update() {
        let bytes = vec![10, 0, 26, 12];
        let cmd = Cmd::UpsertKV(databend_meta_types::UpsertKV::update(
            "__fd_database_by_id/123",
            &bytes,
        ));
        let lines = raw_cmd_values(&cmd);
        assert_eq!(lines.len(), 1);
        assert_eq!(
            lines[0],
            "    raw(__fd_database_by_id/123): [10, 0, 26, 12]"
        );
    }

    #[test]
    fn test_raw_cmd_upsert_kv_delete() {
        let cmd = Cmd::UpsertKV(databend_meta_types::UpsertKV::delete(
            "__fd_database_by_id/1",
        ));
        assert_eq!(raw_cmd_values(&cmd), Vec::<String>::new());
    }

    #[test]
    fn test_raw_cmd_txn_put() {
        let bytes = vec![1, 2, 3, 4, 5];
        let txn = TxnRequest::new(vec![], vec![TxnOp::put("__fd_database_by_id/1", bytes)]);
        let lines = raw_cmd_values(&Cmd::Transaction(txn));
        assert_eq!(lines.len(), 1);
        assert_eq!(
            lines[0],
            "    txn.if_then[0].put __fd_database_by_id/1 raw:\n      [1, 2, 3, 4, 5]"
        );
    }

    #[test]
    fn test_raw_cmd_txn_else_then_put() {
        let bytes = vec![7, 8];
        let txn =
            TxnRequest::new(vec![], vec![]).with_else(vec![TxnOp::put("__fd_db_id_list/2", bytes)]);
        let lines = raw_cmd_values(&Cmd::Transaction(txn));
        assert_eq!(lines.len(), 1);
        assert_eq!(
            lines[0],
            "    txn.else_then[0].put __fd_db_id_list/2 raw:\n      [7, 8]"
        );
    }

    #[test]
    fn test_raw_cmd_txn_non_put_ops_skipped() {
        let txn = TxnRequest::new(vec![], vec![
            TxnOp::get("__fd_database_by_id/1"),
            TxnOp::delete("__fd_database_by_id/2"),
        ]);
        assert_eq!(raw_cmd_values(&Cmd::Transaction(txn)), Vec::<String>::new());
    }

    #[test]
    fn test_raw_cmd_other_variants() {
        let cmd = Cmd::AddNode {
            node_id: 1,
            node: Default::default(),
            overriding: false,
        };
        assert_eq!(raw_cmd_values(&cmd), Vec::<String>::new());
    }
}
