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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use serde::Deserialize;
use serde::Serialize;

mod branches;
mod files;
mod manifests;
mod options;
mod partitions;
mod physical_files_size;
mod referenced_files_size;
mod schemas;
mod snapshots;
mod table;
mod table_indexes;
mod tags;

pub use table::PaimonSystemTable;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum PaimonSystemTableKind {
    Branches,
    Files,
    Manifests,
    Options,
    Partitions,
    PhysicalFilesSize,
    ReferencedFilesSize,
    Schemas,
    Snapshots,
    TableIndexes,
    Tags,
}

impl PaimonSystemTableKind {
    pub const ALL: [Self; 11] = [
        Self::Branches,
        Self::Files,
        Self::Manifests,
        Self::Options,
        Self::Partitions,
        Self::PhysicalFilesSize,
        Self::ReferencedFilesSize,
        Self::Schemas,
        Self::Snapshots,
        Self::TableIndexes,
        Self::Tags,
    ];

    pub fn parse(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "branches" => Some(Self::Branches),
            "files" => Some(Self::Files),
            "manifests" => Some(Self::Manifests),
            "options" => Some(Self::Options),
            "partitions" => Some(Self::Partitions),
            "physical_files_size" => Some(Self::PhysicalFilesSize),
            "referenced_files_size" => Some(Self::ReferencedFilesSize),
            "schemas" => Some(Self::Schemas),
            "snapshots" => Some(Self::Snapshots),
            "table_indexes" => Some(Self::TableIndexes),
            "tags" => Some(Self::Tags),
            _ => None,
        }
    }

    pub fn schema(self) -> TableSchemaRef {
        use NumberDataType::*;
        use TableDataType::*;

        let string = || String;
        let int32 = || Number(Int32);
        let int64 = || Number(Int64);
        let nullable = |ty| Nullable(Box::new(ty));
        let fields = match self {
            Self::Options => vec![f("key", string()), f("value", string())],
            Self::Snapshots => vec![
                f("snapshot_id", int64()),
                f("schema_id", int64()),
                f("commit_user", string()),
                f("commit_identifier", int64()),
                f("commit_kind", string()),
                f("commit_time", Timestamp),
                f("base_manifest_list", string()),
                f("delta_manifest_list", string()),
                f("changelog_manifest_list", nullable(string())),
                f("total_record_count", nullable(int64())),
                f("delta_record_count", nullable(int64())),
                f("changelog_record_count", nullable(int64())),
                f("watermark", nullable(int64())),
                f("next_row_id", nullable(int64())),
            ],
            Self::Schemas => vec![
                f("schema_id", int64()),
                f("fields", string()),
                f("partition_keys", string()),
                f("primary_keys", string()),
                f("options", string()),
                f("comment", nullable(string())),
                f("update_time", Timestamp),
            ],
            Self::Branches => vec![f("branch_name", string()), f("create_time", Timestamp)],
            Self::Tags => vec![
                f("tag_name", string()),
                f("snapshot_id", int64()),
                f("schema_id", int64()),
                f("commit_time", Timestamp),
                f("record_count", nullable(int64())),
                f("create_time", nullable(Timestamp)),
                f("time_retained", nullable(string())),
            ],
            Self::Files => vec![
                f("partition", nullable(string())),
                f("bucket", int32()),
                f("file_path", string()),
                f("file_format", string()),
                f("schema_id", int64()),
                f("level", int32()),
                f("record_count", int64()),
                f("file_size_in_bytes", int64()),
                f("min_key", nullable(string())),
                f("max_key", nullable(string())),
                f("null_value_counts", string()),
                f("min_value_stats", string()),
                f("max_value_stats", string()),
                f("min_sequence_number", nullable(int64())),
                f("max_sequence_number", nullable(int64())),
                f("creation_time", nullable(Timestamp)),
                f("delete_row_count", nullable(int64())),
                f("file_source", nullable(string())),
                f("first_row_id", nullable(int64())),
                f("write_cols", nullable(Array(Box::new(nullable(string()))))),
            ],
            Self::Manifests => vec![
                f("file_name", string()),
                f("file_size", int64()),
                f("num_added_files", int64()),
                f("num_deleted_files", int64()),
                f("schema_id", int64()),
                f("min_partition_stats", nullable(string())),
                f("max_partition_stats", nullable(string())),
                f("min_row_id", nullable(int64())),
                f("max_row_id", nullable(int64())),
            ],
            Self::Partitions => vec![
                f("partition", nullable(string())),
                f("record_count", int64()),
                f("file_size_in_bytes", int64()),
                f("file_count", int64()),
                f("last_update_time", nullable(Timestamp)),
                f("created_at", nullable(Timestamp)),
                f("created_by", nullable(string())),
                f("updated_by", nullable(string())),
                f("options", nullable(string())),
                f("total_buckets", int32()),
                f("done", Boolean),
            ],
            Self::PhysicalFilesSize => size_fields(false),
            Self::ReferencedFilesSize => size_fields(true),
            Self::TableIndexes => vec![
                f("partition", nullable(string())),
                f("bucket", int32()),
                f("index_type", string()),
                f("file_name", string()),
                f("file_size", int64()),
                f("row_count", int64()),
                f(
                    "dv_ranges",
                    nullable(Array(Box::new(Tuple {
                        fields_name: vec![
                            "file_name".into(),
                            "offset".into(),
                            "length".into(),
                            "cardinality".into(),
                        ],
                        fields_type: vec![string(), int32(), int32(), nullable(int64())],
                    }))),
                ),
                f("row_range_start", nullable(int64())),
                f("row_range_end", nullable(int64())),
                f("index_field_id", nullable(int32())),
                f("index_field_name", nullable(string())),
            ],
        };
        TableSchemaRefExt::create(fields)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParsedName {
    Base(String),
    System {
        base: String,
        branch: Option<String>,
        kind: PaimonSystemTableKind,
    },
    UnknownSystem {
        base: String,
        suffix: String,
    },
}

pub fn parse_system_name(name: &str) -> ParsedName {
    let parts = name.split('$').collect::<Vec<_>>();
    match parts.as_slice() {
        [base, suffix] if !base.is_empty() => match PaimonSystemTableKind::parse(suffix) {
            Some(kind) => ParsedName::System {
                base: (*base).to_string(),
                branch: None,
                kind,
            },
            None => ParsedName::UnknownSystem {
                base: (*base).to_string(),
                suffix: (*suffix).to_string(),
            },
        },
        [base, branch, suffix]
            if !base.is_empty()
                && branch.starts_with("branch_")
                && branch.len() > "branch_".len() =>
        {
            match PaimonSystemTableKind::parse(suffix) {
                Some(kind) => ParsedName::System {
                    base: (*base).to_string(),
                    branch: Some(branch["branch_".len()..].to_string()),
                    kind,
                },
                None => ParsedName::UnknownSystem {
                    base: (*base).to_string(),
                    suffix: (*suffix).to_string(),
                },
            }
        }
        _ => ParsedName::Base(name.to_string()),
    }
}

fn f(name: &str, ty: TableDataType) -> TableField {
    TableField::new(name, ty)
}

fn size_fields(with_source: bool) -> Vec<TableField> {
    let mut fields = Vec::new();
    if with_source {
        fields.push(f("source", TableDataType::String));
    }
    for name in [
        "manifest_file_count",
        "manifest_file_size",
        "data_file_count",
        "data_file_size",
        "index_file_count",
        "index_file_size",
    ] {
        fields.push(f(name, TableDataType::Number(NumberDataType::Int64)));
    }
    fields
}

pub async fn read_system_table(
    kind: PaimonSystemTableKind,
    catalog: Arc<dyn paimon::Catalog>,
    identifier: paimon::catalog::Identifier,
    table: paimon::Table,
) -> Result<DataBlock> {
    match kind {
        PaimonSystemTableKind::Branches => branches::read(&table).await,
        PaimonSystemTableKind::Options => options::read(&table),
        PaimonSystemTableKind::Schemas => schemas::read(&table).await,
        PaimonSystemTableKind::Snapshots => snapshots::read(&table).await,
        PaimonSystemTableKind::Tags => tags::read(&table).await,
        PaimonSystemTableKind::Files => files::read(&table).await,
        PaimonSystemTableKind::Manifests => manifests::read(&table).await,
        PaimonSystemTableKind::Partitions => partitions::read(catalog, &identifier, &table).await,
        PaimonSystemTableKind::PhysicalFilesSize => physical_files_size::read(&table).await,
        PaimonSystemTableKind::ReferencedFilesSize => referenced_files_size::read(&table).await,
        PaimonSystemTableKind::TableIndexes => table_indexes::read(&table).await,
    }
}

fn millis_to_micros(millis: i64, field: &str) -> Result<i64> {
    millis.checked_mul(1000).ok_or_else(|| {
        ErrorCode::ReadTableDataError(format!(
            "Paimon {field} timestamp overflows Databend timestamp: {millis}ms"
        ))
    })
}

fn unsigned_millis_to_micros(millis: u64, field: &str) -> Result<i64> {
    let millis = i64::try_from(millis).map_err(|_| {
        ErrorCode::ReadTableDataError(format!(
            "Paimon {field} timestamp overflows Databend timestamp: {millis}ms"
        ))
    })?;
    millis_to_micros(millis, field)
}

fn json<T: ?Sized + Serialize>(value: &T, field: &str) -> Result<String> {
    serde_json::to_string(value).map_err(|err| {
        ErrorCode::ReadTableDataError(format!("failed to serialize Paimon {field}: {err}"))
    })
}

/// Partition key fields (name + type) in partition-column order, used to decode
/// a partition `BinaryRow` into a display string.
fn partition_fields(table: &paimon::Table) -> Result<Vec<(String, paimon::spec::DataType)>> {
    let schema = table.schema();
    let fields = schema.fields();
    schema
        .partition_keys()
        .iter()
        .map(|key| {
            fields
                .iter()
                .find(|field| field.name() == key)
                .map(|field| (field.name().to_string(), field.data_type().clone()))
                .ok_or_else(|| {
                    ErrorCode::ReadTableDataError(format!(
                        "Paimon partition key {key} is missing from table schema"
                    ))
                })
        })
        .collect()
}

/// Renders a partition `BinaryRow` as `[v0, v1]`, or `None` for unpartitioned tables.
fn format_partition(
    row: &paimon::spec::BinaryRow,
    fields: &[(String, paimon::spec::DataType)],
) -> Result<Option<String>> {
    if fields.is_empty() || row.is_empty() {
        return Ok(None);
    }
    let mut values = Vec::with_capacity(fields.len());
    for (pos, (_name, data_type)) in fields.iter().enumerate() {
        match crate::error::map_paimon_result(row.get_datum(pos, data_type))? {
            Some(datum) => values.push(datum_plain(&datum)),
            None => values.push("null".to_string()),
        }
    }
    Ok(Some(format!("[{}]", values.join(", "))))
}

/// Renders a partition spec (key -> value string) as `[v0, v1]` in partition-key
/// order, or `None` for unpartitioned tables. Used by the `partitions` table,
/// whose values come from the catalog rather than a `BinaryRow`.
fn format_partition_spec(
    spec: &std::collections::HashMap<String, String>,
    partition_keys: &[String],
) -> Option<String> {
    if partition_keys.is_empty() {
        return None;
    }
    let values: Vec<String> = partition_keys
        .iter()
        .map(|key| spec.get(key).cloned().unwrap_or_else(|| "null".to_string()))
        .collect();
    Some(format!("[{}]", values.join(", ")))
}

/// Renders a `Datum` as plain text (unquoted strings), matching Paimon's cast
/// semantics for the partition/key display columns.
fn datum_plain(datum: &paimon::spec::Datum) -> String {
    match datum {
        paimon::spec::Datum::String(value) => value.clone(),
        other => other.to_string(),
    }
}

/// Stats columns as (name, type) in stats-encoding order. Type is `None` when a
/// stats column is absent from the current schema (then it decodes to null).
type StatsColumns = Vec<(String, Option<paimon::spec::DataType>)>;

/// Resolves the columns covered by a data file's `value_stats`, following the
/// same precedence Paimon uses internally: `value_stats_cols`, then `write_cols`,
/// then all schema fields in order.
fn value_stats_columns(
    table: &paimon::Table,
    value_stats_cols: Option<&Vec<String>>,
    write_cols: Option<&Vec<String>>,
) -> StatsColumns {
    let fields = table.schema().fields();
    let names: Vec<String> = value_stats_cols.or(write_cols).cloned().unwrap_or_else(|| {
        fields
            .iter()
            .map(|field| field.name().to_string())
            .collect()
    });
    stats_columns_with_types(fields, names)
}

/// Trimmed primary-key columns (primary keys minus partition keys), used to
/// decode a data file's `min_key`/`max_key`.
fn trimmed_key_columns(table: &paimon::Table) -> StatsColumns {
    let schema = table.schema();
    let partition: HashSet<&str> = schema.partition_keys().iter().map(String::as_str).collect();
    let names: Vec<String> = schema
        .primary_keys()
        .iter()
        .filter(|key| !partition.contains(key.as_str()))
        .cloned()
        .collect();
    stats_columns_with_types(schema.fields(), names)
}

fn stats_columns_with_types(
    fields: &[paimon::spec::DataField],
    names: Vec<String>,
) -> StatsColumns {
    names
        .into_iter()
        .map(|name| {
            let data_type = fields
                .iter()
                .find(|field| field.name() == name)
                .map(|field| field.data_type().clone());
            (name, data_type)
        })
        .collect()
}

/// Decodes a serialized stats `BinaryRow` (min/max values of `BinaryTableStats`)
/// into a `{column: value}` JSON object. Returns `None` when the stats are empty
/// or cannot be decoded, matching Paimon's own fail-open stats handling.
fn decode_stats_json(
    bytes: &[u8],
    columns: &[(String, Option<paimon::spec::DataType>)],
) -> Result<Option<String>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let Ok(row) = paimon::spec::BinaryRow::from_serialized_bytes(bytes) else {
        return Ok(None);
    };
    let mut map = serde_json::Map::with_capacity(columns.len());
    for (pos, (name, data_type)) in columns.iter().enumerate() {
        let value = match data_type {
            Some(data_type) => match row.get_datum(pos, data_type).ok().flatten() {
                Some(datum) => datum_to_json(&datum),
                None => serde_json::Value::Null,
            },
            None => serde_json::Value::Null,
        };
        map.insert(name.clone(), value);
    }
    json(&serde_json::Value::Object(map), "paimon stats").map(Some)
}

/// Decodes a serialized key `BinaryRow` (`min_key`/`max_key`) as `[v0, v1]`, or
/// `None` when empty (e.g. append-only tables carry no trimmed primary key).
fn decode_key(
    bytes: &[u8],
    columns: &[(String, Option<paimon::spec::DataType>)],
) -> Option<String> {
    if bytes.is_empty() || columns.is_empty() {
        return None;
    }
    let row = paimon::spec::BinaryRow::from_serialized_bytes(bytes).ok()?;
    let mut values = Vec::with_capacity(columns.len());
    for (pos, (_name, data_type)) in columns.iter().enumerate() {
        let rendered = match data_type {
            Some(data_type) => match row.get_datum(pos, data_type).ok().flatten() {
                Some(datum) => datum_plain(&datum),
                None => "null".to_string(),
            },
            None => "null".to_string(),
        };
        values.push(rendered);
    }
    Some(format!("[{}]", values.join(", ")))
}

fn datum_to_json(datum: &paimon::spec::Datum) -> serde_json::Value {
    use paimon::spec::Datum;
    use serde_json::Value;
    match datum {
        Datum::Bool(v) => Value::Bool(*v),
        Datum::TinyInt(v) => Value::from(*v),
        Datum::SmallInt(v) => Value::from(*v),
        Datum::Int(v) => Value::from(*v),
        Datum::Long(v) => Value::from(*v),
        Datum::Float(v) => serde_json::Number::from_f64(*v as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Datum::Double(v) => serde_json::Number::from_f64(*v)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Datum::String(v) => Value::String(v.clone()),
        other => Value::String(other.to_string()),
    }
}
