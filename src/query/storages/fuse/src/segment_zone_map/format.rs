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

//! Parquet layout of the segment zone map file.
//!
//! Fields 0..3 describe the segment: `path`, `block_count`, and `fresh`
//! (whether the build that wrote the file added the row, or copied it from the
//! previous zone map). Each covered table column `c` then takes three adjacent
//! fields: `min_c` and `max_c` (nullable, of the column's non-null type) and
//! `null_count_c` (nullable UInt64). A NULL `null_count_c` means the segment has
//! no usable statistics for `c`; readers leave the column out of the statistics
//! map, which range pruning treats as an unknown domain. A non-NULL
//! `null_count_c` with NULL `min_c`/`max_c` means the column is entirely NULL
//! in that segment, as in the segment summary.
//!
//! The footer's key-value metadata records the covered columns with their
//! types and the admission ratio the build used. The snapshot only points at
//! the footer; readers fetch it first and then the column chunks they need.

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::future::IntoFuture;
use std::ops::Range;

use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentZoneMapMeta;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::supported_stat_type;
use databend_storages_common_table_meta::table::TableCompression;
use futures::future::try_join_all;
use opendal::Buffer;
use opendal::Operator;
use parquet::file::metadata::KeyValue;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::metadata::ParquetMetaDataReader;
use serde::Deserialize;
use serde::Serialize;

use crate::io::read::DataItem;
use crate::io::read::column_chunks_to_record_batch;

/// Number of leading fields describing the segment itself: path, block_count, fresh.
const SEGMENT_FIELDS: usize = 3;
/// Fields per covered column: min, max, null_count.
const STAT_FIELDS: usize = 3;

const KV_COLUMNS: &str = "databend.segment_zone_map.columns";
const KV_RATIO: &str = "databend.segment_zone_map.ratio";

const WRITE_COMPRESSION: TableCompression = TableCompression::Zstd;
const READ_COMPRESSION: Compression = Compression::Zstd;
/// Parquet footer: 4-byte little-endian metadata length followed by `PAR1`.
const PARQUET_FOOTER_TAIL: usize = 8;

const U64: TableDataType = TableDataType::Number(NumberDataType::UInt64);

/// Type of the `null_count` fields: NULL marks unknown statistics.
fn null_count_type() -> TableDataType {
    U64.wrap_nullable()
}

/// A table leaf column stored in the segment zone map.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZoneMapColumn {
    pub column_id: ColumnId,
    pub data_type: TableDataType,
}

impl ZoneMapColumn {
    /// Type of the stored `min`/`max`: the column's non-null type, made nullable.
    fn value_type(&self) -> TableDataType {
        self.data_type.remove_nullable().wrap_nullable()
    }
}

/// Leaf columns of `schema` that carry min/max statistics.
pub fn zone_map_columns(schema: &TableSchema) -> Vec<ZoneMapColumn> {
    let mut columns = Vec::new();
    for field in schema.leaf_fields() {
        if supported_stat_type(&DataType::from(field.data_type())) {
            let data_type = field.data_type().clone();
            columns.push(ZoneMapColumn {
                column_id: field.column_id(),
                data_type,
            });
        }
    }
    columns
}

/// Field ids follow the field order, as all fields are primitive.
fn file_schema(columns: &[ZoneMapColumn]) -> TableSchema {
    let mut fields = vec![
        TableField::new("path", TableDataType::String),
        TableField::new("block_count", U64),
        TableField::new("fresh", TableDataType::Boolean),
    ];
    for column in columns {
        let (id, value_type) = (column.column_id, column.value_type());
        fields.push(TableField::new(&format!("min_{id}"), value_type.clone()));
        fields.push(TableField::new(&format!("max_{id}"), value_type));
        fields.push(TableField::new(
            &format!("null_count_{id}"),
            null_count_type(),
        ));
    }
    TableSchema::new(fields)
}

/// Field id of `min` of the covered column at `idx`; `max` and `null_count` follow.
fn stat_field_id(idx: usize) -> usize {
    SEGMENT_FIELDS + idx * STAT_FIELDS
}

fn u64_scalar(value: u64) -> ScalarRef<'static> {
    ScalarRef::Number(NumberScalar::UInt64(value))
}

struct StatColumnBuilder {
    /// Non-null type the stored scalars must have.
    value_type: DataType,
    min: ColumnBuilder,
    max: ColumnBuilder,
    null_count: ColumnBuilder,
}

impl StatColumnBuilder {
    fn new(column: &ZoneMapColumn, capacity: usize) -> Self {
        let value_type = DataType::from(&column.value_type());
        Self {
            value_type: value_type.remove_nullable(),
            min: ColumnBuilder::with_capacity(&value_type, capacity),
            max: ColumnBuilder::with_capacity(&value_type, capacity),
            null_count: ColumnBuilder::with_capacity(&DataType::from(&null_count_type()), capacity),
        }
    }

    fn accepts(&self, value: ScalarRef) -> bool {
        value == ScalarRef::Null || value.infer_data_type() == self.value_type
    }

    /// Statistics of another type than the column's are as unknown as missing ones.
    fn push(&mut self, stats: Option<&ColumnStatistics>) {
        match stats {
            Some(s) if self.accepts(s.min.as_ref()) && self.accepts(s.max.as_ref()) => {
                self.min.push(s.min.as_ref());
                self.max.push(s.max.as_ref());
                self.null_count.push(u64_scalar(s.null_count));
            }
            _ => self.push_unknown(),
        }
    }

    fn push_unknown(&mut self) {
        self.min.push(ScalarRef::Null);
        self.max.push(ScalarRef::Null);
        self.null_count.push(ScalarRef::Null);
    }

    fn push_from(&mut self, stats: &StatColumns, row: usize) {
        self.min.push(stats.min.index(row).unwrap());
        self.max.push(stats.max.index(row).unwrap());
        self.null_count.push(stats.null_count.index(row).unwrap());
    }
}

/// For each column of a builder, the position of the same column (same id and
/// type) in a previous zone map, if it was read. See [`SegmentZoneMapBuilder::copy_sources`].
pub struct CopySources(Vec<Option<usize>>);

/// Accumulates zone map rows in segment order.
pub struct SegmentZoneMapBuilder {
    columns: Vec<ZoneMapColumn>,
    ratio: f64,
    paths: ColumnBuilder,
    block_counts: ColumnBuilder,
    fresh: ColumnBuilder,
    stats: Vec<StatColumnBuilder>,
    num_rows: usize,
}

impl SegmentZoneMapBuilder {
    /// `ratio` is the admission ratio of this build, recorded for the next one.
    pub fn new(columns: Vec<ZoneMapColumn>, ratio: f64, capacity: usize) -> Self {
        let mut stats = Vec::with_capacity(columns.len());
        for column in &columns {
            stats.push(StatColumnBuilder::new(column, capacity));
        }
        Self {
            columns,
            ratio,
            paths: ColumnBuilder::with_capacity(&DataType::String, capacity),
            block_counts: ColumnBuilder::with_capacity(&DataType::from(&U64), capacity),
            fresh: ColumnBuilder::with_capacity(&DataType::Boolean, capacity),
            stats,
            num_rows: 0,
        }
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Append a fresh row from a segment summary.
    pub fn push_summary(&mut self, path: &str, summary: &Statistics) {
        self.paths.push(ScalarRef::String(path));
        self.block_counts.push(u64_scalar(summary.block_count));
        self.fresh.push(ScalarRef::Boolean(true));
        for (column, builder) in self.columns.iter().zip(&mut self.stats) {
            builder.push(summary.col_stats.get(&column.column_id));
        }
        self.num_rows += 1;
    }

    /// Match this builder's columns against `previous`. A column is copied only
    /// when `previous` stores it with the same id and type and it was read; a
    /// column whose type changed, or that `previous` lacks, is written as unknown.
    pub fn copy_sources(&self, previous: &SegmentZoneMapData) -> CopySources {
        let mut sources = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            let idx = previous.columns.iter().position(|c| c == column);
            sources.push(idx.filter(|idx| previous.stats[*idx].is_some()));
        }
        CopySources(sources)
    }

    /// Copy row `row` of `previous` as a non-fresh row.
    pub fn push_copy(&mut self, previous: &SegmentZoneMapData, row: usize, sources: &CopySources) {
        let (path, block_count) = (previous.path(row), previous.block_count(row));
        self.paths.push(ScalarRef::String(path));
        self.block_counts.push(u64_scalar(block_count));
        self.fresh.push(ScalarRef::Boolean(false));
        for (builder, source) in self.stats.iter_mut().zip(&sources.0) {
            match source.and_then(|idx| previous.stats[idx].as_ref()) {
                Some(stats) => builder.push_from(stats, row),
                None => builder.push_unknown(),
            }
        }
        self.num_rows += 1;
    }

    /// Serialize and write the zone map, returning its pointer.
    pub async fn write(
        self,
        operator: &Operator,
        location: Location,
    ) -> Result<SegmentZoneMapMeta> {
        if self.num_rows == 0 {
            return Err(ErrorCode::Internal("segment zone map has no rows"));
        }
        let schema = file_schema(&self.columns);
        let mut columns = vec![
            self.paths.build(),
            self.block_counts.build(),
            self.fresh.build(),
        ];
        for stats in self.stats {
            columns.push(stats.min.build());
            columns.push(stats.max.build());
            columns.push(stats.null_count.build());
        }
        let blocks = vec![DataBlock::new_from_columns(columns)];
        let metadata = vec![
            KeyValue::new(
                KV_COLUMNS.to_string(),
                serde_json::to_string(&self.columns)?,
            ),
            KeyValue::new(KV_RATIO.to_string(), self.ratio.to_string()),
        ];
        let compression = WRITE_COMPRESSION;
        let serialized = blocks_to_parquet(&schema, blocks, compression, false, Some(metadata))?;
        let footer_offset = footer_offset(&serialized.payload)?;
        let payload = Buffer::from(serialized.payload);
        operator.write(&location.0, payload).await?;
        Ok(SegmentZoneMapMeta {
            location,
            footer_offset,
        })
    }
}

/// Offset of the footer metadata of a complete parquet file given as chunks.
fn footer_offset(payload: &[Bytes]) -> Result<u64> {
    let total: usize = payload.iter().map(Bytes::len).sum();
    // The 8-byte tail may straddle chunks; gather it from the end.
    let mut tail = Vec::with_capacity(PARQUET_FOOTER_TAIL);
    for chunk in payload.iter().rev() {
        let take = chunk.len().min(PARQUET_FOOTER_TAIL - tail.len());
        tail.splice(0..0, chunk[chunk.len() - take..].iter().copied());
        if tail.len() == PARQUET_FOOTER_TAIL {
            break;
        }
    }
    let Some(footer_len) = parse_footer_tail(&tail) else {
        return Err(ErrorCode::Internal(
            "serialized segment zone map is not parquet",
        ));
    };
    match total.checked_sub(PARQUET_FOOTER_TAIL + footer_len) {
        Some(offset) => Ok(offset as u64),
        None => Err(ErrorCode::Internal(
            "serialized segment zone map footer is longer than the file",
        )),
    }
}

/// Footer metadata length from the 8 trailing bytes of a parquet file.
fn parse_footer_tail(tail: &[u8]) -> Option<usize> {
    if tail.len() != PARQUET_FOOTER_TAIL || &tail[4..] != b"PAR1" {
        return None;
    }
    Some(u32::from_le_bytes(tail[..4].try_into().unwrap()) as usize)
}

fn kv_value<'a>(parquet: &'a ParquetMetaData, key: &str) -> Option<&'a str> {
    let kvs = parquet.file_metadata().key_value_metadata()?;
    kvs.iter().find(|kv| kv.key == key)?.value.as_deref()
}

struct StatColumns {
    min: Column,
    max: Column,
    null_count: Column,
}

/// An opened zone map file: its footer, parsed. Column data is read on demand.
pub struct SegmentZoneMapFile {
    operator: Operator,
    path: String,
    num_rows: usize,
    parquet: ParquetMetaData,
    columns: Vec<ZoneMapColumn>,
    ratio: f64,
}

impl SegmentZoneMapFile {
    pub async fn open(operator: &Operator, meta: &SegmentZoneMapMeta) -> Result<Self> {
        let (path, version) = meta.location.clone();
        let invalid = |what: String| invalid(&path, what);
        // A file of another format version may lay its fields out differently.
        if version != SegmentZoneMapMeta::VERSION {
            return Err(invalid(format!("unsupported format version {version}")));
        }
        // One read to the end of the file: footer metadata, its length, magic.
        let tail = operator.read_with(&path).range(meta.footer_offset..);
        let tail = tail.await?.to_bytes();
        let footer_len = tail.len().saturating_sub(PARQUET_FOOTER_TAIL);
        if parse_footer_tail(&tail[footer_len..]) != Some(footer_len) {
            return Err(invalid("footer offset does not match the file".into()));
        }
        let parquet = ParquetMetaDataReader::decode_metadata(&tail[..footer_len])
            .map_err(|e| invalid(format!("footer: {e}")))?;

        let json = kv_value(&parquet, KV_COLUMNS).ok_or_else(|| invalid("no columns".into()))?;
        let columns: Vec<ZoneMapColumn> =
            serde_json::from_str(json).map_err(|e| invalid(format!("columns: {e}")))?;
        let ratio = kv_value(&parquet, KV_RATIO).ok_or_else(|| invalid("no ratio".into()))?;
        let ratio: f64 = ratio.parse().map_err(|e| invalid(format!("ratio: {e}")))?;

        let expected = SEGMENT_FIELDS + columns.len() * STAT_FIELDS;
        let fields = parquet.file_metadata().schema_descr().num_columns();
        let groups = parquet.num_row_groups();
        if groups != 1 || fields != expected {
            let found = format!("{groups} row groups of {fields} fields");
            return Err(invalid(format!("{found}, want 1 of {expected}")));
        }
        let num_rows = usize::try_from(parquet.file_metadata().num_rows())?;
        Ok(Self {
            operator: operator.clone(),
            path,
            num_rows,
            parquet,
            columns,
            ratio,
        })
    }

    pub fn columns(&self) -> &[ZoneMapColumn] {
        &self.columns
    }

    /// Admission ratio the build that wrote this file used.
    pub fn ratio(&self) -> f64 {
        self.ratio
    }

    /// Read the segment fields and the statistics of the given table columns.
    pub async fn read(&self, column_ids: &HashSet<ColumnId>) -> Result<SegmentZoneMapData> {
        let mut selected = Vec::new();
        let mut field_ids: Vec<usize> = (0..SEGMENT_FIELDS).collect();
        for (idx, column) in self.columns.iter().enumerate() {
            if column_ids.contains(&column.column_id) {
                selected.push(idx);
                field_ids.extend(stat_field_id(idx)..stat_field_id(idx + 1));
            }
        }

        let chunks = self.read_fields(&field_ids).await?;
        let schema = file_schema(&self.columns);
        let (rows, compression) = (self.num_rows, &READ_COMPRESSION);
        let batch = column_chunks_to_record_batch(&schema, rows, &chunks, compression, None, None)?;
        let column_of = |field_id: usize| -> Result<Column> {
            let field = &schema.fields()[field_id];
            let missing = || invalid(&self.path, format!("missing {}", field.name()));
            let array = batch.column_by_name(field.name()).ok_or_else(missing)?;
            Column::from_arrow_rs(array.clone(), &DataType::from(field.data_type()))
        };

        let mut stats = Vec::new();
        stats.resize_with(self.columns.len(), || None);
        for idx in selected {
            let first = stat_field_id(idx);
            stats[idx] = Some(StatColumns {
                min: column_of(first)?,
                max: column_of(first + 1)?,
                null_count: column_of(first + 2)?,
            });
        }
        Ok(SegmentZoneMapData {
            columns: self.columns.clone(),
            paths: column_of(0)?,
            block_counts: column_of(1)?,
            fresh: column_of(2)?,
            stats,
        })
    }

    /// Fetch the chunks of `field_ids` (ascending). Adjacent fields are adjacent
    /// in the file: one request per run of them, all issued concurrently.
    async fn read_fields(&self, field_ids: &[usize]) -> Result<HashMap<ColumnId, DataItem<'_>>> {
        let row_group = self.parquet.row_group(0);
        let bytes_of = |field_id: usize| -> Range<u64> {
            let (offset, len) = row_group.column(field_id).byte_range();
            offset..offset + len
        };
        // Runs of adjacent fields, each with the byte range covering them.
        let mut runs: Vec<(Range<usize>, Range<u64>)> = Vec::new();
        for &field_id in field_ids {
            let bytes = bytes_of(field_id);
            match runs.last_mut() {
                Some((fields, run)) if fields.end == field_id => {
                    fields.end += 1;
                    run.start = run.start.min(bytes.start);
                    run.end = run.end.max(bytes.end);
                }
                _ => runs.push((field_id..field_id + 1, bytes)),
            }
        }
        let read = |run: &Range<u64>| self.operator.read_with(&self.path).range(run.clone());
        let buffers = try_join_all(runs.iter().map(|(_, run)| read(run).into_future())).await?;

        let mut chunks = HashMap::with_capacity(field_ids.len());
        for ((fields, run), buffer) in runs.iter().zip(buffers) {
            for field_id in fields.clone() {
                let bytes = bytes_of(field_id);
                let start = (bytes.start - run.start) as usize;
                let end = (bytes.end - run.start) as usize;
                if end > buffer.len() {
                    return Err(invalid(&self.path, format!("truncated field {field_id}")));
                }
                chunks.insert(
                    field_id as ColumnId,
                    DataItem::RawData(buffer.slice(start..end)),
                );
            }
        }
        Ok(chunks)
    }
}

/// Rows of one zone map file: the segment fields plus the statistics that were read.
pub struct SegmentZoneMapData {
    columns: Vec<ZoneMapColumn>,
    paths: Column,
    block_counts: Column,
    fresh: Column,
    /// Aligned with `columns`; `None` for columns that were not read.
    stats: Vec<Option<StatColumns>>,
}

impl SegmentZoneMapData {
    pub fn num_rows(&self) -> usize {
        self.paths.len()
    }

    pub fn path(&self, row: usize) -> &str {
        match self.paths.index(row) {
            Some(ScalarRef::String(path)) => path,
            _ => unreachable!("segment zone map path column is a non-null string"),
        }
    }

    pub fn block_count(&self, row: usize) -> u64 {
        match self.block_counts.index(row) {
            Some(ScalarRef::Number(NumberScalar::UInt64(count))) => count,
            _ => unreachable!("segment zone map block_count column is a non-null UInt64"),
        }
    }

    /// Whether the build that wrote the file added this row.
    pub fn is_fresh(&self, row: usize) -> bool {
        match self.fresh.index(row) {
            Some(ScalarRef::Boolean(fresh)) => fresh,
            _ => unreachable!("segment zone map fresh column is a non-null boolean"),
        }
    }

    /// Row of each covered segment path.
    pub fn path_rows(&self) -> HashMap<&str, usize> {
        let rows = 0..self.num_rows();
        rows.map(|row| (self.path(row), row)).collect()
    }

    /// Fill `out` with the known statistics of row `row` among the columns read.
    pub fn fill_stats(&self, row: usize, out: &mut StatisticsOfColumns) {
        out.clear();
        for (column, stats) in self.columns.iter().zip(&self.stats) {
            let Some(stats) = stats else { continue };
            // A NULL null_count marks the statistics as unknown.
            let Some(ScalarRef::Number(NumberScalar::UInt64(nulls))) = stats.null_count.index(row)
            else {
                continue;
            };
            let (min, max) = (stats.min.index(row).unwrap(), stats.max.index(row).unwrap());
            let stats = ColumnStatistics::new(min.to_owned(), max.to_owned(), nulls, 0, None);
            out.insert(column.column_id, stats);
        }
    }
}

fn invalid(path: &str, what: impl Display) -> ErrorCode {
    ErrorCode::StorageOther(format!("invalid segment zone map {path}: {what}"))
}

#[cfg(test)]
mod tests {
    use databend_common_column::types::timestamp_tz;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::DecimalDataType;
    use databend_common_expression::types::DecimalScalar;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::i256;
    use opendal::services::Memory;

    use super::*;

    fn schema() -> TableSchema {
        TableSchema::new(vec![
            TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("b", TableDataType::String.wrap_nullable()),
            // No min/max statistics for Variant, so it is not covered.
            TableField::new("v", TableDataType::Variant),
        ])
    }

    fn stats(min: Scalar, max: Scalar, null_count: u64) -> ColumnStatistics {
        ColumnStatistics::new(min, max, null_count, 0, None)
    }

    fn int_stats(min: i32, max: i32) -> ColumnStatistics {
        let scalar = |v| Scalar::Number(NumberScalar::Int32(v));
        stats(scalar(min), scalar(max), 0)
    }

    fn string_stats(min: &str, max: &str, null_count: u64) -> ColumnStatistics {
        stats(
            Scalar::String(min.into()),
            Scalar::String(max.into()),
            null_count,
        )
    }

    fn summary(block_count: u64, col_stats: StatisticsOfColumns) -> Statistics {
        Statistics {
            block_count,
            col_stats,
            ..Default::default()
        }
    }

    fn all_column_ids(columns: &[ZoneMapColumn]) -> HashSet<ColumnId> {
        columns.iter().map(|column| column.column_id).collect()
    }

    async fn write(
        builder: SegmentZoneMapBuilder,
        op: &Operator,
        name: &str,
    ) -> Result<SegmentZoneMapMeta> {
        let location = (name.to_string(), SegmentZoneMapMeta::VERSION);
        builder.write(op, location).await
    }

    /// Open and read every covered column.
    async fn read_all(op: &Operator, meta: &SegmentZoneMapMeta) -> Result<SegmentZoneMapData> {
        let file = SegmentZoneMapFile::open(op, meta).await?;
        file.read(&all_column_ids(file.columns())).await
    }

    fn read_stats(data: &SegmentZoneMapData, path: &str) -> StatisticsOfColumns {
        let mut out = StatisticsOfColumns::new();
        data.fill_stats(data.path_rows()[path], &mut out);
        out
    }

    /// The zone map keeps min, max and null_count only.
    fn normalize(col_stats: &StatisticsOfColumns) -> StatisticsOfColumns {
        let keep = |s: &ColumnStatistics| stats(s.min.clone(), s.max.clone(), s.null_count);
        col_stats.iter().map(|(id, s)| (*id, keep(s))).collect()
    }

    #[tokio::test]
    async fn test_segment_zone_map_round_trip() -> Result<()> {
        let op = Operator::new(Memory::default())?.finish();
        let columns = zone_map_columns(&schema());
        assert_eq!(all_column_ids(&columns), HashSet::from([0, 1]));

        let seg_a =
            StatisticsOfColumns::from([(0, int_stats(1, 10)), (1, string_stats("x", "y", 2))]);
        // All-null column: min/max are NULL but the statistics are known.
        let seg_b = StatisticsOfColumns::from([
            (0, int_stats(20, 30)),
            (1, stats(Scalar::Null, Scalar::Null, 5)),
        ]);
        // Missing statistics and statistics of the wrong type are both unknown.
        let seg_c = StatisticsOfColumns::from([(0, string_stats("1", "2", 0))]);

        let mut builder = SegmentZoneMapBuilder::new(columns.clone(), 0.75, 3);
        builder.push_summary("seg_a", &summary(1000, seg_a.clone()));
        builder.push_summary("seg_b", &summary(7, seg_b.clone()));
        builder.push_summary("seg_c", &summary(1, seg_c));
        let meta = write(builder, &op, "zm_1.parquet").await?;

        let file = SegmentZoneMapFile::open(&op, &meta).await?;
        assert_eq!(file.columns(), &columns[..]);
        assert_eq!(file.ratio(), 0.75);

        let data = file.read(&all_column_ids(&columns)).await?;
        assert_eq!(data.num_rows(), 3);
        assert_eq!(read_stats(&data, "seg_a"), normalize(&seg_a));
        assert_eq!(read_stats(&data, "seg_b"), normalize(&seg_b));
        assert!(read_stats(&data, "seg_c").is_empty());
        assert_eq!(data.block_count(0), 1000);
        assert_eq!(data.block_count(1), 7);
        assert!((0..3).all(|row| data.is_fresh(row)));

        // Only the requested column is read.
        let data = file.read(&HashSet::from([1])).await?;
        assert_eq!(
            read_stats(&data, "seg_a").into_keys().collect::<Vec<_>>(),
            vec![1]
        );

        // Segment fields alone, as used for the start decision.
        let data = file.read(&HashSet::new()).await?;
        assert!(read_stats(&data, "seg_b").is_empty());
        assert_eq!(data.path_rows().len(), 3);
        assert_eq!(data.block_count(2), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_segment_zone_map_copy_rows() -> Result<()> {
        let op = Operator::new(Memory::default())?.finish();
        let columns = zone_map_columns(&schema());
        let seg_a =
            StatisticsOfColumns::from([(0, int_stats(1, 10)), (1, string_stats("x", "y", 2))]);
        let mut builder = SegmentZoneMapBuilder::new(columns.clone(), 1.0, 1);
        builder.push_summary("seg_a", &summary(1000, seg_a.clone()));
        let previous = read_all(&op, &write(builder, &op, "zm_1.parquet").await?).await?;

        // Column 1 changed type (string to binary), column 2 is new.
        let mut changed = columns.clone();
        changed[1].data_type = TableDataType::Binary.wrap_nullable();
        changed.push(ZoneMapColumn {
            column_id: 2,
            data_type: TableDataType::Date,
        });
        let seg_b = StatisticsOfColumns::from([(0, int_stats(20, 30))]);
        let mut builder = SegmentZoneMapBuilder::new(changed.clone(), 0.5, 2);
        let sources = builder.copy_sources(&previous);
        assert_eq!(sources.0, vec![Some(0), None, None]);
        builder.push_summary("seg_b", &summary(3, seg_b.clone()));
        builder.push_copy(&previous, previous.path_rows()["seg_a"], &sources);
        let data = read_all(&op, &write(builder, &op, "zm_2.parquet").await?).await?;

        assert_eq!(data.path(0), "seg_b");
        assert!(data.is_fresh(0));
        assert_eq!(data.path(1), "seg_a");
        assert!(!data.is_fresh(1));
        assert_eq!(data.block_count(1), 1000);
        // Only the column that kept its type was copied.
        assert_eq!(
            read_stats(&data, "seg_a"),
            StatisticsOfColumns::from([(0, int_stats(1, 10))])
        );
        assert_eq!(read_stats(&data, "seg_b"), normalize(&seg_b));
        Ok(())
    }

    /// Every type that carries min/max statistics survives the parquet round
    /// trip unchanged: pruning dereferences these scalars by variant.
    #[tokio::test]
    async fn test_segment_zone_map_round_trip_all_stat_types() -> Result<()> {
        use NumberDataType::*;
        use NumberScalar as N;
        use Scalar::Number as Num;
        use TableDataType::Number as NumType;

        let decimal = |precision, scale, value: i128| {
            let size = DecimalSize::new_unchecked(precision, scale);
            let scalar = match DecimalDataType::from(size) {
                DecimalDataType::Decimal64(_) => DecimalScalar::Decimal64(value as i64, size),
                DecimalDataType::Decimal128(_) => DecimalScalar::Decimal128(value, size),
                DecimalDataType::Decimal256(_) => {
                    DecimalScalar::Decimal256(i256::from(value), size)
                }
            };
            Scalar::Decimal(scalar)
        };
        let decimal_type = |precision, scale| {
            TableDataType::Decimal(DecimalSize::new_unchecked(precision, scale).into())
        };
        let ts = |micros, offset| Scalar::TimestampTz(timestamp_tz::new(micros, offset));
        // (type, min, max) per column.
        let cases: Vec<(TableDataType, Scalar, Scalar)> = vec![
            (NumType(Int8), Num(N::Int8(-128)), Num(N::Int8(127))),
            (NumType(UInt8), Num(N::UInt8(0)), Num(N::UInt8(255))),
            (NumType(Int16), Num(N::Int16(-7)), Num(N::Int16(7))),
            (NumType(UInt16), Num(N::UInt16(1)), Num(N::UInt16(65535))),
            (
                NumType(Int32),
                Num(N::Int32(i32::MIN)),
                Num(N::Int32(i32::MAX)),
            ),
            (NumType(UInt32), Num(N::UInt32(0)), Num(N::UInt32(u32::MAX))),
            (
                NumType(Int64),
                Num(N::Int64(i64::MIN)),
                Num(N::Int64(i64::MAX)),
            ),
            (NumType(UInt64), Num(N::UInt64(0)), Num(N::UInt64(u64::MAX))),
            (
                NumType(Float32),
                Num(N::Float32((-1.5f32).into())),
                Num(N::Float32(f32::MAX.into())),
            ),
            (
                NumType(Float64),
                Num(N::Float64((-2.25f64).into())),
                Num(N::Float64(f64::MAX.into())),
            ),
            (
                TableDataType::Date,
                Scalar::Date(-365),
                Scalar::Date(20_000),
            ),
            (
                TableDataType::Timestamp,
                Scalar::Timestamp(-1),
                Scalar::Timestamp(1_700_000_000_000_000),
            ),
            (
                TableDataType::TimestampTz,
                ts(-1, -8 * 3600),
                ts(1_700_000_000_000_000, 8 * 3600),
            ),
            (
                TableDataType::String,
                Scalar::String(String::new()),
                Scalar::String("\u{10FFFF}zzz".into()),
            ),
            (
                decimal_type(10, 2),
                decimal(10, 2, -12345),
                decimal(10, 2, 99999),
            ),
            (
                decimal_type(38, 10),
                decimal(38, 10, i128::MIN / 2),
                decimal(38, 10, i128::MAX / 2),
            ),
            (decimal_type(60, 5), decimal(60, 5, -1), decimal(60, 5, 1)),
        ];

        let mut fields = Vec::new();
        let mut col_stats = StatisticsOfColumns::new();
        for (i, (ty, min, max)) in cases.into_iter().enumerate() {
            fields.push(TableField::new(&format!("c{i}"), ty));
            col_stats.insert(i as ColumnId, stats(min, max, i as u64));
        }
        let columns = zone_map_columns(&TableSchema::new(fields));
        assert_eq!(columns.len(), col_stats.len());

        let op = Operator::new(Memory::default())?.finish();
        let mut builder = SegmentZoneMapBuilder::new(columns, 1.0, 1);
        builder.push_summary("seg", &summary(1, col_stats.clone()));
        let data = read_all(&op, &write(builder, &op, "zm.parquet").await?).await?;
        assert_eq!(read_stats(&data, "seg"), col_stats);
        Ok(())
    }

    /// Segments written before decimal storage followed the precision keep
    /// 128-bit values for small decimals; the zone map stores them at the width
    /// the column has now.
    #[tokio::test]
    async fn test_segment_zone_map_narrows_wide_decimal_stats() -> Result<()> {
        let size = DecimalSize::new_unchecked(8, 4);
        let schema = TableSchema::new(vec![TableField::new(
            "d",
            TableDataType::Decimal(size.into()),
        )]);
        let wide = |v: i128| Scalar::Decimal(DecimalScalar::Decimal128(v, size));
        let narrow = |v: i64| Scalar::Decimal(DecimalScalar::Decimal64(v, size));
        let col_stats = StatisticsOfColumns::from([(0, stats(wide(-15000), wide(25000), 1))]);

        let op = Operator::new(Memory::default())?.finish();
        let mut builder = SegmentZoneMapBuilder::new(zone_map_columns(&schema), 1.0, 1);
        builder.push_summary("seg", &summary(1, col_stats));
        let data = read_all(&op, &write(builder, &op, "zm.parquet").await?).await?;
        let expected = StatisticsOfColumns::from([(0, stats(narrow(-15000), narrow(25000), 1))]);
        assert_eq!(read_stats(&data, "seg"), expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_segment_zone_map_open_rejects_corrupt_pointer() -> Result<()> {
        let op = Operator::new(Memory::default())?.finish();
        let mut builder = SegmentZoneMapBuilder::new(zone_map_columns(&schema()), 1.0, 1);
        builder.push_summary("seg_a", &summary(1, StatisticsOfColumns::new()));
        let meta = write(builder, &op, "zm_1.parquet").await?;
        let corrupt = |f: fn(&mut SegmentZoneMapMeta)| {
            let mut meta = meta.clone();
            f(&mut meta);
            meta
        };
        for meta in [
            corrupt(|m| m.footer_offset += 1),
            corrupt(|m| m.footer_offset = 0),
            corrupt(|m| m.location.1 += 1),
            corrupt(|m| m.location.0 = "missing.parquet".to_string()),
        ] {
            assert!(
                SegmentZoneMapFile::open(&op, &meta).await.is_err(),
                "{meta:?}"
            );
        }
        Ok(())
    }
}
