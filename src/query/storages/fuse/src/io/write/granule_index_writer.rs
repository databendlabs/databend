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

//! Sparse granule index: a ClickHouse-style mark file mapping cluster-key granules to the physical
//! page byte ranges of each leaf column. Split into two columnar parquet sidecar files by access
//! pattern:
//!
//! - **mins** (`m{i}`, one per cluster-key element): each granule's cluster-key min. Read on the
//!   prune hot path when a cluster-key predicate applies. Absent for a table without a cluster key.
//! - **offsets** (`g_{leaf_column_id}`, plus granule-level index columns like `gbloom_*`): each
//!   granule's first-data-page absolute byte offset within the block file. Read only when a
//!   surviving block is actually scanned.
//!
//! Neither file carries any custom footer metadata. Everything the read path needs to *locate* a
//! column is recorded in block meta as a [`GranuleIndexLayout`]: per file, per logical column, the
//! byte range(s) of that column's parquet chunk. The read path fetches those raw bytes and decodes
//! them via the column-chunk path ([`column_chunks_to_record_batch`]) without ever parsing the
//! sidecar's parquet footer. Facts that used to live in the footer are recovered elsewhere:
//! - `num_granules` — derived from `block_rows` and `granule_rows`;
//! - `granule_rows` — recorded once in `GranuleIndexLayout`;
//! - cluster-key element types — from the table schema (cluster-key ids match the block);
//! - each leaf column's dict-page / chunk-end — from `BlockMeta.col_metas` (`offset`, `len`).

use std::collections::HashMap;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt64Type;
use databend_storages_common_blocks::LeafPageLayout;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_io::MergeIOReader;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BytesRange;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::GranuleIndexFileLayout;
use databend_storages_common_table_meta::meta::GranuleIndexLayout;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::table::TableCompression;
use opendal::Buffer;
use opendal::Operator;

use crate::io::DataItem;
use crate::io::read::column_chunks_to_record_batch;

/// Prefix of a per-cluster-key-element granule-min column name (`m{i}`).
pub const GRANULE_INDEX_MIN_COL_PREFIX: &str = "m";

/// Prefix of a per-leaf-column offset column name (`g_{column_id}`).
pub const GRANULE_INDEX_OFFSET_COL_PREFIX: &str = "g_";

/// One serialized sidecar file: the raw parquet bytes to write plus the block-meta layout
/// (location, size, per-column byte ranges) that lets the read path decode it without a footer.
#[derive(Debug, Clone)]
pub struct GranuleIndexFileState {
    pub data: Buffer,
    pub layout: GranuleIndexFileLayout,
}

/// Both sidecar files for one block, ready to persist, plus the granularity to record in block meta.
#[derive(Debug, Clone)]
pub struct GranuleIndexState {
    pub granule_rows: u32,
    /// The cluster-key mins file. `None` for an offset-only index (table without a cluster key).
    pub mins: Option<GranuleIndexFileState>,
    /// The per-granule page-offset file (always produced when the block spans >= 2 granules).
    pub offsets: GranuleIndexFileState,
}

impl GranuleIndexState {
    /// The block-meta layout describing both files.
    pub fn layout(&self) -> GranuleIndexLayout {
        GranuleIndexLayout {
            granule_rows: self.granule_rows,
            mins: self.mins.as_ref().map(|f| f.layout.clone()),
            offsets: self.offsets.layout.clone(),
        }
    }
}

/// Builds the two sparse-granule-index sidecar files for one block from the writer's captured page
/// layout plus the per-granule cluster-key mins.
pub struct GranuleIndexWriter {
    /// `Some` when the table has a cluster key (mins are recorded for pruning); `None` for an
    /// offset-only index (page byte offsets only, no mins file).
    cluster_key_id: Option<u32>,
    granule_rows: usize,
    /// Leaf column ids in parquet leaf order — `page_layout[i]` describes `leaf_column_ids[i]`.
    leaf_column_ids: Vec<ColumnId>,
}

impl GranuleIndexWriter {
    pub fn new(
        cluster_key_id: Option<u32>,
        granule_rows: usize,
        leaf_column_ids: Vec<ColumnId>,
    ) -> Self {
        Self {
            cluster_key_id,
            granule_rows,
            leaf_column_ids,
        }
    }

    /// Assemble and serialize both sidecar files. `granule_mins` is empty for an offset-only index
    /// (no cluster key), in which case no mins file is produced. `extra_fields`/`extra_columns`
    /// (paired) append extra per-granule columns to the *offsets* file for granule-level index
    /// implementations (e.g. bloom payload offsets); empty for a plain sparse index.
    ///
    /// `mins_location` / `offsets_location` are where the two files will be written.
    pub fn build_with_extra_columns(
        &self,
        page_layout: &[LeafPageLayout],
        granule_mins: &[Scalar],
        mins_location: Location,
        offsets_location: Location,
        extra_fields: Vec<TableField>,
        extra_columns: Vec<Column>,
    ) -> Result<GranuleIndexState> {
        if page_layout.len() != self.leaf_column_ids.len() {
            return Err(ErrorCode::Internal(format!(
                "granule index: layout leaves {} != leaf column ids {}",
                page_layout.len(),
                self.leaf_column_ids.len()
            )));
        }

        let has_mins = !granule_mins.is_empty();
        // Offset-only indexes carry no mins; the granule count then comes from the page layout
        // (a page starts on every granule boundary, so page count == granule count).
        let num_granules = if has_mins {
            granule_mins.len()
        } else {
            page_layout
                .first()
                .map(|leaf| leaf.data_pages.len())
                .unwrap_or(0)
        };
        if num_granules == 0 {
            return Err(ErrorCode::Internal(
                "granule index build called with zero granules",
            ));
        }

        // --- offsets file: one `g_{column_id}` UInt64 column per leaf, plus extra columns ---
        let mut offset_fields = Vec::with_capacity(self.leaf_column_ids.len() + extra_fields.len());
        let mut offset_columns =
            Vec::with_capacity(self.leaf_column_ids.len() + extra_columns.len());
        for (leaf_idx, leaf) in page_layout.iter().enumerate() {
            let column_id = self.leaf_column_ids[leaf_idx];
            let offsets = self.granule_offsets(leaf, num_granules)?;
            let name = format!("{GRANULE_INDEX_OFFSET_COL_PREFIX}{column_id}");
            offset_fields.push(TableField::new(
                &name,
                TableDataType::Number(NumberDataType::UInt64),
            ));
            offset_columns.push(UInt64Type::from_data(offsets));
        }
        for (field, column) in extra_fields.into_iter().zip(extra_columns.into_iter()) {
            offset_fields.push(field);
            offset_columns.push(column);
        }
        let offsets = serialize_sidecar(offset_fields, offset_columns, offsets_location)?;

        // --- mins file: one `m{i}` native column per cluster-key element ---
        let mins = if has_mins {
            let (types, columns) = build_min_columns(granule_mins)?;
            let mut fields = Vec::with_capacity(columns.len());
            for (i, ty) in types.iter().enumerate() {
                let name = format!("{GRANULE_INDEX_MIN_COL_PREFIX}{i}");
                fields.push(TableField::new(&name, infer_schema_type(ty)?));
            }
            Some(serialize_sidecar(fields, columns, mins_location)?)
        } else {
            None
        };

        // `cluster_key_id` currently only affects whether a mins file exists; it is validated at
        // prune time against `cluster_stats.cluster_key_id`, not stored here.
        let _ = self.cluster_key_id;

        Ok(GranuleIndexState {
            granule_rows: self.granule_rows as u32,
            mins,
            offsets,
        })
    }

    /// For one leaf, pick the absolute offset of the first data page at each granule boundary.
    /// `flush_page` guarantees a page starts exactly on every granule boundary row, so the page
    /// whose `first_row_index == g * granule_rows` always exists.
    fn granule_offsets(&self, leaf: &LeafPageLayout, num_granules: usize) -> Result<Vec<u64>> {
        let by_row: HashMap<u64, u64> = leaf
            .data_pages
            .iter()
            .map(|p| (p.first_row_index, p.offset))
            .collect();
        let mut offsets = Vec::with_capacity(num_granules);
        for g in 0..num_granules {
            let boundary = (g * self.granule_rows) as u64;
            let off = by_row.get(&boundary).copied().ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "granule index: no page starts at granule boundary row {boundary}"
                ))
            })?;
            offsets.push(off);
        }
        Ok(offsets)
    }
}

/// Serialize `columns` (each `num_granules` rows) to one uncompressed, dictionary-free parquet
/// sidecar and capture, per logical column, the byte range of its parquet chunk. The read path
/// uses those ranges to fetch and decode each column from raw bytes without parsing the footer.
///
/// Column order in `fields`/`columns` matches the parquet leaf order, which is exactly the order
/// `row_group.columns()` reports, so field name `i` maps to chunk `i`.
fn serialize_sidecar(
    fields: Vec<TableField>,
    columns: Vec<Column>,
    location: Location,
) -> Result<GranuleIndexFileState> {
    let schema = TableSchema::new(fields.clone());
    let block = DataBlock::new_from_columns(columns);
    let serialized = blocks_to_parquet(&schema, vec![block], TableCompression::None, false, None)?;
    let size = serialized.len() as u64;

    let row_group = &serialized.metadata.row_groups()[0];
    if row_group.columns().len() != fields.len() {
        return Err(ErrorCode::Internal(format!(
            "granule index sidecar: parquet has {} chunks but {} fields",
            row_group.columns().len(),
            fields.len()
        )));
    }
    let mut columns_layout: HashMap<String, Vec<BytesRange>> = HashMap::with_capacity(fields.len());
    for (field, chunk_meta) in fields.iter().zip(row_group.columns().iter()) {
        let (offset, len) = chunk_meta.byte_range();
        // Forward-looking shape: today one chunk per column; page-splitting will push several.
        columns_layout.insert(field.name().clone(), vec![BytesRange { offset, len }]);
    }

    let data = Buffer::from(serialized.payload);
    Ok(GranuleIndexFileState {
        data,
        layout: GranuleIndexFileLayout {
            location,
            size,
            columns: columns_layout,
        },
    })
}

/// Split per-granule cluster-key min tuples into one native column per tuple element. Returns the
/// element types (tuple order) and the built columns. Element types are inferred from the first
/// granule's tuple; every granule must share the same arity.
fn build_min_columns(granule_mins: &[Scalar]) -> Result<(Vec<DataType>, Vec<Column>)> {
    let first = granule_mins[0]
        .as_tuple()
        .ok_or_else(|| ErrorCode::Internal("granule index: granule min must be a tuple scalar"))?;
    let arity = first.len();

    // Infer each element's type from the first granule. A min can be NULL (cluster key nullable),
    // so wrap every element type as nullable to give the builder a stable, decodable type.
    let cluster_key_types: Vec<DataType> = first
        .iter()
        .map(|s| s.as_ref().infer_data_type().wrap_nullable())
        .collect();

    let mut builders: Vec<ColumnBuilder> = cluster_key_types
        .iter()
        .map(|ty| ColumnBuilder::with_capacity(ty, granule_mins.len()))
        .collect();

    for m in granule_mins {
        let tuple = m.as_tuple().ok_or_else(|| {
            ErrorCode::Internal("granule index: granule min must be a tuple scalar")
        })?;
        if tuple.len() != arity {
            return Err(ErrorCode::Internal(format!(
                "granule index: granule min arity {} != expected {arity}",
                tuple.len()
            )));
        }
        for (i, elem) in tuple.iter().enumerate() {
            builders[i].push(elem.as_ref());
        }
    }

    let columns = builders.into_iter().map(|b| b.build()).collect();
    Ok((cluster_key_types, columns))
}

// ============================ read path ============================

use std::ops::Range;

/// Number of granules a block of `block_rows` rows produces at `granule_rows` granularity. The last
/// granule may be short. Used to size sidecar decode and bound the last granule's row/byte range.
pub fn num_granules_of(block_rows: usize, granule_rows: usize) -> usize {
    if granule_rows == 0 {
        return 0;
    }
    block_rows.div_ceil(granule_rows)
}

/// Fetch the raw parquet-chunk bytes of the named sidecar columns via a single merged IO over the
/// byte ranges recorded in `layout` (no footer parse). Columns absent from `layout` are skipped
/// (returned map simply lacks them), so callers tolerate older sidecars missing a column.
async fn fetch_sidecar_columns(
    dal: &Operator,
    settings: &ReadSettings,
    layout: &GranuleIndexFileLayout,
    names: &[String],
) -> Result<HashMap<String, Buffer>> {
    let mut ranges: Vec<(ColumnId, Range<u64>)> = Vec::new();
    // synthetic id -> (name, page_index_within_column)
    let mut plan: Vec<(u32, String)> = Vec::new();
    let mut next_id: u32 = 0;
    for name in names {
        let Some(spans) = layout.columns.get(name) else {
            continue;
        };
        for span in spans {
            ranges.push((next_id, span.offset..span.offset + span.len));
            plan.push((next_id, name.clone()));
            next_id += 1;
        }
    }
    if ranges.is_empty() {
        return Ok(HashMap::new());
    }

    let read =
        MergeIOReader::merge_io_read(settings, dal.clone(), &layout.location.0, &ranges).await?;

    // Reassemble each column's pages (in order) into one contiguous chunk buffer.
    let mut per_column: HashMap<String, Vec<u8>> = HashMap::new();
    for (synth_id, name) in &plan {
        let (chunk_idx, byte_range) =
            read.columns_chunk_offsets.get(synth_id).ok_or_else(|| {
                ErrorCode::Internal(format!("granule index sidecar missing range {synth_id}"))
            })?;
        let chunk = read
            .owner_memory
            .get_chunk(*chunk_idx, &layout.location.0)?;
        let bytes = chunk.slice(byte_range.clone());
        per_column
            .entry(name.clone())
            .or_default()
            .extend_from_slice(&bytes.to_bytes());
    }
    Ok(per_column
        .into_iter()
        .map(|(k, v)| (k, Buffer::from(v)))
        .collect())
}

/// Decode a single-column parquet chunk (raw bytes, no footer) of `num_rows` rows and type `ty`.
fn decode_single_column(bytes: Buffer, ty: &DataType, num_rows: usize) -> Result<Column> {
    let field = TableField::new("c", infer_schema_type(ty)?);
    let schema = TableSchema::new(vec![field]);
    let mut chunks: HashMap<ColumnId, DataItem> = HashMap::new();
    chunks.insert(0, DataItem::RawData(bytes));
    let batch =
        column_chunks_to_record_batch(&schema, num_rows, &chunks, &Compression::None, None)?;
    Column::from_arrow_rs(batch.column(0).clone(), ty)
}

/// Decode a `UInt64` sidecar column to a `Vec<u64>`.
fn decode_u64_column(bytes: Buffer, num_rows: usize) -> Result<Vec<u64>> {
    let ty = DataType::Number(NumberDataType::UInt64);
    let column = decode_single_column(bytes, &ty, num_rows)?;
    let mut out = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        match column.index(i) {
            Some(databend_common_expression::ScalarRef::Number(
                databend_common_expression::types::number::NumberScalar::UInt64(v),
            )) => out.push(v),
            other => {
                return Err(ErrorCode::Internal(format!(
                    "granule index: expected u64 at {i}, got {other:?}"
                )));
            }
        }
    }
    Ok(out)
}

/// Load named `UInt64` sidecar columns from `layout` via the recorded byte ranges (no footer).
/// Each requested column that exists is decoded to a `Vec<u64>` of `num_rows`; columns absent from
/// `layout` (e.g. an older sidecar predating the index) are simply missing from the result, letting
/// callers skip them. Used by granule-level indexes (e.g. bloom) to recover their offset columns.
pub async fn load_u64_columns(
    dal: &Operator,
    settings: &ReadSettings,
    layout: &GranuleIndexFileLayout,
    names: &[String],
    num_rows: usize,
) -> Result<HashMap<String, Vec<u64>>> {
    let mut buffers = fetch_sidecar_columns(dal, settings, layout, names).await?;
    let mut out = HashMap::with_capacity(names.len());
    for name in names {
        if let Some(bytes) = buffers.remove(name) {
            out.insert(name.clone(), decode_u64_column(bytes, num_rows)?);
        }
    }
    Ok(out)
}

/// Load the per-granule cluster-key mins from the mins sidecar. `element_types` are the cluster-key
/// element types (from the table schema, since the block's cluster key matches the table's), in
/// tuple order — this is what replaces the old footer `cluster_key_types`.
pub async fn load_granule_mins(
    dal: &Operator,
    settings: &ReadSettings,
    layout: &GranuleIndexFileLayout,
    element_types: &[DataType],
    num_granules: usize,
) -> Result<Vec<Scalar>> {
    let names: Vec<String> = (0..element_types.len())
        .map(|i| format!("{GRANULE_INDEX_MIN_COL_PREFIX}{i}"))
        .collect();
    let mut buffers = fetch_sidecar_columns(dal, settings, layout, &names).await?;

    let mut element_cols: Vec<Column> = Vec::with_capacity(element_types.len());
    for (i, ty) in element_types.iter().enumerate() {
        let name = &names[i];
        let bytes = buffers.remove(name).ok_or_else(|| {
            ErrorCode::Internal(format!("granule index mins missing column {name}"))
        })?;
        // Mins were written with the nullable-wrapped element type.
        element_cols.push(decode_single_column(
            bytes,
            &ty.wrap_nullable(),
            num_granules,
        )?);
    }

    Ok((0..num_granules)
        .map(|g| {
            let elems = element_cols
                .iter()
                .map(|col| col.index(g).unwrap().to_owned())
                .collect();
            Scalar::Tuple(elems)
        })
        .collect())
}

/// A narrowed read plan for one leaf column, derived from a contiguous run of selected granules.
/// The column's partial chunk is reconstructed as `[dict bytes] ++ [data bytes]`, a valid standalone
/// parquet column-chunk layout the page reader can decode on its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnReadPlan {
    pub column_id: ColumnId,
    /// Dictionary page byte range, if the column chunk is dict-encoded. Always read when present.
    pub dict_range: Option<Range<u64>>,
    /// Contiguous data-page byte range covering exactly the selected granules.
    pub data_range: Range<u64>,
}

/// A whole-block narrowed read plan: per-column byte ranges plus the row count the reconstructed
/// partial chunks contain (identical across columns, so rows stay aligned).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockReadPlan {
    pub columns: Vec<ColumnReadPlan>,
    pub num_rows: usize,
    pub start_row: usize,
}

/// The decoded per-granule page offsets for a block's projected leaf columns, loaded from the
/// offsets sidecar file. Combined with `col_metas` (each column's chunk `[offset, offset+len)`) it
/// yields narrowed byte-range read plans without reading any footer:
/// - `chunk_start = col_metas[id].offset`, `chunk_end = offset + len`;
/// - `dict_range = chunk_start .. offsets[id][0]` (empty when the column has no dictionary page);
/// - a granule sub-run `[s, e)` reads `data_range = offsets[id][s] .. (offsets[id][e] or chunk_end)`.
pub struct OffsetsIndex {
    granule_rows: usize,
    /// leaf column id -> per-granule first-data-page absolute offset (length == num_granules).
    offsets: HashMap<ColumnId, Vec<u64>>,
}

impl OffsetsIndex {
    /// Load the offsets for `column_ids` from the offsets sidecar (partial reads over the byte
    /// ranges recorded in `layout`; no footer parse). `block_rows`/`granule_rows` derive the
    /// granule count.
    pub async fn load(
        dal: &Operator,
        settings: &ReadSettings,
        layout: &GranuleIndexFileLayout,
        granule_rows: usize,
        block_rows: usize,
        column_ids: &[ColumnId],
    ) -> Result<Self> {
        let num_granules = num_granules_of(block_rows, granule_rows);
        let names: Vec<String> = column_ids
            .iter()
            .map(|id| format!("{GRANULE_INDEX_OFFSET_COL_PREFIX}{id}"))
            .collect();
        let mut buffers = fetch_sidecar_columns(dal, settings, layout, &names).await?;
        let mut offsets = HashMap::with_capacity(column_ids.len());
        for (id, name) in column_ids.iter().zip(names.iter()) {
            if let Some(bytes) = buffers.remove(name) {
                offsets.insert(*id, decode_u64_column(bytes, num_granules)?);
            }
        }
        Ok(OffsetsIndex {
            granule_rows,
            offsets,
        })
    }

    fn plan_for_sub_run(
        &self,
        col_metas: &HashMap<ColumnId, ColumnMeta>,
        s: usize,
        e: usize,
        block_rows: usize,
    ) -> BlockReadPlan {
        let run_start_row = s * self.granule_rows;
        let run_end_row = (e * self.granule_rows).min(block_rows);
        let num_rows = run_end_row.saturating_sub(run_start_row);

        let mut columns = Vec::with_capacity(self.offsets.len());
        for (column_id, offs) in &self.offsets {
            let Some(meta) = col_metas.get(column_id) else {
                continue;
            };
            let (chunk_start, chunk_len) = meta.offset_length();
            let chunk_end = chunk_start + chunk_len;
            let data_start = offs[s];
            let data_end = if e < offs.len() { offs[e] } else { chunk_end };
            // The bytes before the first data page are the dictionary page (if any); when the
            // column has no dict, the first data page starts at the chunk start and this is empty.
            let dict_range = if offs[0] > chunk_start {
                Some(chunk_start..offs[0])
            } else {
                None
            };
            columns.push(ColumnReadPlan {
                column_id: *column_id,
                dict_range,
                data_range: data_start..data_end,
            });
        }
        BlockReadPlan {
            columns,
            num_rows,
            start_row: run_start_row,
        }
    }

    fn empty_plan(&self, col_metas: &HashMap<ColumnId, ColumnMeta>) -> BlockReadPlan {
        let columns = self
            .offsets
            .keys()
            .filter(|id| col_metas.contains_key(id))
            .map(|column_id| ColumnReadPlan {
                column_id: *column_id,
                dict_range: None,
                data_range: 0..0,
            })
            .collect();
        BlockReadPlan {
            columns,
            num_rows: 0,
            start_row: 0,
        }
    }

    /// Build one read plan per sub-run, splitting each surviving granule run so no plan covers more
    /// than `max_block_rows` rows. Empty input (every granule pruned) yields a single zero-row plan.
    pub fn read_plans_for_ranges(
        &self,
        col_metas: &HashMap<ColumnId, ColumnMeta>,
        ranges: &[Range<usize>],
        block_rows: usize,
        max_block_rows: usize,
    ) -> Vec<BlockReadPlan> {
        if ranges.is_empty() {
            return vec![self.empty_plan(col_metas)];
        }
        let granules_per_plan = (max_block_rows / self.granule_rows.max(1)).max(1);
        let mut plans = Vec::new();
        for range in ranges {
            let mut s = range.start;
            while s < range.end {
                let e = (s + granules_per_plan).min(range.end);
                plans.push(self.plan_for_sub_run(col_metas, s, e, block_rows));
                s = e;
            }
        }
        plans
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::NumberScalar;
    use databend_storages_common_blocks::DataPageOffset;
    use databend_storages_common_table_meta::meta::SingleColumnMeta;
    use opendal::Operator;
    use opendal::services::Memory;

    use super::*;

    fn layout(dict: Option<(u64, u64)>, chunk_end: u64, pages: &[(u64, u64)]) -> LeafPageLayout {
        LeafPageLayout {
            dict_page: dict,
            chunk_end,
            data_pages: pages
                .iter()
                .map(|&(first_row_index, offset)| DataPageOffset {
                    first_row_index,
                    offset,
                })
                .collect(),
        }
    }

    fn test_settings() -> ReadSettings {
        ReadSettings {
            max_gap_size: 48,
            max_range_size: 1024 * 1024,
            parquet_fast_read_bytes: 0,
        }
    }

    async fn persist(op: &Operator, state: &GranuleIndexState) {
        if let Some(mins) = &state.mins {
            op.write(&mins.layout.location.0, mins.data.to_bytes())
                .await
                .unwrap();
        }
        op.write(
            &state.offsets.layout.location.0,
            state.offsets.data.to_bytes(),
        )
        .await
        .unwrap();
    }

    // Build both sidecar files for 3 granules over 2 leaf columns (one dict-encoded), persist them
    // to an in-memory operator, then read mins and offsets back via the recorded byte ranges (no
    // footer) and confirm they round-trip exactly.
    #[tokio::test]
    async fn test_two_file_roundtrip() {
        let granule_rows = 100;
        // Column 7: no dict page; pages start at granule rows 0/100/200, plus interior splits.
        let leaf_a = layout(None, 1000, &[(0, 100), (50, 150), (100, 260), (200, 480)]);
        // Column 9: dict page [10,40) contiguous with first data page at 50.
        let leaf_b = layout(Some((10, 40)), 2000, &[(0, 50), (100, 600), (200, 1500)]);
        let page_layout = vec![leaf_a, leaf_b];

        let granule_mins = vec![
            Scalar::Tuple(vec![Scalar::Number(NumberScalar::Int64(0))]),
            Scalar::Tuple(vec![Scalar::Number(NumberScalar::Int64(100))]),
            Scalar::Tuple(vec![Scalar::Number(NumberScalar::Int64(200))]),
        ];

        let builder = GranuleIndexWriter::new(Some(42), granule_rows, vec![7, 9]);
        let state = builder
            .build_with_extra_columns(
                &page_layout,
                &granule_mins,
                ("mins.parquet".to_string(), 0),
                ("offs.parquet".to_string(), 0),
                vec![],
                vec![],
            )
            .unwrap();

        let op = Operator::new(Memory::default()).unwrap().finish();
        persist(&op, &state).await;
        let settings = test_settings();
        let layout = state.layout();

        // Mins round-trip: the block's cluster-key element type is Int64 (nullable at load time).
        let element_types = vec![DataType::Number(NumberDataType::Int64)];
        let mins = load_granule_mins(
            &op,
            &settings,
            layout.mins.as_ref().unwrap(),
            &element_types,
            3,
        )
        .await
        .unwrap();
        assert_eq!(mins, granule_mins);

        // Offsets round-trip: g_7 / g_9 first-data-page offsets at granule boundaries 0/100/200.
        let offsets =
            OffsetsIndex::load(&op, &settings, &layout.offsets, granule_rows, 300, &[7, 9])
                .await
                .unwrap();
        assert_eq!(offsets.offsets.get(&7).unwrap(), &vec![100, 260, 480]);
        assert_eq!(offsets.offsets.get(&9).unwrap(), &vec![50, 600, 1500]);
    }

    // Byte-range plan: chunk boundaries come from col_metas, dict range from the gap before the
    // first data page, data range from the offsets (last granule bounded by chunk_end).
    #[test]
    fn test_plan_for_sub_run() {
        let mut offsets = HashMap::new();
        offsets.insert(7u32, vec![100u64, 260, 480]);
        offsets.insert(9u32, vec![50u64, 600, 1500]);
        let index = OffsetsIndex {
            granule_rows: 100,
            offsets,
        };

        let mut col_metas = HashMap::new();
        // Column 7 chunk [100, 1000): no dict (first data page == chunk start).
        col_metas.insert(
            7u32,
            ColumnMeta::Parquet(SingleColumnMeta {
                offset: 100,
                len: 900,
                num_values: 300,
            }),
        );
        // Column 9 chunk [10, 2000): dict page occupies [10, 50) before the first data page.
        col_metas.insert(
            9u32,
            ColumnMeta::Parquet(SingleColumnMeta {
                offset: 10,
                len: 1990,
                num_values: 300,
            }),
        );

        // Sub-run [1, 3): granules 1 and 2, rows 100..300.
        let plan = index.plan_for_sub_run(&col_metas, 1, 3, 300);
        assert_eq!(plan.start_row, 100);
        assert_eq!(plan.num_rows, 200);

        let get = |id: u32| plan.columns.iter().find(|c| c.column_id == id).unwrap();
        let c7 = get(7);
        assert_eq!(c7.dict_range, None);
        assert_eq!(c7.data_range, 260..1000); // last granule bounded by chunk_end
        let c9 = get(9);
        assert_eq!(c9.dict_range, Some(10..50));
        assert_eq!(c9.data_range, 600..2000);
    }
}
