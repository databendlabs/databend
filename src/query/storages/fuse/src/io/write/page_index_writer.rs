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

//! Sparse page index: a ClickHouse-style mark file mapping cluster-key granules to the physical
//! page byte ranges of each leaf column. There is no dedicated in-memory struct — the index is a
//! columnar parquet file (one row per granule) that the read path decodes straight into a
//! `DataBlock`. This module builds that file on the write path.
//!
//! On-disk columnar schema (one row per granule):
//! - `m{i}` (i in 0..cluster_key_arity) : the i-th cluster-key element's min at the granule start,
//!   stored as a native nullable column of the element's own type. Decoded rows are zipped back
//!   into a `Scalar::Tuple` so the prune-time evaluator sees the same shape as before.
//! - `g_{leaf_column_id}`  : UInt64, that leaf column's first-data-page absolute byte offset for
//!   the granule. A granule g's data range for a column is `[off[g], off[g+1])`, the last granule
//!   bounded by that column's `chunk_end` (carried in the footer metadata).
//! - `active`              : Boolean, written all-true. Reserved so a future granule deletion can
//!   mark a granule inactive by rewriting only this tiny sidecar, never the block data file.
//!
//! Footer key-value metadata (small fixed scalars, keyed `databend_page_index`): granule_rows,
//! cluster_key_id, the ordered cluster-key element types, the ordered indexed leaf column ids, and
//! per-column dict-page range + chunk_end.

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
use databend_storages_common_blocks::LeafPageLayout;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::table::TableCompression;
use opendal::Buffer;
use opendal::Operator;
use parquet::file::metadata::KeyValue;
use serde::Deserialize;
use serde::Serialize;

/// Footer key under which the page-index scalars are embedded in the index parquet file.
const PAGE_INDEX_META_KEY: &str = "databend_page_index";

/// Prefix of a per-cluster-key-element granule-min column name in the index file (`m{i}`).
const PAGE_INDEX_MIN_COL_PREFIX: &str = "m";

/// Prefix of a per-leaf-column offset column name in the index file (`g_{column_id}`).
const PAGE_INDEX_OFFSET_COL_PREFIX: &str = "g_";

/// Column name of the reserved per-granule `active` flag. Written all-true today; reserved so a
/// future granule deletion can flip a granule to inactive by rewriting only this tiny sidecar file,
/// leaving the (large) block data file untouched. An absent column (older index files) reads as
/// all-active.
const PAGE_INDEX_ACTIVE_COL: &str = "active";

/// Fixed per-column scalars stored in the index file's footer metadata (not columnar).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnPageMeta {
    pub column_id: ColumnId,
    /// Dictionary page absolute `(offset, len)`, present iff the column chunk is dict-encoded.
    /// The read path must always fetch this to decode any of the column's data pages.
    pub dict_page: Option<(u64, u64)>,
    /// End offset of the column chunk's data pages; bounds the last granule's data range.
    pub chunk_end: u64,
}

/// The scalars embedded in the index file footer; together with the columnar data they fully
/// describe the sparse index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageIndexMeta {
    pub cluster_key_id: u32,
    pub granule_rows: u32,
    pub num_granules: u32,
    /// Cluster-key element types, in tuple order. Drives how the `m{i}` granule-min columns are
    /// built (write) and decoded back into `Scalar::Tuple` elements (read).
    pub cluster_key_types: Vec<DataType>,
    /// Indexed leaf columns, in the same order as the `g_{column_id}` columns appear.
    pub columns: Vec<ColumnPageMeta>,
}

#[derive(Debug, Clone)]
pub struct PageIndexState {
    pub location: Location,
    pub size: u64,
    pub data: Buffer,
}

/// Builds the sparse page index for one block from the writer's captured page layout plus the
/// per-granule cluster-key mins, and serializes it to the columnar parquet index file.
pub struct PageIndexBuilder {
    cluster_key_id: u32,
    granule_rows: usize,
    /// Leaf column ids in parquet leaf order — `page_layout[i]` describes `leaf_column_ids[i]`.
    leaf_column_ids: Vec<ColumnId>,
}

impl PageIndexBuilder {
    pub fn new(cluster_key_id: u32, granule_rows: usize, leaf_column_ids: Vec<ColumnId>) -> Self {
        Self {
            cluster_key_id,
            granule_rows,
            leaf_column_ids,
        }
    }

    /// Assemble and serialize the index file.
    ///
    /// * `page_layout` — per-leaf page layout (absolute offsets), in parquet leaf order.
    /// * `granule_mins` — cluster-key min Scalar (a tuple) at the start of each granule.
    /// * `location` — pre-generated index file location.
    pub fn build(
        &self,
        page_layout: &[LeafPageLayout],
        granule_mins: &[Scalar],
        location: Location,
    ) -> Result<PageIndexState> {
        let num_granules = granule_mins.len();
        if num_granules == 0 {
            return Err(ErrorCode::Internal(
                "page index build called with zero granules",
            ));
        }
        if page_layout.len() != self.leaf_column_ids.len() {
            return Err(ErrorCode::Internal(format!(
                "page index: layout leaves {} != leaf column ids {}",
                page_layout.len(),
                self.leaf_column_ids.len()
            )));
        }

        // Granule mins are cluster-key tuples; split them into one native column per element.
        let (cluster_key_types, min_columns) = build_min_columns(granule_mins)?;

        // Per-leaf offset columns + footer scalars.
        let mut offset_columns: Vec<Vec<u64>> = Vec::with_capacity(page_layout.len());
        let mut columns_meta: Vec<ColumnPageMeta> = Vec::with_capacity(page_layout.len());
        for (leaf_idx, leaf) in page_layout.iter().enumerate() {
            let column_id = self.leaf_column_ids[leaf_idx];
            let offsets = self.granule_offsets(leaf, num_granules)?;
            offset_columns.push(offsets);
            columns_meta.push(ColumnPageMeta {
                column_id,
                dict_page: leaf.dict_page,
                chunk_end: leaf.chunk_end,
            });
        }

        let meta = PageIndexMeta {
            cluster_key_id: self.cluster_key_id,
            granule_rows: self.granule_rows as u32,
            num_granules: num_granules as u32,
            cluster_key_types,
            columns: columns_meta,
        };

        let (schema, block) = build_index_block(min_columns, &offset_columns, &meta)?;
        let kv = KeyValue {
            key: PAGE_INDEX_META_KEY.to_string(),
            value: Some(serde_json::to_string(&meta).map_err(|e| {
                ErrorCode::Internal(format!("failed to encode page index meta: {e}"))
            })?),
        };

        let serialized = blocks_to_parquet(
            &schema,
            vec![block],
            TableCompression::None,
            false,
            Some(vec![kv]),
        )?;
        let size = serialized.len() as u64;
        let data = Buffer::from(serialized.payload);
        Ok(PageIndexState {
            location,
            size,
            data,
        })
    }

    /// For one leaf, pick the absolute offset of the first data page at each granule boundary.
    /// `flush_page` guarantees a page starts exactly on every granule boundary row, so the page
    /// whose `first_row_index == g * granule_rows` always exists.
    fn granule_offsets(&self, leaf: &LeafPageLayout, num_granules: usize) -> Result<Vec<u64>> {
        use std::collections::HashMap;
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
                    "page index: no page starts at granule boundary row {boundary}"
                ))
            })?;
            offsets.push(off);
        }
        Ok(offsets)
    }
}

/// Split per-granule cluster-key min tuples into one native column per tuple element. Returns the
/// element types (tuple order) and the built columns. Element types are inferred from the first
/// granule's tuple; every granule must share the same arity.
fn build_min_columns(granule_mins: &[Scalar]) -> Result<(Vec<DataType>, Vec<Column>)> {
    let first = granule_mins[0]
        .as_tuple()
        .ok_or_else(|| ErrorCode::Internal("page index: granule min must be a tuple scalar"))?;
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
        let tuple = m
            .as_tuple()
            .ok_or_else(|| ErrorCode::Internal("page index: granule min must be a tuple scalar"))?;
        if tuple.len() != arity {
            return Err(ErrorCode::Internal(format!(
                "page index: granule min arity {} != expected {arity}",
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

fn build_index_block(
    min_columns: Vec<Column>,
    offset_columns: &[Vec<u64>],
    meta: &PageIndexMeta,
) -> Result<(TableSchema, DataBlock)> {
    let num_granules = meta.num_granules as usize;
    let mut fields = Vec::with_capacity(min_columns.len() + offset_columns.len() + 1);
    let mut columns = Vec::with_capacity(min_columns.len() + offset_columns.len() + 1);

    for (i, (col, ty)) in min_columns
        .iter()
        .zip(meta.cluster_key_types.iter())
        .enumerate()
    {
        let name = format!("{PAGE_INDEX_MIN_COL_PREFIX}{i}");
        fields.push(TableField::new(&name, infer_schema_type(ty)?));
        columns.push(col.clone());
    }

    for (offsets, col_meta) in offset_columns.iter().zip(meta.columns.iter()) {
        let name = format!("{PAGE_INDEX_OFFSET_COL_PREFIX}{}", col_meta.column_id);
        fields.push(TableField::new(
            &name,
            TableDataType::Number(NumberDataType::UInt64),
        ));
        columns.push(databend_common_expression::types::UInt64Type::from_data(
            offsets.clone(),
        ));
    }

    // Reserved `active` flag: written all-true. A future granule deletion can flip entries here and
    // rewrite only this sidecar, never touching the block data file.
    fields.push(TableField::new(
        PAGE_INDEX_ACTIVE_COL,
        TableDataType::Boolean,
    ));
    columns.push(databend_common_expression::types::BooleanType::from_data(
        vec![true; num_granules],
    ));

    let schema = TableSchema::new(fields);
    let block = DataBlock::new_from_columns(columns);
    Ok((schema, block))
}

/// The decoded sparse page index for one block: granule cluster-key mins plus, per indexed leaf
/// column, the absolute byte offset at which each granule's data starts. Decoded from the sidecar
/// parquet index file written by [`PageIndexBuilder`].
#[derive(Debug, Clone)]
pub struct PageIndex {
    pub meta: PageIndexMeta,
    /// Decoded cluster-key min Scalar per granule (granule order).
    pub granule_mins: Vec<Scalar>,
    /// Per indexed leaf column (same order as `meta.columns`): the absolute start offset of each
    /// granule's first data page. Length == num_granules.
    pub offsets: Vec<Vec<u64>>,
    /// Per-granule active flag (granule order). All-true for freshly written indexes; reserved so a
    /// future granule deletion can mark granules inactive by rewriting only this sidecar. Index
    /// files written before this column existed decode as all-active. Not consumed by the read path
    /// yet — kept so granule deletion can be added without another on-disk format change.
    #[allow(dead_code)]
    pub active: Vec<bool>,
}

impl PageIndex {
    /// Decode a page index from the raw sidecar parquet bytes.
    pub fn decode(data: &[u8]) -> Result<Self> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let bytes = bytes::Bytes::copy_from_slice(data);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| ErrorCode::StorageOther(format!("failed to open page index file: {e}")))?;

        let meta_json = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .and_then(|kvs| kvs.iter().find(|kv| kv.key == PAGE_INDEX_META_KEY))
            .and_then(|kv| kv.value.clone())
            .ok_or_else(|| {
                ErrorCode::StorageOther("page index file missing footer metadata".to_string())
            })?;
        let meta: PageIndexMeta = serde_json::from_str(&meta_json).map_err(|e| {
            ErrorCode::StorageOther(format!("failed to decode page index meta: {e}"))
        })?;

        let mut reader = builder
            .with_batch_size(usize::MAX)
            .build()
            .map_err(|e| ErrorCode::StorageOther(format!("failed to read page index: {e}")))?;
        let batch = reader
            .next()
            .ok_or_else(|| ErrorCode::StorageOther("empty page index file".to_string()))?
            .map_err(|e| {
                ErrorCode::StorageOther(format!("failed to read page index batch: {e}"))
            })?;

        // Per-element granule-min columns `m{i}`, decoded with their stored types and zipped back
        // into one tuple Scalar per granule.
        let mut min_element_cols: Vec<Column> = Vec::with_capacity(meta.cluster_key_types.len());
        for (i, ty) in meta.cluster_key_types.iter().enumerate() {
            let name = format!("{PAGE_INDEX_MIN_COL_PREFIX}{i}");
            let arr = batch.column_by_name(&name).ok_or_else(|| {
                ErrorCode::StorageOther(format!("page index missing min column {name}"))
            })?;
            let col = Column::from_arrow_rs(arr.clone(), ty).map_err(|e| {
                ErrorCode::StorageOther(format!(
                    "failed to decode page index min column {name}: {e}"
                ))
            })?;
            min_element_cols.push(col);
        }
        let granule_mins = zip_granule_mins(&min_element_cols, meta.num_granules as usize);

        // One UInt64 offset column per indexed leaf, keyed `g_{column_id}`.
        let mut offsets = Vec::with_capacity(meta.columns.len());
        for col in &meta.columns {
            let name = format!("{PAGE_INDEX_OFFSET_COL_PREFIX}{}", col.column_id);
            let arr = batch.column_by_name(&name).ok_or_else(|| {
                ErrorCode::StorageOther(format!("page index missing offset column {name}"))
            })?;
            offsets.push(decode_u64_column(arr)?);
        }

        // Reserved `active` flag. Absent in index files written before this column existed; those
        // decode as all-active so every granule stays eligible.
        let num_granules = meta.num_granules as usize;
        let active = match batch.column_by_name(PAGE_INDEX_ACTIVE_COL) {
            Some(arr) => decode_bool_column(arr)?,
            None => vec![true; num_granules],
        };

        Ok(PageIndex {
            meta,
            granule_mins,
            offsets,
            active,
        })
    }

    /// Load and decode the sparse page index sidecar file for a block. The index is tiny (one row
    /// per granule), so the whole object is fetched in a single read rather than projecting
    /// columns. `size` is the byte length recorded in `BlockMeta.page_index_size` (read hint).
    #[fastrace::trace]
    pub async fn load(dal: &Operator, location: &str, size: u64) -> Result<Self> {
        let buffer = if size > 0 {
            dal.read_with(location).range(0..size).await
        } else {
            dal.read(location).await
        }
        .map_err(|e| {
            ErrorCode::StorageOther(format!("failed to read page index {location}: {e}"))
        })?;
        Self::decode(&buffer.to_bytes())
    }
}

/// A narrowed read plan for one leaf column, derived from a contiguous run of selected granules.
/// The column's partial chunk is reconstructed as `[dict bytes] ++ [data bytes]`, which is a valid
/// parquet column-chunk layout (dictionary page followed by data pages) that the page reader can
/// decode standalone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnReadPlan {
    pub column_id: ColumnId,
    /// Dictionary page byte range, if the column chunk is dict-encoded. Must always be read.
    pub dict_range: Option<std::ops::Range<u64>>,
    /// Contiguous data-page byte range covering exactly the selected granules.
    pub data_range: std::ops::Range<u64>,
}

/// A whole-block narrowed read plan: the per-column byte ranges plus the number of rows the
/// reconstructed partial chunks will contain (identical across all columns, so rows stay aligned).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockReadPlan {
    pub columns: Vec<ColumnReadPlan>,
    pub num_rows: usize,
}

impl PageIndex {
    /// Build a narrowed read plan from the granule range `[range.start, range.end)` carried in
    /// `BlockMetaIndex.page_granule_range` (computed at prune time). `block_rows` is the block's
    /// total row count. An empty range means every granule was pruned: returns a plan with zero
    /// rows and zero-length data ranges so the reader fetches nothing.
    ///
    /// The range is bounded by num_granules at prune time, so all granule indices are in bounds.
    pub fn read_plan_for_range(
        &self,
        range: &std::ops::Range<usize>,
        block_rows: usize,
    ) -> BlockReadPlan {
        if range.start >= range.end {
            // Whole block excluded by the predicate.
            let columns = self
                .meta
                .columns
                .iter()
                .map(|col| ColumnReadPlan {
                    column_id: col.column_id,
                    dict_range: None,
                    data_range: 0..0,
                })
                .collect();
            return BlockReadPlan {
                columns,
                num_rows: 0,
            };
        }

        let granule_rows = self.meta.granule_rows as usize;
        // Rows covered by the run: full granules, with the block's tail bounding the last one.
        let run_start_row = range.start * granule_rows;
        let run_end_row = (range.end * granule_rows).min(block_rows);
        let num_rows = run_end_row.saturating_sub(run_start_row);

        let mut columns = Vec::with_capacity(self.meta.columns.len());
        for (col_idx, col) in self.meta.columns.iter().enumerate() {
            let offs = &self.offsets[col_idx];
            let data_start = offs[range.start];
            let data_end = if range.end < offs.len() {
                offs[range.end]
            } else {
                col.chunk_end
            };
            columns.push(ColumnReadPlan {
                column_id: col.column_id,
                dict_range: col.dict_page.map(|(off, len)| off..off + len),
                data_range: data_start..data_end,
            });
        }
        BlockReadPlan { columns, num_rows }
    }
}

/// Zip the per-element granule-min columns (tuple order) back into one `Scalar::Tuple` per granule,
/// matching the shape the prune-time evaluator expects.
fn zip_granule_mins(element_cols: &[Column], num_granules: usize) -> Vec<Scalar> {
    (0..num_granules)
        .map(|g| {
            let elems = element_cols
                .iter()
                .map(|col| col.index(g).unwrap().to_owned())
                .collect();
            Scalar::Tuple(elems)
        })
        .collect()
}

fn decode_u64_column(array: &dyn arrow_array::Array) -> Result<Vec<u64>> {
    use arrow_array::UInt64Array;
    let a = array
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            ErrorCode::StorageOther(format!(
                "page index offset column has unexpected arrow type {:?}",
                array.data_type()
            ))
        })?;
    let n = arrow_array::Array::len(a);
    Ok((0..n).map(|i| a.value(i)).collect())
}

fn decode_bool_column(array: &dyn arrow_array::Array) -> Result<Vec<bool>> {
    use arrow_array::BooleanArray;
    let a = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            ErrorCode::StorageOther(format!(
                "page index active column has unexpected arrow type {:?}",
                array.data_type()
            ))
        })?;
    let n = arrow_array::Array::len(a);
    Ok((0..n).map(|i| a.value(i)).collect())
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::NumberScalar;
    use databend_storages_common_blocks::DataPageOffset;

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

    // Build an index for 3 granules over 2 leaf columns (one dict-encoded), then decode it and
    // confirm granule mins, per-column offsets, and byte ranges round-trip exactly.
    #[test]
    fn test_page_index_roundtrip() {
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

        let builder = PageIndexBuilder::new(42, granule_rows, vec![7, 9]);
        let state = builder
            .build(
                &page_layout,
                &granule_mins,
                ("page_index/test.parquet".to_string(), 0),
            )
            .unwrap();

        let bytes: Vec<u8> = state.data.to_bytes().to_vec();
        let index = PageIndex::decode(&bytes).unwrap();

        assert_eq!(index.meta.num_granules, 3);
        assert_eq!(index.meta.cluster_key_id, 42);
        assert_eq!(index.meta.granule_rows, 100);
        assert_eq!(index.granule_mins, granule_mins);
        assert_eq!(index.meta.columns[0].column_id, 7);
        assert_eq!(index.meta.columns[1].column_id, 9);
        assert_eq!(index.meta.columns[1].dict_page, Some((10, 40)));

        // Column 7 granule offsets pick the page starting on each granule boundary row (0/100/200).
        assert_eq!(index.offsets[0], vec![100, 260, 480]);
        assert_eq!(index.offsets[1], vec![50, 600, 1500]);

        // Reserved active flag: all granules active on a freshly written index.
        assert_eq!(index.active, vec![true, true, true]);

        // Read plan for the contiguous granule run [1, 3) over 250 block rows (last granule
        // partial: rows 200..250). Each column yields dict (if any) + one contiguous data range.
        let plan = index.read_plan_for_range(&(1..3), 250);
        assert_eq!(plan.num_rows, 150); // rows 100..250
        assert_eq!(plan.columns.len(), 2);
        // Column 7: no dict; data covers granule 1 start (260) to chunk_end (1000).
        assert_eq!(plan.columns[0].column_id, 7);
        assert_eq!(plan.columns[0].dict_range, None);
        assert_eq!(plan.columns[0].data_range, 260..1000);
        // Column 9: dict [10,40); data covers granule 1 start (600) to chunk_end (2000).
        assert_eq!(plan.columns[1].column_id, 9);
        assert_eq!(plan.columns[1].dict_range, Some(10..50));
        assert_eq!(plan.columns[1].data_range, 600..2000);

        // Read plan for a middle-only run [0, 1): data bounded above by the next granule offset.
        let plan = index.read_plan_for_range(&(0..1), 250);
        assert_eq!(plan.num_rows, 100);
        assert_eq!(plan.columns[0].data_range, 100..260);
        assert_eq!(plan.columns[1].data_range, 50..600);

        // Empty range -> zero rows, zero-length data ranges (whole block excluded).
        let plan = index.read_plan_for_range(&(0..0), 250);
        assert_eq!(plan.num_rows, 0);
        assert_eq!(plan.columns[0].data_range, 0..0);
    }

    // Multi-element cluster key (Int64, String) with a NULL min element: the granule mins must
    // round-trip element-by-element through the typed `m{i}` columns and rebuild as tuple scalars.
    #[test]
    fn test_page_index_multi_column_mins_roundtrip() {
        let granule_rows = 100;
        let leaf = layout(None, 500, &[(0, 100), (100, 300)]);
        let page_layout = vec![leaf];

        let s_int = |v: i64| Scalar::Number(NumberScalar::Int64(v));
        let s_str = |v: &str| Scalar::String(v.to_string());
        let granule_mins = vec![
            Scalar::Tuple(vec![s_int(1), s_str("aaa")]),
            // Second granule's string element is NULL — exercises the nullable column path.
            Scalar::Tuple(vec![s_int(2), Scalar::Null]),
        ];

        let builder = PageIndexBuilder::new(7, granule_rows, vec![5]);
        let state = builder
            .build(
                &page_layout,
                &granule_mins,
                ("page_index/multi.parquet".to_string(), 0),
            )
            .unwrap();

        let bytes: Vec<u8> = state.data.to_bytes().to_vec();
        let index = PageIndex::decode(&bytes).unwrap();

        // Two cluster-key element types, both stored nullable.
        assert_eq!(index.meta.cluster_key_types.len(), 2);
        assert!(index.meta.cluster_key_types.iter().all(|t| t.is_nullable()));
        // Granule mins rebuild as tuples, NULL element preserved.
        assert_eq!(index.granule_mins, granule_mins);
    }
}
