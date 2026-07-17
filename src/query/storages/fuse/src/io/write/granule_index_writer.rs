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

//! Sparse granule index marks are split into two Parquet files: cluster-key mins (`m{i}`) and
//! data-page offsets (`g_{column_id}` plus granule-index payload offsets). Their column byte ranges
//! are stored in `GranuleIndexLayout`, so readers do not need either file's footer.

use std::collections::HashMap;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_storages_common_blocks::LeafPageLayout;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_io::OperatorRangeReader;
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
use crate::io::granule_index::GranuleMark;
use crate::io::read::column_chunks_to_record_batch;

/// Prefix of a per-cluster-key-element granule-min column name (`m{i}`).
pub const GRANULE_INDEX_MIN_COL_PREFIX: &str = "m";

/// Prefix of a per-leaf-column offset column name (`g_{column_id}`).
pub const GRANULE_INDEX_OFFSET_COL_PREFIX: &str = "g_";

#[derive(Debug, Clone)]
pub struct GranuleIndexFileState {
    pub data: Buffer,
    pub layout: GranuleIndexFileLayout,
}

#[derive(Debug, Clone)]
pub struct GranuleIndexState {
    pub granule_rows: u32,
    pub mins: Option<GranuleIndexFileState>,
    pub offsets: GranuleIndexFileState,
}

impl GranuleIndexState {
    pub fn layout(&self) -> GranuleIndexLayout {
        GranuleIndexLayout {
            granule_rows: self.granule_rows,
            mins: self.mins.as_ref().map(|f| f.layout.clone()),
            offsets: self.offsets.layout.clone(),
        }
    }
}

pub struct GranuleIndexWriter {
    cluster_key_id: Option<u32>,
    granule_rows: usize,
    // Must match Parquet leaf order in `page_layout`.
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

    pub fn build_with_extra_marks(
        &self,
        page_layout: &[LeafPageLayout],
        num_granules: usize,
        granule_mins: &[Scalar],
        cluster_key_types: &[DataType],
        mins_location: Location,
        offsets_location: Location,
        extra_marks: Vec<GranuleMark>,
    ) -> Result<GranuleIndexState> {
        let granule_rows = u32::try_from(self.granule_rows).map_err(|_| {
            ErrorCode::Internal(format!(
                "granule index rows {} exceed metadata limit {}",
                self.granule_rows,
                u32::MAX
            ))
        })?;
        if page_layout.len() != self.leaf_column_ids.len() {
            return Err(ErrorCode::Internal(format!(
                "granule index: layout leaves {} != leaf column ids {}",
                page_layout.len(),
                self.leaf_column_ids.len()
            )));
        }

        if num_granules == 0 {
            return Err(ErrorCode::Internal(
                "granule index build called with zero granules",
            ));
        }
        if !granule_mins.is_empty() && granule_mins.len() != num_granules {
            return Err(ErrorCode::Internal(format!(
                "granule index has {} mins, expected {num_granules}",
                granule_mins.len()
            )));
        }
        if granule_mins.is_empty() != cluster_key_types.is_empty() {
            return Err(ErrorCode::Internal(format!(
                "granule index has {} mins but {} cluster key types",
                granule_mins.len(),
                cluster_key_types.len()
            )));
        }
        let has_mins = !granule_mins.is_empty();

        let mut offset_marks = Vec::with_capacity(self.leaf_column_ids.len() + extra_marks.len());
        for (leaf_idx, leaf) in page_layout.iter().enumerate() {
            let column_id = self.leaf_column_ids[leaf_idx];
            let offsets = self.granule_offsets(leaf, num_granules)?;
            let name = format!("{GRANULE_INDEX_OFFSET_COL_PREFIX}{column_id}");
            offset_marks.push(GranuleMark::create(&name, offsets));
        }
        offset_marks.extend(extra_marks);
        let offsets = serialize_marks(offset_marks, num_granules, offsets_location)?;

        let mins = if has_mins {
            let columns = build_min_columns(granule_mins, cluster_key_types)?;
            let fields = cluster_key_types
                .iter()
                .enumerate()
                .map(|(i, ty)| {
                    let name = format!("{GRANULE_INDEX_MIN_COL_PREFIX}{i}");
                    Ok(TableField::new(
                        &name,
                        infer_schema_type(&ty.wrap_nullable())?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Some(serialize_columns(
                fields,
                columns,
                num_granules,
                mins_location,
            )?)
        } else {
            None
        };

        // `cluster_key_id` currently only affects whether a mins file exists; it is validated at
        // prune time against `cluster_stats.cluster_key_id`, not stored here.
        let _ = self.cluster_key_id;

        Ok(GranuleIndexState {
            granule_rows,
            mins,
            offsets,
        })
    }

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

fn serialize_marks(
    marks: Vec<GranuleMark>,
    num_granules: usize,
    location: Location,
) -> Result<GranuleIndexFileState> {
    let (fields, columns): (Vec<_>, Vec<_>) = marks
        .into_iter()
        .map(|mark| (mark.field, mark.values))
        .unzip();
    serialize_columns(fields, columns, num_granules, location)
}

fn serialize_columns(
    fields: Vec<TableField>,
    columns: Vec<Column>,
    num_granules: usize,
    location: Location,
) -> Result<GranuleIndexFileState> {
    if fields.is_empty() || fields.len() != columns.len() {
        return Err(ErrorCode::Internal(format!(
            "granule marks file has {} fields and {} columns",
            fields.len(),
            columns.len()
        )));
    }

    let mut names = std::collections::HashSet::with_capacity(fields.len());
    for (field, column) in fields.iter().zip(&columns) {
        if column.len() != num_granules {
            return Err(ErrorCode::Internal(format!(
                "granule mark {} has {} rows, expected {num_granules}",
                field.name(),
                column.len()
            )));
        }
        if !names.insert(field.name()) {
            return Err(ErrorCode::Internal(format!(
                "duplicate granule mark {}",
                field.name()
            )));
        }
    }

    let schema = TableSchema::new(fields.clone());
    let block = DataBlock::new_from_columns(columns);
    block.check_valid()?;
    let serialized = blocks_to_parquet(&schema, vec![block], TableCompression::None, false, None)?;
    let size = serialized.len() as u64;

    let row_group = &serialized.metadata.row_groups()[0];
    if row_group.columns().len() != fields.len() {
        return Err(ErrorCode::Internal(format!(
            "granule index marks file: parquet has {} chunks but {} fields",
            row_group.columns().len(),
            fields.len()
        )));
    }
    let mut columns_layout: HashMap<String, Vec<BytesRange>> = HashMap::with_capacity(fields.len());
    for (field, chunk_meta) in fields.iter().zip(row_group.columns().iter()) {
        let (offset, len) = chunk_meta.byte_range();
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

fn build_min_columns(
    granule_mins: &[Scalar],
    cluster_key_types: &[DataType],
) -> Result<Vec<Column>> {
    let mut builders: Vec<ColumnBuilder> = cluster_key_types
        .iter()
        .map(|ty| ColumnBuilder::with_capacity(&ty.wrap_nullable(), granule_mins.len()))
        .collect();

    for m in granule_mins {
        let tuple = m.as_tuple().ok_or_else(|| {
            ErrorCode::Internal("granule index: granule min must be a tuple scalar")
        })?;
        if tuple.len() != cluster_key_types.len() {
            return Err(ErrorCode::Internal(format!(
                "granule index: granule min arity {} != expected {}",
                tuple.len(),
                cluster_key_types.len()
            )));
        }
        for (i, elem) in tuple.iter().enumerate() {
            if !elem.as_ref().is_value_of_type(&cluster_key_types[i]) {
                return Err(ErrorCode::Internal(format!(
                    "granule index: min element {i} has value {elem:?}, expected type {:?}",
                    cluster_key_types[i]
                )));
            }
            builders[i].push(elem.as_ref());
        }
    }

    Ok(builders.into_iter().map(|b| b.build()).collect())
}

// ============================ read path ============================

use std::ops::Range;

pub fn num_granules_of(block_rows: usize, granule_rows: usize) -> usize {
    if granule_rows == 0 {
        return 0;
    }
    block_rows.div_ceil(granule_rows)
}

fn fetch_granule_marks(
    dal: &Operator,
    settings: &ReadSettings,
    layout: &GranuleIndexFileLayout,
    names: &[String],
) -> Result<HashMap<String, Buffer>> {
    let (byte_ranges, plan) = granule_mark_read_plan(layout, names);
    if byte_ranges.is_empty() {
        return Ok(HashMap::new());
    }
    let mut reader = OperatorRangeReader::create(
        settings,
        dal.clone(),
        layout.location.0.clone(),
        &byte_ranges,
        1,
    )?;
    let mut per_mark: HashMap<String, Vec<u8>> = HashMap::new();
    for name in plan {
        let data = reader.read()?;
        per_mark
            .entry(name)
            .or_default()
            .extend_from_slice(&data.to_bytes());
    }
    Ok(per_mark
        .into_iter()
        .map(|(name, bytes)| (name, Buffer::from(bytes)))
        .collect())
}

type GranuleMarkReadPlan = (Vec<Range<u64>>, Vec<String>);

fn granule_mark_read_plan(
    layout: &GranuleIndexFileLayout,
    names: &[String],
) -> GranuleMarkReadPlan {
    let mut byte_ranges = Vec::new();
    let mut plan = Vec::new();
    for name in names {
        let Some(spans) = layout.columns.get(name) else {
            continue;
        };
        for span in spans {
            byte_ranges.push(span.offset..span.offset + span.len);
            plan.push(name.clone());
        }
    }
    (byte_ranges, plan)
}

fn decode_single_column(bytes: Buffer, ty: &DataType, num_rows: usize) -> Result<Column> {
    let field = TableField::new("c", infer_schema_type(ty)?);
    let schema = TableSchema::new(vec![field]);
    let mut chunks: HashMap<ColumnId, DataItem> = HashMap::new();
    chunks.insert(0, DataItem::RawData(bytes));
    let batch =
        column_chunks_to_record_batch(&schema, num_rows, &chunks, &Compression::None, None)?;
    Column::from_arrow_rs(batch.column(0).clone(), ty)
}

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

/// Marks loaded once for all active granule pruners of a block.
pub struct GranulePruningReadContext {
    num_granules: usize,
    marks: HashMap<String, Vec<u64>>,
}

impl GranulePruningReadContext {
    pub fn load(
        dal: &Operator,
        settings: &ReadSettings,
        layout: &GranuleIndexFileLayout,
        names: &[String],
        num_granules: usize,
    ) -> Result<Self> {
        let mut unique_names = Vec::with_capacity(names.len());
        let mut seen = std::collections::HashSet::with_capacity(names.len());
        for name in names {
            if seen.insert(name.as_str()) {
                unique_names.push(name.clone());
            }
        }

        let raw_marks = fetch_granule_marks(dal, settings, layout, &unique_names)?;
        let mut marks = HashMap::with_capacity(raw_marks.len());
        for (name, raw) in raw_marks {
            marks.insert(name, decode_u64_column(raw, num_granules)?);
        }
        Ok(Self {
            num_granules,
            marks,
        })
    }

    pub fn num_granules(&self) -> usize {
        self.num_granules
    }

    pub fn mark(&self, name: &str) -> Option<&[u64]> {
        self.marks.get(name).map(Vec::as_slice)
    }
}

pub fn load_granule_mins(
    dal: &Operator,
    settings: &ReadSettings,
    layout: &GranuleIndexFileLayout,
    element_types: &[DataType],
    num_granules: usize,
) -> Result<Vec<Scalar>> {
    let names: Vec<String> = (0..element_types.len())
        .map(|i| format!("{GRANULE_INDEX_MIN_COL_PREFIX}{i}"))
        .collect();
    let mut buffers = fetch_granule_marks(dal, settings, layout, &names)?;

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

/// Byte ranges needed to reconstruct a selected leaf column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnReadPlan {
    pub column_id: ColumnId,
    pub dict_range: Option<Range<u64>>,
    pub data_range: Range<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockReadPlan {
    pub columns: Vec<ColumnReadPlan>,
    pub num_rows: usize,
    pub start_row: usize,
}

pub struct OffsetsIndex {
    granule_rows: usize,
    offsets: HashMap<ColumnId, Vec<u64>>,
}

impl OffsetsIndex {
    pub fn load(
        dal: &Operator,
        settings: &ReadSettings,
        layout: &GranuleIndexFileLayout,
        granule_rows: usize,
        block_rows: usize,
        col_metas: &HashMap<ColumnId, ColumnMeta>,
    ) -> Result<Self> {
        let num_granules = num_granules_of(block_rows, granule_rows);
        if num_granules == 0 {
            return Err(ErrorCode::Internal(
                "granule index offsets cannot be loaded for zero granules",
            ));
        }
        let names: Vec<String> = col_metas
            .keys()
            .map(|id| format!("{GRANULE_INDEX_OFFSET_COL_PREFIX}{id}"))
            .collect();
        let mut buffers = fetch_granule_marks(dal, settings, layout, &names)?;
        let mut offsets = HashMap::with_capacity(col_metas.len());
        for (id, meta) in col_metas {
            let name = format!("{GRANULE_INDEX_OFFSET_COL_PREFIX}{id}");
            let bytes = buffers.remove(&name).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "granule index offsets missing projected leaf column {name}"
                ))
            })?;
            let values = decode_u64_column(bytes, num_granules)?;
            let (chunk_start, chunk_len) = meta.offset_length();
            let chunk_end = chunk_start.checked_add(chunk_len).ok_or_else(|| {
                ErrorCode::Internal(format!("granule index column {id} chunk range overflows"))
            })?;
            if values
                .iter()
                .any(|offset| *offset < chunk_start || *offset >= chunk_end)
            {
                return Err(ErrorCode::Internal(format!(
                    "granule index offsets for column {id} fall outside chunk {chunk_start}..{chunk_end}"
                )));
            }
            if values.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err(ErrorCode::Internal(format!(
                    "granule index offsets for column {id} are not strictly increasing"
                )));
            }
            offsets.insert(*id, values);
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

    #[tokio::test]
    async fn test_two_file_roundtrip() {
        crate::test_utils::init_test_globals().unwrap();
        let granule_rows = 100;
        let leaf_a = layout(None, 1000, &[(0, 100), (50, 150), (100, 260), (200, 480)]);
        let leaf_b = layout(Some((10, 40)), 2000, &[(0, 50), (100, 600), (200, 1500)]);
        let page_layout = vec![leaf_a, leaf_b];

        let granule_mins = vec![
            Scalar::Tuple(vec![Scalar::Number(NumberScalar::Int64(0))]),
            Scalar::Tuple(vec![Scalar::Number(NumberScalar::Int64(100))]),
            Scalar::Tuple(vec![Scalar::Number(NumberScalar::Int64(200))]),
        ];

        let builder = GranuleIndexWriter::new(Some(42), granule_rows, vec![7, 9]);
        let state = builder
            .build_with_extra_marks(
                &page_layout,
                3,
                &granule_mins,
                &[DataType::Number(NumberDataType::Int64)],
                ("mins.parquet".to_string(), 0),
                ("offs.parquet".to_string(), 0),
                vec![],
            )
            .unwrap();

        let op = Operator::new(Memory::default()).unwrap().finish();
        persist(&op, &state).await;
        let settings = test_settings();
        let layout = state.layout();

        let element_types = vec![DataType::Number(NumberDataType::Int64)];
        let mins = load_granule_mins(
            &op,
            &settings,
            layout.mins.as_ref().unwrap(),
            &element_types,
            3,
        )
        .unwrap();
        assert_eq!(mins, granule_mins);

        let mut col_metas = HashMap::new();
        col_metas.insert(
            7,
            ColumnMeta::Parquet(SingleColumnMeta {
                offset: 100,
                len: 900,
                num_values: 300,
            }),
        );
        col_metas.insert(
            9,
            ColumnMeta::Parquet(SingleColumnMeta {
                offset: 10,
                len: 1990,
                num_values: 300,
            }),
        );
        let offsets = OffsetsIndex::load(
            &op,
            &settings,
            &layout.offsets,
            granule_rows,
            300,
            &col_metas,
        )
        .unwrap();
        assert_eq!(offsets.offsets.get(&7).unwrap(), &vec![100, 260, 480]);
        assert_eq!(offsets.offsets.get(&9).unwrap(), &vec![50, 600, 1500]);
    }

    #[tokio::test]
    async fn test_nullable_min_type_does_not_depend_on_first_value() {
        crate::test_utils::init_test_globals().unwrap();
        let granule_mins = vec![
            Scalar::Tuple(vec![Scalar::Null]),
            Scalar::Tuple(vec![Scalar::Number(NumberScalar::Int64(100))]),
        ];
        let element_type = DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64)));
        let state = GranuleIndexWriter::new(Some(42), 100, vec![7])
            .build_with_extra_marks(
                &[layout(None, 1000, &[(0, 100), (100, 260)])],
                2,
                &granule_mins,
                std::slice::from_ref(&element_type),
                ("mins.parquet".to_string(), 0),
                ("offs.parquet".to_string(), 0),
                vec![],
            )
            .unwrap();

        let op = Operator::new(Memory::default()).unwrap().finish();
        persist(&op, &state).await;
        let mins = load_granule_mins(
            &op,
            &test_settings(),
            state.layout().mins.as_ref().unwrap(),
            &[element_type],
            2,
        )
        .unwrap();
        assert_eq!(mins, granule_mins);
    }

    #[test]
    fn test_rejects_granule_rows_above_metadata_limit() {
        let error = GranuleIndexWriter::new(None, u32::MAX as usize + 1, vec![7])
            .build_with_extra_marks(
                &[],
                1,
                &[],
                &[],
                ("mins".to_string(), 0),
                ("offsets".to_string(), 0),
                vec![],
            )
            .unwrap_err();
        assert!(error.message().contains("exceed metadata limit"));
    }

    #[test]
    fn test_offset_only_granule_count_is_not_page_count() {
        let state = GranuleIndexWriter::new(None, 100, vec![7])
            .build_with_extra_marks(
                &[layout(None, 1000, &[(0, 100), (50, 180)])],
                1,
                &[],
                &[],
                ("mins".to_string(), 0),
                ("offsets".to_string(), 0),
                vec![],
            )
            .unwrap();
        assert_eq!(state.granule_rows, 100);
    }

    #[tokio::test]
    async fn test_offsets_index_rejects_missing_projected_column() {
        crate::test_utils::init_test_globals().unwrap();
        let op = Operator::new(Memory::default()).unwrap().finish();
        let settings = test_settings();
        let state = GranuleIndexWriter::new(None, 100, vec![7])
            .build_with_extra_marks(
                &[layout(None, 1000, &[(0, 100), (100, 260)])],
                2,
                &[],
                &[],
                ("mins".to_string(), 0),
                ("offsets".to_string(), 0),
                vec![],
            )
            .unwrap();
        persist(&op, &state).await;

        let mut col_metas = HashMap::new();
        col_metas.insert(
            7,
            ColumnMeta::Parquet(SingleColumnMeta {
                offset: 100,
                len: 900,
                num_values: 200,
            }),
        );
        col_metas.insert(
            9,
            ColumnMeta::Parquet(SingleColumnMeta {
                offset: 10,
                len: 990,
                num_values: 200,
            }),
        );
        let err = OffsetsIndex::load(&op, &settings, &state.offsets.layout, 100, 200, &col_metas)
            .err()
            .unwrap();
        assert!(err.message().contains("g_9"), "{err}");
    }

    #[tokio::test]
    async fn test_pruning_context_loads_only_requested_marks() {
        crate::test_utils::init_test_globals().unwrap();
        let state = serialize_marks(
            vec![
                GranuleMark::create("needed_a", vec![1, 2]),
                GranuleMark::create("unused", vec![3, 4]),
                GranuleMark::create("needed_b", vec![5, 6]),
            ],
            2,
            ("marks".to_string(), 0),
        )
        .unwrap();
        let op = Operator::new(Memory::default()).unwrap().finish();
        op.write(&state.layout.location.0, state.data.to_bytes())
            .await
            .unwrap();

        let requested = vec![
            "needed_a".to_string(),
            "needed_b".to_string(),
            "needed_a".to_string(),
        ];
        let context =
            GranulePruningReadContext::load(&op, &test_settings(), &state.layout, &requested, 2)
                .unwrap();

        assert_eq!(context.mark("needed_a"), Some([1, 2].as_slice()));
        assert_eq!(context.mark("needed_b"), Some([5, 6].as_slice()));
        assert_eq!(context.mark("unused"), None);
    }

    #[test]
    fn test_serialize_marks_rejects_wrong_row_count() {
        let error = serialize_marks(
            vec![GranuleMark::create("g_7", vec![100])],
            2,
            ("offsets".to_string(), 0),
        )
        .unwrap_err();
        assert!(error.message().contains("has 1 rows, expected 2"));
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
