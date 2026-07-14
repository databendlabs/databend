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

//! Bloom granule-level index: one payload parquet file per indexed column, one data page per granule
//! (a granule's `FilterImpl` bytes serialized into a single-column Binary parquet). Per-granule
//! page byte offsets are captured from the writer's page layout and returned as sidecar offset
//! columns (`gbloom_{ver}_off/len_{col_id}`); the payload never has to be reopened at prune time.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::ops::Range;
use std::sync::Arc;

use arrow_array::ArrayRef;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FieldIndex;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_expression::types::UInt64Type;
use databend_common_meta_app::schema::TableIndex;
use databend_storages_common_blocks::BulkParquetFileWriter;
use databend_storages_common_blocks::BulkParquetLeafWriter;
use databend_storages_common_blocks::StreamingUploadSink;
use databend_storages_common_blocks::build_parquet_writer_properties;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::BloomIndexBuilder;
use databend_storages_common_index::BloomIndexType;
use databend_storages_common_index::FilterEvalResult;
use databend_storages_common_index::filters::BlockFilter;
use databend_storages_common_index::filters::Filter;
use databend_storages_common_index::filters::FilterImpl;
use databend_storages_common_io::MergeIOReader;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::GranuleIndexLayout;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::table::TableCompression;
use opendal::Buffer;
use opendal::Operator;
use parquet::arrow::arrow_writer::compute_leaves;

use super::GranuleIndexBuildOutput;
use super::GranuleIndexBuilder;
use super::GranuleIndexPruner;
use super::GranuleIndexSpec;
use super::NoopGranuleIndexBuilder;
use crate::io::TableMetaLocationGenerator;
use crate::io::compact_index_version;
use crate::io::load_u64_columns;
use crate::io::num_granules_of;

/// Names of the `(off, len)` sidecar offset columns for one indexed column. Keyed by compacted index
/// version and column id so several indexes on the same column — or a recreated index — never
/// collide in the shared `_pidx` sidecar.
fn sidecar_offset_col_names(index_version: &str, col_id: u32) -> (String, String) {
    let ver = compact_index_version(index_version);
    (
        format!("gbloom_{ver}_off_{col_id}"),
        format!("gbloom_{ver}_len_{col_id}"),
    )
}

/// Column name of the single Binary column in a payload parquet file.
const PAYLOAD_FILTER_COL: &str = "f";

/// Factory for one `TYPE bloom` table index, resolved once from `TableMeta.indexes`.
#[derive(Clone)]
pub struct BloomGranuleIndexSpec {
    index_name: String,
    index_version: String,
    bloom_index_type: BloomIndexType,
    /// Stable IDs from the index definition. Concrete `FieldIndex` values and fields are bound
    /// against the exact schema of each writer or query.
    column_ids: Vec<ColumnId>,
}

impl BloomGranuleIndexSpec {
    /// Resolve a `TYPE bloom` index entry into a spec, or `None` when none of its columns have a
    /// bloom-supported type.
    pub fn try_create(
        index_name: &str,
        index: &TableIndex,
        schema: &TableSchema,
        bloom_index_type: BloomIndexType,
    ) -> Result<Option<Self>> {
        // Resolve the complete declaration against the table schema, but retain only stable IDs.
        // Missing or unsupported fields make the declaration invalid; a later mismatch against the
        // physical write schema is handled by the no-op builder in `new_builder`.
        let all_columns_valid = index.column_ids.iter().all(|column_id| {
            schema
                .fields()
                .iter()
                .find(|field| field.column_id() == *column_id)
                .is_some_and(|field| is_bloom_supported_type(field.data_type()))
        });
        if index.column_ids.is_empty() || !all_columns_valid {
            return Ok(None);
        }
        Ok(Some(BloomGranuleIndexSpec {
            index_name: index_name.to_string(),
            index_version: index.version.clone(),
            bloom_index_type,
            column_ids: index.column_ids.clone(),
        }))
    }

    /// Bind every indexed column to the physical block schema. As with vector and spatial indexes,
    /// partial binding is rejected: callers use a no-op builder rather than materializing an
    /// incomplete index.
    fn bind_columns(&self, physical_schema: &TableSchema) -> Option<Vec<(FieldIndex, TableField)>> {
        self.column_ids
            .iter()
            .map(|column_id| {
                physical_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .find(|(_, field)| {
                        field.column_id() == *column_id
                            && is_bloom_supported_type(field.data_type())
                    })
                    .map(|(field_index, field)| (field_index, field.clone()))
            })
            .collect()
    }
}

impl GranuleIndexSpec for BloomGranuleIndexSpec {
    fn new_builder(
        &self,
        func_ctx: FunctionContext,
        physical_schema: &TableSchema,
        block_location: &str,
        dal: Operator,
    ) -> Result<Box<dyn GranuleIndexBuilder>> {
        let Some(bound_columns) = self.bind_columns(physical_schema) else {
            log::debug!(
                "Ignoring granule bloom index {} while writing: not all indexed columns exist in the physical schema",
                self.index_name
            );
            return Ok(Box::new(NoopGranuleIndexBuilder));
        };
        let mut columns = Vec::with_capacity(bound_columns.len());
        for (field_index, field) in bound_columns {
            let location =
                TableMetaLocationGenerator::gen_granule_bloom_location_from_block_location(
                    block_location,
                    &self.index_name,
                    &self.index_version,
                    field.column_id(),
                );
            columns.push(ColumnPayloadState::new(
                field_index,
                field,
                location,
                dal.clone(),
            )?);
        }
        Ok(Box::new(BloomGranuleIndexBuilder {
            func_ctx,
            bloom_index_type: self.bloom_index_type,
            index_version: self.index_version.clone(),
            columns,
        }))
    }

    fn new_pruner(
        &self,
        func_ctx: FunctionContext,
        schema: &TableSchemaRef,
        filter_expr: Option<&Expr<String>>,
        dal: Operator,
        settings: ReadSettings,
    ) -> Result<Option<Arc<dyn GranuleIndexPruner>>> {
        BloomGranuleIndexPruner::try_create(self, func_ctx, schema, filter_expr, dal, settings)
    }
}

/// Single-column payload parquet schema: one nullable Binary column. A row's value is a granule's
/// `FilterImpl::to_bytes()`, or NULL when that granule produced no filter.
fn payload_table_schema() -> TableSchema {
    TableSchema::new(vec![TableField::new(
        PAYLOAD_FILTER_COL,
        TableDataType::Nullable(Box::new(TableDataType::Binary)),
    )])
}

/// The single (nullable Binary) arrow field the payload leaf is driven with.
fn payload_arrow_field() -> Arc<arrow_schema::Field> {
    let arrow_schema: arrow_schema::Schema = (&payload_table_schema()).into();
    arrow_schema.fields()[0].clone()
}

/// A payload writer streaming into `sink`: PLAIN, no dictionary and no compression. Granule
/// boundaries are forced explicitly with `BulkParquetLeafWriter::flush_page`; dictionary remains
/// disabled so every completed page is forwarded immediately to the streaming sink.
fn new_payload_writer(
    sink: StreamingUploadSink,
) -> Result<BulkParquetFileWriter<StreamingUploadSink>> {
    let table_schema = payload_table_schema();
    let arrow_schema = Arc::new((&table_schema).into());
    let props = Arc::new(build_parquet_writer_properties(
        TableCompression::None,
        false, // no dictionary: flushed pages must reach the sink immediately
        None::<&StatisticsOfColumns>,
        None,
        0,
        &table_schema,
        None, // page boundaries are controlled explicitly per granule
        None,
    ));
    BulkParquetFileWriter::create(sink, arrow_schema, props)
}

/// Per-block, per-column payload state. Only the current granule's Bloom builder is retained; when
/// the granule seals, its filter is encoded into the persistent single-leaf Parquet writer and the
/// page is flushed immediately into the bounded async upload sink.
struct ColumnPayloadState {
    field_index: FieldIndex,
    field: TableField,
    col_id: u32,
    payload_field: Arc<arrow_schema::Field>,
    payload_writer: Option<BulkParquetLeafWriter<StreamingUploadSink>>,
    granules_written: usize,
    current: Option<BloomIndexBuilder>,
}

impl ColumnPayloadState {
    fn new(
        field_index: FieldIndex,
        field: TableField,
        location: String,
        dal: Operator,
    ) -> Result<Self> {
        let col_id = field.column_id();
        let sink = StreamingUploadSink::create(dal, location, 2);
        let payload_writer = new_payload_writer(sink)?.next_leaf()?;
        Ok(Self {
            field_index,
            field,
            col_id,
            payload_field: payload_arrow_field(),
            payload_writer: Some(payload_writer),
            granules_written: 0,
            current: None,
        })
    }

    /// A single-column bloom builder for the current granule. The block fed to it is a one-column
    /// block (the indexed column projected out), so the builder's field index is 0.
    fn ensure_current(&mut self, func_ctx: &FunctionContext, ty: BloomIndexType) -> Result<()> {
        if self.current.is_none() {
            let mut cols = std::collections::BTreeMap::new();
            cols.insert(0usize, self.field.clone());
            self.current = Some(BloomIndexBuilder::create(func_ctx.clone(), ty, cols, &[])?);
        }
        Ok(())
    }

    fn write_filter(&mut self, filter_bytes: Option<Vec<u8>>) -> Result<()> {
        let column = build_single_binary_column(filter_bytes);
        let array = ArrayRef::from(&column);
        let leaves = compute_leaves(&self.payload_field, &array)?;
        if leaves.len() != 1 {
            return Err(ErrorCode::Internal(format!(
                "granule bloom payload expected one parquet leaf, got {}",
                leaves.len()
            )));
        }
        let writer = self.payload_writer.as_mut().expect("payload writer");
        writer.write(&leaves[0])?;
        writer.flush_page()?;
        self.granules_written += 1;
        Ok(())
    }
}

/// Per-block bloom builder: one `ColumnPayloadState` per indexed column.
pub struct BloomGranuleIndexBuilder {
    func_ctx: FunctionContext,
    bloom_index_type: BloomIndexType,
    index_version: String,
    columns: Vec<ColumnPayloadState>,
}

impl GranuleIndexBuilder for BloomGranuleIndexBuilder {
    fn push_rows(&mut self, block: &DataBlock, range: Range<usize>) -> Result<()> {
        if range.is_empty() {
            return Ok(());
        }
        let ty = self.bloom_index_type;
        let func_ctx = self.func_ctx.clone();
        for col in self.columns.iter_mut() {
            col.ensure_current(&func_ctx, ty)?;
            // Project the indexed column into a one-column block sliced to `range`; add_block
            // accumulates incrementally.
            let entry = block.get_by_offset(col.field_index).clone();
            let sub = DataBlock::new(vec![entry], block.num_rows()).slice(range.clone());
            col.current
                .as_mut()
                .expect("current builder created above")
                .add_block(&sub)?;
        }
        Ok(())
    }

    fn finalize_granule(&mut self) -> Result<()> {
        for col in self.columns.iter_mut() {
            // A granule with no pushed rows (should not happen on the write path) yields NULL.
            let filter_bytes = match col.current.take() {
                Some(mut builder) => match builder.finalize()? {
                    Some(bloom) if !bloom.filters.is_empty() => Some(bloom.filters[0].to_bytes()?),
                    _ => None,
                },
                None => None,
            };
            col.write_filter(filter_bytes)?;
        }
        Ok(())
    }

    fn finalize(mut self: Box<Self>) -> Result<GranuleIndexBuildOutput> {
        // Seal a trailing unsealed granule if push_rows happened without a final finalize_granule.
        let has_open = self.columns.iter().any(|c| c.current.is_some());
        if has_open {
            self.finalize_granule()?;
        }

        if self.columns.is_empty() || self.columns[0].granules_written == 0 {
            return Ok(GranuleIndexBuildOutput::default());
        }

        let index_version = self.index_version;
        let mut sidecar_fields = Vec::with_capacity(self.columns.len() * 2);
        let mut sidecar_columns = Vec::with_capacity(self.columns.len() * 2);

        for mut col in self.columns {
            let granules = col.granules_written;
            let leaf = col.payload_writer.take().expect("payload writer");
            let writer = leaf.finish()?;
            // Close the row group + footer, read page offsets, then finish the opendal upload.
            let (metadata, sink) = writer.finish()?;
            let (offs, lens) = page_offsets_from_metadata(&metadata, granules)?;
            sink.close()?;

            let (off_name, len_name) = sidecar_offset_col_names(&index_version, col.col_id);
            sidecar_fields.push(TableField::new(
                &off_name,
                TableDataType::Number(databend_common_expression::types::NumberDataType::UInt64),
            ));
            sidecar_columns.push(UInt64Type::from_data(offs));
            sidecar_fields.push(TableField::new(
                &len_name,
                TableDataType::Number(databend_common_expression::types::NumberDataType::UInt64),
            ));
            sidecar_columns.push(UInt64Type::from_data(lens));
        }

        Ok(GranuleIndexBuildOutput {
            sidecar_fields,
            sidecar_columns,
        })
    }
}

/// Build a one-row nullable-Binary column holding this granule's filter bytes (or NULL).
fn build_single_binary_column(filter_bytes: Option<Vec<u8>>) -> Column {
    use databend_common_expression::types::BinaryType;
    BinaryType::from_opt_data(vec![filter_bytes])
}

/// Extract each payload page's absolute `(offset, len)` from the finished file's `OffsetIndex`.
/// The single column's page locations are in write order, one explicitly flushed page per granule;
/// `offset` is absolute in the file and `compressed_page_size` includes the page header, so the
/// pruner can fetch a page by that exact byte range.
fn page_offsets_from_metadata(
    metadata: &parquet::file::metadata::ParquetMetaData,
    granules: usize,
) -> Result<(Vec<u64>, Vec<u64>)> {
    let offset_index = metadata.offset_index().ok_or_else(|| {
        ErrorCode::Internal("granule bloom payload has no offset index in metadata")
    })?;
    // Single row group, single column.
    let pages = offset_index
        .first()
        .and_then(|rg| rg.first())
        .map(|col| col.page_locations())
        .ok_or_else(|| {
            ErrorCode::Internal("granule bloom payload has no column in offset index")
        })?;
    if pages.len() != granules {
        return Err(ErrorCode::Internal(format!(
            "granule bloom payload page count {} != granule count {granules}",
            pages.len()
        )));
    }
    let mut offs = Vec::with_capacity(granules);
    let mut lens = Vec::with_capacity(granules);
    for page in pages {
        offs.push(page.offset as u64);
        lens.push(page.compressed_page_size as u64);
    }
    Ok((offs, lens))
}

/// One indexed column the filter expression touches: enough to locate its payload file and to name
/// its filter in the per-granule `BloomIndex`.
struct MatchedColumn {
    field: TableField,
    index_name: String,
    index_version: String,
}

/// Read-side bloom pruner: for each surviving granule, evaluates the filter against that granule's
/// per-column bloom filters and drops granules the filter proves cannot match.
pub struct BloomGranuleIndexPruner {
    func_ctx: FunctionContext,
    filter_expr: Expr<String>,
    /// Pre-computed eq-condition scalar digests, reused across all granules.
    eq_scalar_map: HashMap<Scalar, u64>,
    matched_columns: Vec<MatchedColumn>,
    data_schema: TableSchemaRef,
    dal: Operator,
    settings: ReadSettings,
}

impl BloomGranuleIndexPruner {
    /// Build a pruner for one spec, or `None` when it cannot apply: no filter, or the filter touches
    /// none of this index's bloom-supported columns.
    fn try_create(
        spec: &BloomGranuleIndexSpec,
        func_ctx: FunctionContext,
        schema: &TableSchemaRef,
        filter_expr: Option<&Expr<String>>,
        dal: Operator,
        settings: ReadSettings,
    ) -> Result<Option<Arc<dyn GranuleIndexPruner>>> {
        let Some(expr) = filter_expr else {
            return Ok(None);
        };

        let candidate_fields = spec
            .column_ids
            .iter()
            .filter_map(|column_id| {
                schema
                    .fields()
                    .iter()
                    .find(|field| field.column_id() == *column_id)
                    .filter(|field| is_bloom_supported_type(field.data_type()))
                    .cloned()
            })
            .collect::<Vec<_>>();
        let result = BloomIndex::filter_index_field(expr, candidate_fields, vec![])?;
        if result.bloom_fields.is_empty() {
            return Ok(None);
        }

        let mut eq_scalar_map = HashMap::<Scalar, u64>::new();
        for (_, scalar, ty) in result.bloom_scalars.into_iter() {
            if let Entry::Vacant(e) = eq_scalar_map.entry(scalar) {
                let digest = BloomIndex::calculate_scalar_digest(&func_ctx, e.key(), &ty)?;
                e.insert(digest);
            }
        }

        let matched_columns = result
            .bloom_fields
            .into_iter()
            .map(|field| MatchedColumn {
                field,
                index_name: spec.index_name.clone(),
                index_version: spec.index_version.clone(),
            })
            .collect::<Vec<_>>();
        if matched_columns.is_empty() {
            return Ok(None);
        }

        Ok(Some(Arc::new(BloomGranuleIndexPruner {
            func_ctx,
            filter_expr: expr.clone(),
            eq_scalar_map,
            matched_columns,
            data_schema: schema.clone(),
            dal,
            settings,
        })))
    }

    async fn try_prune(
        &self,
        block_meta: &BlockMeta,
        granule_index: &GranuleIndexLayout,
        input_ranges: Option<&[Range<usize>]>,
    ) -> Result<Option<Vec<Range<usize>>>> {
        let num_granules = num_granules_of(
            block_meta.row_count as usize,
            granule_index.granule_rows as usize,
        );
        if num_granules == 0 {
            return Ok(None);
        }

        // The survivor set to evaluate: the previous stage's output, or all granules.
        let survivors: Vec<usize> = match input_ranges {
            Some(ranges) => ranges.iter().flat_map(|r| r.clone()).collect(),
            None => (0..num_granules).collect(),
        };
        if survivors.is_empty() {
            return Ok(Some(Vec::new()));
        }

        // Recover each matched column's per-granule payload (offset, len) columns from the offsets
        // sidecar via the recorded byte ranges (no footer). Columns absent from the layout (block
        // predates this index) are simply missing and their column is skipped below.
        let mut wanted_names = Vec::with_capacity(self.matched_columns.len() * 2);
        for col in &self.matched_columns {
            let (off_name, len_name) =
                sidecar_offset_col_names(&col.index_version, col.field.column_id());
            wanted_names.push(off_name);
            wanted_names.push(len_name);
        }
        let u64_columns = load_u64_columns(
            &self.dal,
            &self.settings,
            &granule_index.offsets,
            &wanted_names,
            num_granules,
        )
        .await?;

        let empty_stats = StatisticsOfColumns::new();
        let column_stats = block_meta.col_stats.clone();

        // granule -> (filter field, filter) contributions across all matched columns.
        let mut per_granule_filters: HashMap<usize, Vec<(TableField, Arc<FilterImpl>)>> =
            HashMap::new();
        let mut any_column_has_payload = false;

        for col in &self.matched_columns {
            let col_id = col.field.column_id();
            let (off_name, len_name) = sidecar_offset_col_names(&col.index_version, col_id);
            let (Some(offs), Some(lens)) = (u64_columns.get(&off_name), u64_columns.get(&len_name))
            else {
                continue;
            };
            any_column_has_payload = true;

            let payload_loc =
                TableMetaLocationGenerator::gen_granule_bloom_location_from_block_location(
                    &block_meta.location.0,
                    &col.index_name,
                    &col.index_version,
                    col_id,
                );

            // Fetch only surviving granules' pages. The merge-io "column id" is reused here as the
            // granule index tag.
            let mut raw_ranges = Vec::with_capacity(survivors.len());
            for &g in &survivors {
                let (start, len) = (offs[g], lens[g]);
                if len > 0 {
                    raw_ranges.push((g as u32, start..start + len));
                }
            }
            if raw_ranges.is_empty() {
                continue;
            }

            let read = MergeIOReader::merge_io_read(
                &self.settings,
                self.dal.clone(),
                &payload_loc,
                &raw_ranges,
            )
            .await?;

            let filter_name =
                BloomIndex::build_filter_bloom_name(BlockFilter::VERSION, &col.field)?;
            let filter_field = TableField::new(&filter_name, TableDataType::Binary);

            for (g_tag, (chunk_idx, byte_range)) in read.columns_chunk_offsets.iter() {
                let chunk = read.owner_memory.get_chunk(*chunk_idx, &payload_loc)?;
                let page_bytes = chunk.slice(byte_range.clone());
                if let Some(filter) = decode_filter_from_page(page_bytes)? {
                    per_granule_filters
                        .entry(*g_tag as usize)
                        .or_default()
                        .push((filter_field.clone(), Arc::new(filter)));
                }
            }
        }

        // No matched column has a payload here (e.g. block predates the index) -> not applicable.
        if !any_column_has_payload {
            return Ok(None);
        }

        let stats = if column_stats.is_empty() {
            &empty_stats
        } else {
            &column_stats
        };

        // Evaluate each surviving granule; keep it unless a filter proves the predicate false.
        let mut kept = Vec::with_capacity(survivors.len());
        for &g in &survivors {
            let keep = match per_granule_filters.get(&g) {
                Some(contribs) if !contribs.is_empty() => {
                    let (fields, filters): (Vec<_>, Vec<_>) = contribs.iter().cloned().unzip();
                    let filter_schema = Arc::new(TableSchema::new(fields));
                    let bloom_index = BloomIndex::from_filter_block(
                        self.func_ctx.clone(),
                        filter_schema,
                        filters,
                        BlockFilter::VERSION,
                    )?;
                    bloom_index.apply(
                        self.filter_expr.clone(),
                        &self.eq_scalar_map,
                        &HashMap::new(),
                        &[],
                        stats,
                        self.data_schema.clone(),
                    )? != FilterEvalResult::MustFalse
                }
                // No filter contributions for this granule -> cannot prove false, keep it.
                _ => true,
            };
            if keep {
                kept.push(g);
            }
        }

        Ok(Some(coalesce(&kept)))
    }
}

#[async_trait::async_trait]
impl GranuleIndexPruner for BloomGranuleIndexPruner {
    async fn prune_granules(
        &self,
        block_meta: &BlockMeta,
        input: Option<&[Range<usize>]>,
    ) -> Option<Vec<Range<usize>>> {
        let granule_index = block_meta.granule_index.as_ref()?;

        match self.try_prune(block_meta, granule_index, input).await {
            Ok(ranges) => ranges,
            Err(e) => {
                log::warn!(
                    "[FUSE-PRUNER] granule bloom pruning failed for {}, keeping block: {e}",
                    block_meta.location.0
                );
                None
            }
        }
    }
}

/// Decode one payload data page (a 1-row single-column nullable-Binary parquet chunk) back into its
/// `FilterImpl`, or `None` when the granule stored NULL (no filter built).
fn decode_filter_from_page(bytes: Buffer) -> Result<Option<FilterImpl>> {
    use databend_storages_common_table_meta::meta::Compression;

    use crate::io::DataItem;
    use crate::io::read::column_chunks_to_record_batch;

    let schema = payload_table_schema();
    let mut chunks: HashMap<u32, DataItem> = HashMap::new();
    chunks.insert(0, DataItem::RawData(bytes));

    let batch = column_chunks_to_record_batch(&schema, 1, &chunks, &Compression::None, None)?;
    let column = Column::from_arrow_rs(
        batch.column(0).clone(),
        &DataType::Nullable(Box::new(DataType::Binary)),
    )?;
    match column.index(0) {
        Some(databend_common_expression::ScalarRef::Binary(b)) => {
            let (filter, _) = FilterImpl::from_bytes(b)?;
            Ok(Some(filter))
        }
        _ => Ok(None),
    }
}

/// Coalesce a sorted list of granule indices into maximally-merged `[start, end)` ranges.
fn coalesce(granules: &[usize]) -> Vec<Range<usize>> {
    let mut ranges: Vec<Range<usize>> = Vec::new();
    for &g in granules {
        match ranges.last_mut() {
            Some(last) if last.end == g => last.end = g + 1,
            _ => ranges.push(g..g + 1),
        }
    }
    ranges
}

/// Whether a column type can be indexed by a bloom filter (mirrors `Xor8Filter::supported_type`).
fn is_bloom_supported_type(data_type: &TableDataType) -> bool {
    let inner = data_type.remove_nullable();
    if let TableDataType::Map(inner_ty) = &inner {
        if let TableDataType::Tuple { fields_type, .. } = inner_ty.remove_nullable() {
            return matches!(
                fields_type[1].remove_nullable(),
                TableDataType::Number(_)
                    | TableDataType::String
                    | TableDataType::Variant
                    | TableDataType::Timestamp
                    | TableDataType::Date
            );
        }
        return false;
    }
    matches!(
        inner,
        TableDataType::Number(_)
            | TableDataType::String
            | TableDataType::Timestamp
            | TableDataType::Date
    )
}

#[cfg(test)]
mod tests {
    use databend_common_expression::DataBlock;
    use databend_common_expression::FunctionContext;
    use databend_common_expression::Scalar;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::NumberDataType;
    use databend_storages_common_index::BloomIndex;
    use databend_storages_common_index::filters::Filter;
    use databend_storages_common_index::filters::FilterImpl;

    use super::*;

    /// Decode one payload data page back into its `FilterImpl`, or `None` if the granule stored
    /// NULL. A single page is a raw column chunk (no parquet footer), so it must be decoded via the
    /// row-group column-chunk path — exactly what the pruner does.
    fn decode_page(bytes: &[u8]) -> Option<FilterImpl> {
        use std::collections::HashMap;

        use databend_storages_common_table_meta::meta::Compression;
        use opendal::Buffer;

        use crate::io::DataItem;
        use crate::io::read::column_chunks_to_record_batch;

        let schema = payload_table_schema();
        let mut chunks: HashMap<u32, DataItem> = HashMap::new();
        chunks.insert(0, DataItem::RawData(Buffer::from(bytes.to_vec())));

        let batch =
            column_chunks_to_record_batch(&schema, 1, &chunks, &Compression::None, None).unwrap();
        let col = Column::from_arrow_rs(
            batch.column(0).clone(),
            &DataType::Nullable(Box::new(DataType::Binary)),
        )
        .unwrap();
        match col.index(0) {
            Some(databend_common_expression::ScalarRef::Binary(raw)) => {
                Some(FilterImpl::from_bytes(raw).unwrap().0)
            }
            _ => None,
        }
    }

    #[test]
    fn test_bind_columns_after_virtual_field_removed() {
        let virtual_field = TableField::new_from_column_id(
            "virtual_col",
            TableDataType::Number(NumberDataType::Int64),
            10,
        );
        let indexed_field = TableField::new_from_column_id(
            "indexed_col",
            TableDataType::Number(NumberDataType::Int64),
            20,
        );
        let table_schema = TableSchema::new_from_column_ids(
            vec![virtual_field, indexed_field.clone()],
            Default::default(),
            21,
        );
        let index = TableIndex {
            name: "idx".to_string(),
            column_ids: vec![indexed_field.column_id()],
            sync_creation: true,
            version: "0".to_string(),
            index_type: databend_common_meta_app::schema::TableIndexType::Bloom,
            options: Default::default(),
        };
        let spec =
            BloomGranuleIndexSpec::try_create("idx", &index, &table_schema, BloomIndexType::Xor8)
                .unwrap()
                .unwrap();

        // Serialization removes the virtual field, shifting indexed_col from offset 1 to offset 0.
        let physical_schema =
            TableSchema::new_from_column_ids(vec![indexed_field.clone()], Default::default(), 21);
        let bound = spec.bind_columns(&physical_schema).unwrap();

        assert_eq!(bound.len(), 1);
        assert_eq!(bound[0].0, 0);
        assert_eq!(bound[0].1.column_id(), indexed_field.column_id());
    }

    #[test]
    fn test_missing_physical_column_uses_noop_builder() {
        crate::test_utils::init_test_globals().unwrap();

        let indexed_field = TableField::new_from_column_id(
            "indexed_col",
            TableDataType::Number(NumberDataType::Int64),
            20,
        );
        let spec = BloomGranuleIndexSpec {
            index_name: "idx".to_string(),
            index_version: "0".to_string(),
            bloom_index_type: BloomIndexType::Xor8,
            column_ids: vec![indexed_field.column_id()],
        };
        let unrelated_field = TableField::new_from_column_id(
            "other_col",
            TableDataType::Number(NumberDataType::Int64),
            30,
        );
        let physical_schema =
            TableSchema::new_from_column_ids(vec![unrelated_field], Default::default(), 31);
        let dal = Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();

        let mut builder = spec
            .new_builder(
                FunctionContext::default(),
                &physical_schema,
                "1/2/_b/block.parquet",
                dal,
            )
            .unwrap();
        let block = DataBlock::new_from_columns(vec![Int64Type::from_data(vec![1i64, 2])]);
        builder.push_rows(&block, 0..2).unwrap();
        builder.finalize_granule().unwrap();
        let output = builder.finalize().unwrap();

        assert!(output.sidecar_fields.is_empty());
        assert!(output.sidecar_columns.is_empty());
    }

    #[test]
    fn test_empty_builder_does_not_write_payload() {
        crate::test_utils::init_test_globals().unwrap();

        let field = TableField::new("a", TableDataType::Number(NumberDataType::Int64));
        let spec = BloomGranuleIndexSpec {
            index_name: "idx".to_string(),
            index_version: "01234567-89ab-cdef-0123-456789abcdef".to_string(),
            bloom_index_type: BloomIndexType::Xor8,
            column_ids: vec![field.column_id()],
        };
        let dal = Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        let schema = TableSchema::new(vec![field]);
        let out = spec
            .new_builder(
                FunctionContext::default(),
                &schema,
                "1/2/_b/block.parquet",
                dal,
            )
            .unwrap()
            .finalize()
            .unwrap();
        assert!(out.sidecar_fields.is_empty());
        assert!(out.sidecar_columns.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_granule_bloom_roundtrip() {
        crate::test_utils::init_test_globals().unwrap();

        let func_ctx = FunctionContext::default();
        let field = TableField::new("a", TableDataType::Number(NumberDataType::Int64));

        let spec = BloomGranuleIndexSpec {
            index_name: "idx".to_string(),
            index_version: "0".to_string(),
            bloom_index_type: BloomIndexType::Xor8,
            column_ids: vec![field.column_id()],
        };

        // The payload starts streaming at each granule boundary, so give the builder an in-memory
        // operator and read the finished file back to verify.
        let dal = Operator::new(opendal::services::Memory::default())
            .unwrap()
            .finish();
        let block_location = "1/2/_b/block.parquet";
        let payload_loc =
            TableMetaLocationGenerator::gen_granule_bloom_location_from_block_location(
                block_location,
                &spec.index_name,
                &spec.index_version,
                field.column_id(),
            );

        // Two granules of two rows each: [1,2] then [3,4].
        let block = DataBlock::new_from_columns(vec![Int64Type::from_data(vec![1i64, 2, 3, 4])]);
        let schema = TableSchema::new(vec![field.clone()]);
        let mut builder = spec
            .new_builder(func_ctx.clone(), &schema, block_location, dal.clone())
            .unwrap();
        builder.push_rows(&block, 0..2).unwrap();
        builder.finalize_granule().unwrap();
        builder.push_rows(&block, 2..4).unwrap();

        let out = builder.finalize().unwrap();

        // Payloads are streamed to storage, not returned; two offset columns (off + len) remain.
        assert_eq!(out.sidecar_fields.len(), 2);
        assert_eq!(out.sidecar_columns.len(), 2);

        // Extract the two granule page ranges from the offset columns.
        let offs = out.sidecar_columns[0].clone();
        let lens = out.sidecar_columns[1].clone();
        let offs = offs.into_number().unwrap().into_u_int64().unwrap();
        let lens = lens.into_number().unwrap().into_u_int64().unwrap();
        assert_eq!(offs.len(), 2);
        assert_eq!(lens.len(), 2);

        // Read the streamed payload file back from storage.
        let payload = dal.read(&payload_loc).await.unwrap().to_bytes();
        let digest = |v: i64| {
            BloomIndex::calculate_scalar_digest(
                &func_ctx,
                &Scalar::Number(databend_common_expression::types::number::NumberScalar::Int64(v)),
                &DataType::Number(NumberDataType::Int64),
            )
            .unwrap()
        };

        // Granule 0 contains 1,2 but not 3,4; granule 1 the reverse.
        let g0 = decode_page(&payload[offs[0] as usize..(offs[0] + lens[0]) as usize]).unwrap();
        assert!(g0.contains_digest(digest(1)));
        assert!(g0.contains_digest(digest(2)));

        let g1 = decode_page(&payload[offs[1] as usize..(offs[1] + lens[1]) as usize]).unwrap();
        assert!(g1.contains_digest(digest(3)));
        assert!(g1.contains_digest(digest(4)));
    }
}
