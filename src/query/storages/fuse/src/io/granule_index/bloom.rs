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

//! Granule Bloom index with one payload Parquet file per indexed column.

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
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_meta_app::schema::TableIndex;
use databend_storages_common_blocks::BulkParquetFileWriter;
use databend_storages_common_blocks::BulkParquetLeafWriter;
use databend_storages_common_blocks::MemoryBlockingWrite;
use databend_storages_common_blocks::build_parquet_writer_properties;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::BloomIndexBuilder;
use databend_storages_common_index::BloomIndexType;
use databend_storages_common_index::FilterEvalResult;
use databend_storages_common_index::filters::BlockFilter;
use databend_storages_common_index::filters::Filter;
use databend_storages_common_index::filters::FilterImpl;
use databend_storages_common_io::BLOCKING_WRITE_MAX_CHUNKS;
use databend_storages_common_io::OpenDalBlockingWrite;
use databend_storages_common_io::OperatorRangeReader;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_io::create_blocking_write;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::table::TableCompression;
use opendal::Buffer;
use opendal::Operator;
use parquet::arrow::arrow_writer::compute_leaves;

use super::GranuleIndexLowLevelColumnWriter;
use super::GranuleIndexLowLevelOutput;
use super::GranuleIndexLowLevelWriter;
use super::GranuleIndexPruner;
use super::GranuleIndexSpec;
use super::GranuleIndexWriter;
use super::GranuleMark;
use super::NoopGranuleIndexLowLevelColumnWriter;
use super::NoopGranuleIndexLowLevelWriter;
use super::NoopGranuleIndexWriter;
use super::PendingGranuleIndexOutput;
use super::PendingGranuleIndexPayload;
use crate::io::GranulePruningReadContext;
use crate::io::TableMetaLocationGenerator;
use crate::io::compact_index_version;

fn bloom_mark_names(index_version: &str, col_id: u32) -> (String, String) {
    let ver = compact_index_version(index_version);
    (
        format!("gbloom_{ver}_off_{col_id}"),
        format!("gbloom_{ver}_len_{col_id}"),
    )
}

const PAYLOAD_FILTER_COL: &str = "f";

#[derive(Clone)]
pub struct BloomGranuleIndexSpec {
    index_name: String,
    index_version: String,
    bloom_index_type: BloomIndexType,
    column_ids: Vec<ColumnId>,
}

impl BloomGranuleIndexSpec {
    pub fn try_create(
        index_name: &str,
        index: &TableIndex,
        schema: &TableSchema,
        bloom_index_type: BloomIndexType,
    ) -> Result<Option<Self>> {
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

    fn bind_columns(&self, physical_schema: &TableSchema) -> Option<Vec<Option<TableField>>> {
        let all_columns_bound = self.column_ids.iter().all(|column_id| {
            physical_schema.fields().iter().any(|field| {
                field.column_id() == *column_id && is_bloom_supported_type(field.data_type())
            })
        });
        if !all_columns_bound {
            return None;
        }
        Some(
            physical_schema
                .fields()
                .iter()
                .map(|field| {
                    self.column_ids
                        .contains(&field.column_id())
                        .then(|| field.clone())
                })
                .collect(),
        )
    }
}

impl GranuleIndexSpec for BloomGranuleIndexSpec {
    fn new_writer(
        &self,
        func_ctx: FunctionContext,
        physical_schema: &TableSchema,
        block_location: &str,
    ) -> Result<Box<dyn GranuleIndexWriter>> {
        let Some(bound_columns) = self.bind_columns(physical_schema) else {
            log::debug!(
                "Ignoring granule bloom index {} while writing: not all indexed columns exist in the physical schema",
                self.index_name
            );
            return Ok(Box::new(NoopGranuleIndexWriter));
        };
        let columns = bound_columns
            .into_iter()
            .enumerate()
            .filter_map(|(field_index, field)| {
                field.map(|field| {
                    let location =
                        TableMetaLocationGenerator::gen_granule_bloom_location_from_block_location(
                            block_location,
                            &self.index_name,
                            &self.index_version,
                            field.column_id(),
                        );
                    let write = MemoryBlockingWrite::default();
                    (
                        field_index,
                        PendingColumnPayloadState::new(field, location, write),
                    )
                })
            })
            .collect();
        Ok(Box::new(BloomGranuleIndexWriter {
            func_ctx,
            bloom_index_type: self.bloom_index_type,
            index_version: self.index_version.clone(),
            columns,
            output: PendingGranuleIndexOutput::default(),
        }))
    }

    fn low_level_blocking_writers(&self, physical_schema: &TableSchema) -> usize {
        self.bind_columns(physical_schema)
            .map(|columns| columns.into_iter().flatten().count())
            .unwrap_or(0)
    }

    fn new_low_level_writer(
        &self,
        func_ctx: FunctionContext,
        physical_schema: &TableSchema,
        block_location: &str,
        dal: Operator,
        granule_rows: usize,
    ) -> Result<Box<dyn GranuleIndexLowLevelWriter>> {
        let Some(bound_columns) = self.bind_columns(physical_schema) else {
            log::debug!(
                "Ignoring granule bloom index {} while writing: not all indexed columns exist in the physical schema",
                self.index_name
            );
            return Ok(Box::new(NoopGranuleIndexLowLevelWriter::new(
                physical_schema.num_fields(),
            )));
        };
        let columns = bound_columns
            .into_iter()
            .map(|field| {
                field.map(|field| {
                    let location =
                        TableMetaLocationGenerator::gen_granule_bloom_location_from_block_location(
                            block_location,
                            &self.index_name,
                            &self.index_version,
                            field.column_id(),
                        );
                    let write =
                        create_blocking_write(dal.clone(), location, BLOCKING_WRITE_MAX_CHUNKS);
                    (field, write)
                })
            })
            .collect();
        Ok(Box::new(BloomGranuleIndexLowLevelWriter {
            func_ctx,
            bloom_index_type: self.bloom_index_type,
            index_version: self.index_version.clone(),
            granule_rows,
            columns,
            next_column: 0,
            output: GranuleIndexLowLevelOutput::default(),
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

fn payload_table_schema() -> TableSchema {
    TableSchema::new(vec![TableField::new(
        PAYLOAD_FILTER_COL,
        TableDataType::Nullable(Box::new(TableDataType::Binary)),
    )])
}

fn payload_arrow_field() -> Arc<arrow_schema::Field> {
    let arrow_schema: arrow_schema::Schema = (&payload_table_schema()).into();
    arrow_schema.fields()[0].clone()
}

fn new_payload_writer<W: databend_storages_common_blocks::BlockingWrite + 'static>(
    writer: W,
) -> Result<BulkParquetFileWriter<W>> {
    let table_schema = payload_table_schema();
    let arrow_schema = Arc::new((&table_schema).into());
    let props = Arc::new(build_parquet_writer_properties(
        TableCompression::None,
        false, // no dictionary: flushed pages must reach the writer immediately
        None::<&StatisticsOfColumns>,
        None,
        0,
        &table_schema,
        None, // page boundaries are controlled explicitly per granule
        None,
    ));
    BulkParquetFileWriter::create(writer, arrow_schema, props)
}

struct PendingColumnPayloadState {
    field: TableField,
    col_id: u32,
    location: String,
    payload_field: Arc<arrow_schema::Field>,
    payload_write: Option<MemoryBlockingWrite>,
    payload_writer: Option<BulkParquetLeafWriter<MemoryBlockingWrite>>,
    granules_written: usize,
    current: Option<BloomIndexBuilder>,
}

impl PendingColumnPayloadState {
    fn new(field: TableField, location: String, payload_write: MemoryBlockingWrite) -> Self {
        let col_id = field.column_id();
        Self {
            field,
            col_id,
            location,
            payload_field: payload_arrow_field(),
            payload_write: Some(payload_write),
            payload_writer: None,
            granules_written: 0,
            current: None,
        }
    }

    fn current_builder(
        &mut self,
        func_ctx: &FunctionContext,
        ty: BloomIndexType,
    ) -> Result<&mut BloomIndexBuilder> {
        current_builder(&self.field, &mut self.current, func_ctx, ty)
    }

    fn write_filter(&mut self, filter_bytes: Option<Vec<u8>>) -> Result<()> {
        write_filter(
            &self.payload_field,
            &mut self.payload_write,
            &mut self.payload_writer,
            &mut self.granules_written,
            filter_bytes,
        )
    }
}

struct ColumnPayloadState {
    field: TableField,
    col_id: u32,
    payload_field: Arc<arrow_schema::Field>,
    payload_write: Option<OpenDalBlockingWrite>,
    payload_writer: Option<BulkParquetLeafWriter<OpenDalBlockingWrite>>,
    granules_written: usize,
    current: Option<BloomIndexBuilder>,
}

impl ColumnPayloadState {
    fn new(field: TableField, payload_write: OpenDalBlockingWrite) -> Self {
        let col_id = field.column_id();
        Self {
            field,
            col_id,
            payload_field: payload_arrow_field(),
            payload_write: Some(payload_write),
            payload_writer: None,
            granules_written: 0,
            current: None,
        }
    }

    fn current_builder(
        &mut self,
        func_ctx: &FunctionContext,
        ty: BloomIndexType,
    ) -> Result<&mut BloomIndexBuilder> {
        current_builder(&self.field, &mut self.current, func_ctx, ty)
    }

    fn write_filter(&mut self, filter_bytes: Option<Vec<u8>>) -> Result<()> {
        write_filter(
            &self.payload_field,
            &mut self.payload_write,
            &mut self.payload_writer,
            &mut self.granules_written,
            filter_bytes,
        )
    }
}

fn current_builder<'a>(
    field: &TableField,
    current: &'a mut Option<BloomIndexBuilder>,
    func_ctx: &FunctionContext,
    ty: BloomIndexType,
) -> Result<&'a mut BloomIndexBuilder> {
    if current.is_none() {
        let mut cols = std::collections::BTreeMap::new();
        cols.insert(0usize, field.clone());
        *current = Some(BloomIndexBuilder::create(func_ctx.clone(), ty, cols, &[])?);
    }
    Ok(current.as_mut().expect("current builder initialized above"))
}

fn write_filter<W: databend_storages_common_blocks::BlockingWrite + 'static>(
    payload_field: &Arc<arrow_schema::Field>,
    payload_write: &mut Option<W>,
    payload_writer: &mut Option<BulkParquetLeafWriter<W>>,
    granules_written: &mut usize,
    filter_bytes: Option<Vec<u8>>,
) -> Result<()> {
    if payload_writer.is_none() {
        let Some(write) = payload_write.take() else {
            return Err(ErrorCode::Internal(
                "granule bloom payload write has already been consumed",
            ));
        };
        *payload_writer = Some(new_payload_writer(write)?.next_leaf()?);
    }
    let column = build_single_binary_column(filter_bytes);
    let array = ArrayRef::from(&column);
    let leaves = compute_leaves(payload_field, &array)?;
    if leaves.len() != 1 {
        return Err(ErrorCode::Internal(format!(
            "granule bloom payload expected one parquet leaf, got {}",
            leaves.len()
        )));
    }
    let writer = payload_writer.as_mut().expect("payload writer");
    writer.write(&leaves[0])?;
    writer.flush_page()?;
    *granules_written += 1;
    Ok(())
}

struct BloomGranuleIndexWriter {
    func_ctx: FunctionContext,
    bloom_index_type: BloomIndexType,
    index_version: String,
    columns: Vec<(usize, PendingColumnPayloadState)>,
    output: PendingGranuleIndexOutput,
}

impl GranuleIndexWriter for BloomGranuleIndexWriter {
    fn write(&mut self, block: &DataBlock, range: std::ops::Range<usize>) -> Result<()> {
        if range.is_empty() {
            return Ok(());
        }
        for (field_index, payload) in &mut self.columns {
            let column = block.get_by_offset(*field_index).to_column();
            let sub = DataBlock::new_from_columns(vec![column.slice(range.clone())]);
            payload
                .current_builder(&self.func_ctx, self.bloom_index_type)?
                .add_block(&sub)?;
        }
        Ok(())
    }

    fn finish_granule(&mut self) -> Result<()> {
        for (_, payload) in &mut self.columns {
            let filter_bytes = finalize_filter(payload.current.take())?;
            payload.write_filter(filter_bytes)?;
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<PendingGranuleIndexOutput> {
        if self
            .columns
            .iter()
            .any(|(_, payload)| payload.current.is_some())
        {
            self.finish_granule()?;
        }
        for (_, payload) in self.columns {
            self.output
                .merge(finish_pending_payload(payload, &self.index_version)?)?;
        }
        Ok(self.output)
    }
}

struct BloomGranuleIndexLowLevelWriter {
    func_ctx: FunctionContext,
    bloom_index_type: BloomIndexType,
    index_version: String,
    granule_rows: usize,
    columns: Vec<Option<(TableField, OpenDalBlockingWrite)>>,
    next_column: usize,
    output: GranuleIndexLowLevelOutput,
}

impl GranuleIndexLowLevelWriter for BloomGranuleIndexLowLevelWriter {
    fn next_column(mut self: Box<Self>) -> Result<Box<dyn GranuleIndexLowLevelColumnWriter>> {
        let index = self.next_column;
        if index >= self.columns.len() {
            return Err(ErrorCode::Internal(
                "granule bloom low-level writer has no remaining columns",
            ));
        }
        self.next_column += 1;
        let slot = self.columns[index].take();
        let Some((field, payload_write)) = slot else {
            return Ok(Box::new(NoopGranuleIndexLowLevelColumnWriter::new(self)));
        };
        Ok(Box::new(BloomGranuleIndexLowLevelColumnWriter {
            parent: Some(self),
            rows_in_granule: 0,
            payload: ColumnPayloadState::new(field, payload_write),
        }))
    }

    fn finish(self: Box<Self>) -> Result<GranuleIndexLowLevelOutput> {
        if self.next_column != self.columns.len() {
            return Err(ErrorCode::Internal(format!(
                "granule bloom low-level writer consumed {} of {} columns",
                self.next_column,
                self.columns.len()
            )));
        }
        Ok(self.output)
    }
}

struct BloomGranuleIndexLowLevelColumnWriter {
    parent: Option<Box<BloomGranuleIndexLowLevelWriter>>,
    rows_in_granule: usize,
    payload: ColumnPayloadState,
}

fn finalize_filter(builder: Option<BloomIndexBuilder>) -> Result<Option<Vec<u8>>> {
    match builder {
        Some(mut builder) => match builder.finalize()? {
            Some(bloom) if !bloom.filters.is_empty() => Ok(Some(bloom.filters[0].to_bytes()?)),
            _ => Ok(None),
        },
        None => Ok(None),
    }
}

fn finish_pending_payload(
    mut payload: PendingColumnPayloadState,
    index_version: &str,
) -> Result<PendingGranuleIndexOutput> {
    let granules = payload.granules_written;
    if granules == 0 {
        return Ok(PendingGranuleIndexOutput::default());
    }
    let leaf = payload
        .payload_writer
        .take()
        .expect("non-empty payload has writer");
    let writer = leaf.finish()?;
    let (metadata, write) = writer.finish()?;
    let (offs, lens) = page_offsets_from_metadata(&metadata, granules)?;
    let (off_name, len_name) = bloom_mark_names(index_version, payload.col_id);
    Ok(PendingGranuleIndexOutput {
        marks: vec![
            GranuleMark::create(&off_name, offs),
            GranuleMark::create(&len_name, lens),
        ],
        pending_payloads: vec![PendingGranuleIndexPayload {
            location: (payload.location, 0),
            data: opendal::Buffer::from(write.into_chunks()),
        }],
    })
}

fn finish_payload(
    mut payload: ColumnPayloadState,
    index_version: &str,
) -> Result<GranuleIndexLowLevelOutput> {
    let granules = payload.granules_written;
    if granules == 0 {
        return Ok(GranuleIndexLowLevelOutput::default());
    }
    let leaf = payload
        .payload_writer
        .take()
        .expect("non-empty payload has writer");
    let writer = leaf.finish()?;
    let (metadata, _) = writer.finish()?;
    let (offs, lens) = page_offsets_from_metadata(&metadata, granules)?;
    let (off_name, len_name) = bloom_mark_names(index_version, payload.col_id);
    Ok(GranuleIndexLowLevelOutput {
        marks: vec![
            GranuleMark::create(&off_name, offs),
            GranuleMark::create(&len_name, lens),
        ],
    })
}

impl BloomGranuleIndexLowLevelColumnWriter {
    fn finish_granule(&mut self) -> Result<()> {
        let filter_bytes = finalize_filter(self.payload.current.take())?;
        self.payload.write_filter(filter_bytes)
    }
}

impl GranuleIndexLowLevelColumnWriter for BloomGranuleIndexLowLevelColumnWriter {
    fn write(&mut self, column: &Column) -> Result<()> {
        let parent = self
            .parent
            .as_ref()
            .ok_or_else(|| ErrorCode::Internal("granule bloom column writer has no parent"))?;
        let granule_rows = parent.granule_rows;
        let func_ctx = parent.func_ctx.clone();
        let bloom_index_type = parent.bloom_index_type;
        let mut offset = 0;
        while offset < column.len() {
            let take = (granule_rows - self.rows_in_granule).min(column.len() - offset);
            let sub = DataBlock::new_from_columns(vec![column.slice(offset..offset + take)]);
            self.payload
                .current_builder(&func_ctx, bloom_index_type)?
                .add_block(&sub)?;
            offset += take;
            self.rows_in_granule += take;
            if self.rows_in_granule == granule_rows {
                self.finish_granule()?;
                self.rows_in_granule = 0;
            }
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<Box<dyn GranuleIndexLowLevelWriter>> {
        if self.rows_in_granule != 0 {
            self.finish_granule()?;
        }
        let mut parent = self
            .parent
            .take()
            .ok_or_else(|| ErrorCode::Internal("granule bloom column writer has no parent"))?;
        let output = finish_payload(self.payload, &parent.index_version)?;
        parent.output.merge(output)?;
        Ok(parent)
    }
}

fn build_single_binary_column(filter_bytes: Option<Vec<u8>>) -> Column {
    use databend_common_expression::types::BinaryType;
    BinaryType::from_opt_data(vec![filter_bytes])
}

fn page_offsets_from_metadata(
    metadata: &parquet::file::metadata::ParquetMetaData,
    granules: usize,
) -> Result<(Vec<u64>, Vec<u64>)> {
    let offset_index = metadata.offset_index().ok_or_else(|| {
        ErrorCode::Internal("granule bloom payload has no offset index in metadata")
    })?;
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

struct MatchedColumn {
    field: TableField,
    index_name: String,
    index_version: String,
}

struct BloomPayloadReader {
    filter_field: TableField,
    reader: OperatorRangeReader,
}

type LoadedBloomFilters = Vec<(TableField, Arc<FilterImpl>)>;
type BloomSurvivor = (usize, LoadedBloomFilters);

impl BloomPayloadReader {
    fn next_filter(&mut self) -> Result<Option<FilterImpl>> {
        let page = self.reader.read()?;
        if page.is_empty() {
            return Ok(None);
        }
        decode_filter_from_page(page)
    }
}

pub struct BloomGranuleIndexPruner {
    func_ctx: FunctionContext,
    filter_expr: Expr<String>,
    eq_scalar_map: HashMap<Scalar, u64>,
    matched_columns: Vec<MatchedColumn>,
    data_schema: TableSchemaRef,
    dal: Operator,
    settings: ReadSettings,
}

impl BloomGranuleIndexPruner {
    fn required_mark_names(&self) -> Vec<String> {
        let mut names = Vec::with_capacity(self.matched_columns.len() * 2);
        for col in &self.matched_columns {
            let (off_name, len_name) = bloom_mark_names(&col.index_version, col.field.column_id());
            names.push(off_name);
            names.push(len_name);
        }
        names
    }

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

    fn try_prune(
        &self,
        block_meta: &BlockMeta,
        input_ranges: &[Range<usize>],
        read_ctx: &GranulePruningReadContext,
    ) -> Result<Vec<Range<usize>>> {
        let num_granules = read_ctx.num_granules();

        if num_granules == 0 {
            return Ok(input_ranges.to_vec());
        }

        let survivors = input_ranges
            .iter()
            .flat_map(|range| range.clone())
            .collect::<Vec<_>>();
        if survivors.is_empty() {
            return Ok(Vec::new());
        }

        let wanted_names = self.required_mark_names();
        let mut marks = HashMap::with_capacity(wanted_names.len());
        for name in &wanted_names {
            if let Some(mark) = read_ctx.mark(name) {
                marks.insert(name.clone(), mark);
            }
        }

        let empty_stats = StatisticsOfColumns::new();
        let column_stats = block_meta.col_stats.clone();
        let stats = if column_stats.is_empty() {
            &empty_stats
        } else {
            &column_stats
        };
        let mut survivors = survivors
            .into_iter()
            .map(|granule| (granule, Vec::new()))
            .collect::<Vec<BloomSurvivor>>();
        let mut any_column_has_payload = false;

        for col in &self.matched_columns {
            let col_id = col.field.column_id();
            let (off_name, len_name) = bloom_mark_names(&col.index_version, col_id);
            let (Some(offs), Some(lens)) = (marks.get(&off_name), marks.get(&len_name)) else {
                continue;
            };
            any_column_has_payload = true;

            survivors =
                self.prune_column(&block_meta.location.0, col, offs, lens, survivors, stats)?;
            if survivors.is_empty() {
                break;
            }
        }

        if !any_column_has_payload {
            return Err(ErrorCode::Internal(
                "granule bloom payload marks are missing for this block",
            ));
        }

        Ok(coalesce(
            &survivors
                .into_iter()
                .map(|(granule, _)| granule)
                .collect::<Vec<_>>(),
        ))
    }

    fn prune_column(
        &self,
        block_location: &str,
        col: &MatchedColumn,
        offsets: &[u64],
        lengths: &[u64],
        survivors: Vec<BloomSurvivor>,
        stats: &StatisticsOfColumns,
    ) -> Result<Vec<BloomSurvivor>> {
        let byte_ranges = bloom_byte_ranges(&survivors, offsets, lengths);
        if byte_ranges.iter().all(Range::is_empty) {
            return Ok(survivors);
        }

        let payload_loc =
            TableMetaLocationGenerator::gen_granule_bloom_location_from_block_location(
                block_location,
                &col.index_name,
                &col.index_version,
                col.field.column_id(),
            );
        let reader = OperatorRangeReader::create(
            &self.settings,
            self.dal.clone(),
            payload_loc,
            &byte_ranges,
            1,
        )?;
        let filter_name = BloomIndex::build_filter_bloom_name(BlockFilter::VERSION, &col.field)?;
        let mut reader = BloomPayloadReader {
            filter_field: TableField::new(&filter_name, TableDataType::Binary),
            reader,
        };

        let mut next_survivors = Vec::with_capacity(survivors.len());
        for (granule, mut loaded_filters) in survivors {
            if let Some(filter) = reader.next_filter()? {
                loaded_filters.push((reader.filter_field.clone(), Arc::new(filter)));
            }
            if loaded_filters.is_empty() || self.may_match(&loaded_filters, stats)? {
                next_survivors.push((granule, loaded_filters));
            }
        }
        Ok(next_survivors)
    }

    fn may_match(
        &self,
        loaded_filters: &[(TableField, Arc<FilterImpl>)],
        stats: &StatisticsOfColumns,
    ) -> Result<bool> {
        let (fields, filters): (Vec<_>, Vec<_>) = loaded_filters.iter().cloned().unzip();
        let bloom_index = BloomIndex::from_filter_block(
            self.func_ctx.clone(),
            Arc::new(TableSchema::new(fields)),
            filters,
            BlockFilter::VERSION,
        )?;
        Ok(bloom_index.apply(
            self.filter_expr.clone(),
            &self.eq_scalar_map,
            &HashMap::new(),
            &[],
            stats,
            self.data_schema.clone(),
        )? != FilterEvalResult::MustFalse)
    }
}

impl GranuleIndexPruner for BloomGranuleIndexPruner {
    fn name(&self) -> &'static str {
        super::GRANULE_BLOOM_INDEX_NAME
    }

    fn required_marks(&self) -> Vec<String> {
        self.required_mark_names()
    }

    fn prune_granules(
        &self,
        block_meta: &BlockMeta,
        input: &[Range<usize>],
        read_ctx: &GranulePruningReadContext,
    ) -> Result<Vec<Range<usize>>> {
        self.try_prune(block_meta, input, read_ctx)
    }
}

fn bloom_byte_ranges(
    survivors: &[BloomSurvivor],
    offsets: &[u64],
    lengths: &[u64],
) -> Vec<Range<u64>> {
    survivors
        .iter()
        .map(|(granule, _)| {
            let (start, len) = (offsets[*granule], lengths[*granule]);
            start..start + len
        })
        .collect()
}

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
    fn test_next_column_ranges_only_include_current_survivors() {
        let survivors = vec![(1, Vec::new()), (3, Vec::new())];
        let offsets = [0, 10, 20, 30, 40];
        let lengths = [1, 2, 3, 4, 5];

        assert_eq!(bloom_byte_ranges(&survivors, &offsets, &lengths), vec![
            10..12,
            30..34
        ]);
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

        let physical_schema =
            TableSchema::new_from_column_ids(vec![indexed_field.clone()], Default::default(), 21);
        let bound = spec.bind_columns(&physical_schema).unwrap();

        assert_eq!(bound.len(), 1);
        assert_eq!(
            bound[0].as_ref().unwrap().column_id(),
            indexed_field.column_id()
        );
        assert_eq!(spec.low_level_blocking_writers(&physical_schema), 1);

        let unrelated_schema = TableSchema::new_from_column_ids(
            vec![TableField::new_from_column_id(
                "other_col",
                TableDataType::Number(NumberDataType::Int64),
                30,
            )],
            Default::default(),
            31,
        );
        assert_eq!(spec.low_level_blocking_writers(&unrelated_schema), 0);
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

        let writer = spec
            .new_low_level_writer(
                FunctionContext::default(),
                &physical_schema,
                "1/2/_b/block.parquet",
                dal,
                2,
            )
            .unwrap();
        let mut column = writer.next_column().unwrap();
        column.write(&Int64Type::from_data(vec![1i64, 2])).unwrap();
        let writer = column.finish().unwrap();
        assert!(writer.finish().unwrap().marks.is_empty());
    }

    #[test]
    fn test_granule_pending_output_rejects_duplicate_marks() {
        let mark = |name: &str| GranuleMark::create(name, vec![1]);
        let mut output = PendingGranuleIndexOutput {
            marks: vec![mark("duplicate")],
            ..Default::default()
        };
        let error = output
            .merge(PendingGranuleIndexOutput {
                marks: vec![mark("duplicate")],
                ..Default::default()
            })
            .unwrap_err();
        assert!(error.message().contains("duplicate granule mark"));
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
        let writer = spec
            .new_low_level_writer(
                FunctionContext::default(),
                &schema,
                "1/2/_b/block.parquet",
                dal,
                2,
            )
            .unwrap();
        let column = writer.next_column().unwrap();
        let writer = column.finish().unwrap();
        assert!(writer.finish().unwrap().marks.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_granule_bloom_returns_pending_payload() {
        crate::test_utils::init_test_globals().unwrap();

        let field = TableField::new("a", TableDataType::Number(NumberDataType::Int64));
        let spec = BloomGranuleIndexSpec {
            index_name: "idx".to_string(),
            index_version: "0".to_string(),
            bloom_index_type: BloomIndexType::Xor8,
            column_ids: vec![field.column_id()],
        };
        let block_location = "1/2/_b/block.parquet";
        let payload_loc =
            TableMetaLocationGenerator::gen_granule_bloom_location_from_block_location(
                block_location,
                &spec.index_name,
                &spec.index_version,
                field.column_id(),
            );
        let schema = TableSchema::new(vec![field]);
        let block = DataBlock::new_from_columns(vec![Int64Type::from_data(vec![1i64, 2, 3])]);
        let mut writer = spec
            .new_writer(FunctionContext::default(), &schema, block_location)
            .unwrap();
        writer.write(&block, 0..2).unwrap();
        writer.finish_granule().unwrap();
        writer.write(&block, 2..3).unwrap();
        let output = writer.finish().unwrap();

        assert_eq!(output.marks.len(), 2);
        assert_eq!(output.pending_payloads.len(), 1);
        assert_eq!(output.pending_payloads[0].location.0, payload_loc);
        assert!(!output.pending_payloads[0].data.is_empty());
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

        let column = Int64Type::from_data(vec![1i64, 2, 3, 4]);
        let schema = TableSchema::new(vec![field.clone()]);
        let writer = spec
            .new_low_level_writer(func_ctx.clone(), &schema, block_location, dal.clone(), 2)
            .unwrap();
        let mut column_writer = writer.next_column().unwrap();
        column_writer.write(&column.slice(0..2)).unwrap();
        column_writer.write(&column.slice(2..4)).unwrap();

        let writer = column_writer.finish().unwrap();
        let out = writer.finish().unwrap();

        assert_eq!(out.marks.len(), 2);

        let offs = out.marks[0].values.clone();
        let lens = out.marks[1].values.clone();
        let offs = offs.into_number().unwrap().into_u_int64().unwrap();
        let lens = lens.into_number().unwrap().into_u_int64().unwrap();
        assert_eq!(offs.len(), 2);
        assert_eq!(lens.len(), 2);

        let payload = dal.read(&payload_loc).await.unwrap().to_bytes();
        let digest = |v: i64| {
            BloomIndex::calculate_scalar_digest(
                &func_ctx,
                &Scalar::Number(databend_common_expression::types::number::NumberScalar::Int64(v)),
                &DataType::Number(NumberDataType::Int64),
            )
            .unwrap()
        };

        let g0 = decode_page(&payload[offs[0] as usize..(offs[0] + lens[0]) as usize]).unwrap();
        assert!(g0.contains_digest(digest(1)));
        assert!(g0.contains_digest(digest(2)));

        let g1 = decode_page(&payload[offs[1] as usize..(offs[1] + lens[1]) as usize]).unwrap();
        assert!(g1.contains_digest(digest(3)));
        assert!(g1.contains_digest(digest(4)));

        let settings = ReadSettings {
            max_gap_size: 48,
            max_range_size: 1024 * 1024,
            parquet_fast_read_bytes: 0,
        };
        let ranges = vec![
            offs[0]..offs[0] + lens[0],
            offs[1]..offs[1],
            offs[1]..offs[1] + lens[1],
        ];
        let new_reader = || BloomPayloadReader {
            filter_field: TableField::new("f", TableDataType::Binary),
            reader: OperatorRangeReader::create(
                &settings,
                dal.clone(),
                payload_loc.clone(),
                &ranges,
                1,
            )
            .unwrap(),
        };
        let mut first_column = new_reader();
        let mut second_column = new_reader();
        for expected in [Some([1, 2]), None, Some([3, 4])] {
            let first = first_column.next_filter().unwrap();
            let second = second_column.next_filter().unwrap();
            match expected {
                Some(values) => {
                    let first = first.unwrap();
                    let second = second.unwrap();
                    for value in values {
                        assert!(first.contains_digest(digest(value)));
                        assert!(second.contains_digest(digest(value)));
                    }
                }
                None => {
                    assert!(first.is_none());
                    assert!(second.is_none());
                }
            }
        }
        assert!(first_column.reader.read().is_err());
        assert!(second_column.reader.read().is_err());
    }
}
