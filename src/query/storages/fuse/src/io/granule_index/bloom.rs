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
use databend_common_expression::FieldIndex;
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
use databend_storages_common_blocks::build_parquet_writer_properties;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::BloomIndexBuilder;
use databend_storages_common_index::BloomIndexType;
use databend_storages_common_index::FilterEvalResult;
use databend_storages_common_index::filters::BlockFilter;
use databend_storages_common_index::filters::Filter;
use databend_storages_common_index::filters::FilterImpl;
use databend_storages_common_io::BlockingOperatorWriter;
use databend_storages_common_io::OperatorRangeReader;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
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
use super::GranuleMark;
use super::NoopGranuleIndexBuilder;
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

fn new_payload_writer(
    sink: BlockingOperatorWriter,
) -> Result<BulkParquetFileWriter<BlockingOperatorWriter>> {
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

struct ColumnPayloadState {
    field_index: FieldIndex,
    field: TableField,
    col_id: u32,
    payload_field: Arc<arrow_schema::Field>,
    payload_writer: Option<BulkParquetLeafWriter<BlockingOperatorWriter>>,
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
        let sink = BlockingOperatorWriter::create(dal, location, 2);
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
        let has_open = self.columns.iter().any(|c| c.current.is_some());
        if has_open {
            self.finalize_granule()?;
        }

        if self.columns.is_empty() || self.columns[0].granules_written == 0 {
            return Ok(GranuleIndexBuildOutput::default());
        }

        let index_version = self.index_version;
        let mut marks = Vec::with_capacity(self.columns.len() * 2);

        for mut col in self.columns {
            let granules = col.granules_written;
            let leaf = col.payload_writer.take().expect("payload writer");
            let writer = leaf.finish()?;
            let (metadata, sink) = writer.finish()?;
            let (offs, lens) = page_offsets_from_metadata(&metadata, granules)?;
            sink.close()?;

            let (off_name, len_name) = bloom_mark_names(&index_version, col.col_id);
            marks.push(GranuleMark::create(&off_name, offs));
            marks.push(GranuleMark::create(&len_name, lens));
        }

        Ok(GranuleIndexBuildOutput { marks })
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

        assert!(output.marks.is_empty());
    }

    #[test]
    fn test_granule_index_output_rejects_duplicate_marks() {
        let mark = |name: &str| GranuleMark::create(name, vec![1]);
        let mut output = GranuleIndexBuildOutput {
            marks: vec![mark("duplicate")],
        };
        let error = output
            .merge(GranuleIndexBuildOutput {
                marks: vec![mark("duplicate")],
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
        assert!(out.marks.is_empty());
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

        let block = DataBlock::new_from_columns(vec![Int64Type::from_data(vec![1i64, 2, 3, 4])]);
        let schema = TableSchema::new(vec![field.clone()]);
        let mut builder = spec
            .new_builder(func_ctx.clone(), &schema, block_location, dal.clone())
            .unwrap();
        builder.push_rows(&block, 0..2).unwrap();
        builder.finalize_granule().unwrap();
        builder.push_rows(&block, 2..4).unwrap();

        let out = builder.finalize().unwrap();

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
