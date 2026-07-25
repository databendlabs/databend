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

//! Independent vertical recluster executor.
//!
//! `SortBlocks` owns one source and one output writer at a time. `MergeBlocks`
//! builds a task-wide compressed row mapping from key-only inputs, then rewrites
//! one physical field at a time. This module deliberately does not share the
//! horizontal sort/compact/serialize processors.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use databend_common_catalog::plan::ReclusterTask;
use databend_common_catalog::plan::VerticalReclusterKind;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::ChunkIndex;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataBlockVec;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::ORIGIN_BLOCK_ID_COLUMN_ID;
use databend_common_expression::ORIGIN_BLOCK_ROW_NUM_COLUMN_ID;
use databend_common_expression::ORIGIN_VERSION_COLUMN_ID;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::compare_columns;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::decimal::DecimalScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline::sources::SyncSource;
use databend_common_sql::DefaultExprBinder;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::parse_cluster_keys;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::Versioned;

use crate::FuseBlockPartInfo;
use crate::FuseStorageFormat;
use crate::FuseTable;
use crate::io::FuseBlockWriteOptions;
use crate::io::FuseLowLevelBlockReadOptions;
use crate::io::FuseLowLevelBlockReader;
use crate::io::FuseLowLevelBlockWriter;
use crate::io::FuseLowLevelColumnReader;
use crate::operations::MutationLogEntry;
use crate::operations::MutationLogs;

const MAPPING_SAFETY_BYTES: usize = 256 * 1024;

enum SortColumnSource {
    Physical(FuseLowLevelColumnReader),
    Default { scalar: Scalar, data_type: DataType },
    Origin,
}

struct SortColumnInput {
    source: SortColumnSource,
    column_id: u32,
    source_location: String,
    table_version: u64,
    position: usize,
    expected_rows: usize,
}

impl SortColumnInput {
    fn read(&mut self, range: Range<usize>) -> Result<Column> {
        if range.start != self.position || range.end > self.expected_rows {
            return Err(ErrorCode::BadArguments(format!(
                "SortBlocks column {} expected range {}..={}, got {:?}",
                self.column_id, self.position, self.expected_rows, range
            )));
        }

        let rows = range.len();
        let column = match &mut self.source {
            SortColumnSource::Physical(reader) => {
                let persisted = reader.read(rows)?;

                if is_origin_column(self.column_id) {
                    materialize_stream_field(
                        self.column_id,
                        Some(persisted),
                        &self.source_location,
                        range.clone(),
                        self.table_version,
                    )?
                } else {
                    persisted
                }
            }
            SortColumnSource::Default { scalar, data_type } => {
                ColumnBuilder::repeat(&scalar.as_ref(), rows, data_type).build()
            }
            SortColumnSource::Origin => materialize_stream_field(
                self.column_id,
                None,
                &self.source_location,
                range.clone(),
                self.table_version,
            )?,
        };
        self.position = range.end;
        Ok(column)
    }

    fn finish(self) -> Result<()> {
        if self.position != self.expected_rows {
            return Err(ErrorCode::BadArguments(format!(
                "SortBlocks column {} returned {} of {} rows",
                self.column_id, self.position, self.expected_rows
            )));
        }
        if let SortColumnSource::Physical(reader) = self.source {
            reader.finish()?.finish()?;
        }
        Ok(())
    }
}

pub struct VerticalReclusterSource {
    ctx: Arc<dyn TableContext>,
    table: FuseTable,
    task: ReclusterTask,
    table_meta_timestamps: TableMetaTimestamps,
    emitted: bool,
}

impl VerticalReclusterSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        table: FuseTable,
        task: ReclusterTask,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Self {
        Self {
            ctx,
            table,
            task,
            table_meta_timestamps,
            emitted: false,
        }
    }

    fn execute(&self) -> Result<DataBlock> {
        let Some(kind) = &self.task.vertical_kind else {
            return Err(ErrorCode::Internal(
                "vertical recluster source received a horizontal task",
            ));
        };

        if self.table.get_write_settings().storage_format != FuseStorageFormat::Parquet {
            return Err(ErrorCode::Unimplemented(
                "vertical recluster supports Parquet FUSE blocks only".to_string(),
            ));
        }

        let schema = self.table.schema_with_stream();
        let schema = Arc::new(schema.remove_virtual_computed_fields());

        let source_metas = self.source_metas()?;
        let key_exprs = self.cluster_key_exprs(schema.clone())?;

        if key_exprs.is_empty() {
            return Err(ErrorCode::Unimplemented(
                "vertical recluster can't supports empty keys".to_string(),
            ));
        }

        let mut key_types = Vec::with_capacity(key_exprs.len());
        let mut key_fields = HashSet::new();
        for key_expr in key_exprs {
            let data_type = key_expr.data_type().clone();
            if data_type.remove_nullable().is_vector() {
                return Err(ErrorCode::Unimplemented(
                    "vertical recluster supports pure scalar cluster keys only".to_string(),
                ));
            }

            key_types.push(data_type);
            key_fields.extend(key_expr.column_refs().into_keys());
        }

        let output_rows = self.table.get_block_thresholds().max_rows_per_block.max(1);

        let block_options = FuseBlockWriteOptions::try_create(
            self.ctx.clone(),
            &self.table,
            MutationKind::Recluster,
            self.table_meta_timestamps,
        )?;

        let outputs = match kind {
            VerticalReclusterKind::SortBlocks => {
                if source_metas.len() != 1 {
                    return Err(ErrorCode::Internal(format!(
                        "SortBlocks requires one source, got {}",
                        source_metas.len()
                    )));
                }

                let source_meta = source_metas.into_iter().next().unwrap();
                self.execute_sort_blocks(
                    schema,
                    source_meta,
                    &key_exprs,
                    &key_types,
                    &key_fields,
                    block_options,
                    output_rows,
                )?
            }
            VerticalReclusterKind::MergeBlocks => {
                if !self.task.all_ordered {
                    return Err(ErrorCode::Internal(
                        "MergeBlocks requires planner-verified ordered sources",
                    ));
                }
                self.execute_merge_blocks(
                    schema,
                    source_metas,
                    &key_exprs,
                    &key_types,
                    &key_fields,
                    block_options,
                    output_rows,
                )?
            }
        };

        let input_rows = self.task.total_rows as u64;
        let output_row_count = outputs
            .iter()
            .map(|output| output.block_meta.row_count)
            .sum::<u64>();
        if input_rows != output_row_count {
            return Err(ErrorCode::Internal(format!(
                "vertical recluster row-count mismatch: input {input_rows}, output {output_row_count}"
            )));
        }
        let entries = outputs
            .into_iter()
            .map(|output| MutationLogEntry::AppendBlock {
                block_meta: Arc::new(output),
            })
            .collect();
        Ok(MutationLogs {
            entries,
            logical_updated_rows: 0,
            logical_deleted_rows: 0,
        }
        .into())
    }

    fn source_metas(&self) -> Result<Vec<Arc<BlockMeta>>> {
        self.task
            .parts
            .partitions
            .iter()
            .map(|part| {
                let part = FuseBlockPartInfo::from_part(part)?;
                Ok(Arc::new(BlockMeta {
                    row_count: part.nums_rows as u64,
                    block_size: 0,
                    file_size: 0,
                    col_stats: part.columns_stat.clone().unwrap_or_default(),
                    col_metas: part.columns_meta.clone(),
                    cluster_stats: None,
                    location: (part.location.clone(), DataBlock::VERSION),
                    bloom_filter_index_location: part.bloom_filter_index_location.clone(),
                    bloom_filter_index_size: part.bloom_filter_index_size,
                    inverted_index_size: None,
                    ngram_filter_index_size: None,
                    vector_index_size: None,
                    vector_index_location: None,
                    spatial_index_size: None,
                    spatial_index_location: None,
                    spatial_stats: None,
                    granule_index: part.granule_index.clone(),
                    vector_stats: None,
                    virtual_block_meta: None,
                    compression: part.compression,
                    create_on: part.create_on,
                }))
            })
            .collect()
    }

    fn cluster_key_exprs(&self, schema: TableSchemaRef) -> Result<Vec<Expr<usize>>> {
        let ast = self.table.resolve_cluster_keys().ok_or_else(|| {
            ErrorCode::InvalidClusterKeys("vertical recluster requires cluster keys")
        })?;
        let table_schema = self.table.schema();
        parse_cluster_keys(self.ctx.clone(), Arc::new(self.table.clone()), ast).map(|exprs| {
            exprs
                .into_iter()
                .map(|expr| {
                    expr.project_column_ref(|index| {
                        let name = table_schema.field(*index).name();
                        schema.index_of(name)
                    })
                })
                .collect::<Result<Vec<_>>>()
        })?
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_sort_blocks(
        &self,
        schema: TableSchemaRef,
        source: Arc<BlockMeta>,
        key_exprs: &[Expr<usize>],
        key_types: &[DataType],
        key_fields: &HashSet<usize>,
        block_options: Arc<FuseBlockWriteOptions>,
        output_rows: usize,
    ) -> Result<Vec<ExtendedBlockMeta>> {
        let rows = source.row_count as usize;
        let mut inputs = self.create_sort_column_inputs(
            schema.clone(),
            source.clone(),
            output_rows.clamp(1, 8192),
        )?;

        let mut outputs = Vec::new();

        for range in ranges(rows, output_rows) {
            let mut dependency_columns = HashMap::with_capacity(schema.num_fields());

            for &field_idx in key_fields {
                let column = inputs[field_idx].read(range.clone())?;
                dependency_columns.insert(field_idx, column);
            }

            let keys = evaluate_keys(
                &dependency_columns,
                key_exprs,
                &self.ctx.get_function_context()?,
            )?;

            let permutation = compare_columns(keys.clone(), range.len())?;
            let sorted_keys = take_columns(&keys, permutation.as_slice())?;

            let write_options = block_options.create_low_level_options(
                self.table.get_operator(),
                key_types.to_vec(),
                self.table.cluster_key_id().unwrap(),
                self.task.level + 1,
                range.len(),
            )?;

            let block_writer = FuseLowLevelBlockWriter::create(write_options)?;
            let mut key_writer = block_writer.write_cluster_keys()?;
            key_writer.write_columns(&sorted_keys)?;
            let block_writer = key_writer.finish()?;

            drop(keys);
            drop(sorted_keys);

            let mut data_writer = block_writer.write_data()?;
            for field_idx in 0..schema.num_fields() {
                let source_column = match dependency_columns.remove(&field_idx) {
                    Some(column) => column,
                    None => inputs[field_idx].read(range.clone())?,
                };

                let reordered = take_columns(&[source_column], permutation.as_slice())?
                    .pop()
                    .expect("one reordered SortBlocks column");

                let mut column_writer = data_writer.next_column()?;
                column_writer.write(&reordered)?;
                data_writer = column_writer.finish()?;
            }

            let output = data_writer.finish()?.finish()?;

            outputs.push(ExtendedBlockMeta {
                block_meta: output.block_meta,
                draft_virtual_block_meta: output.draft_virtual_block_meta,
                column_hlls: output.column_hlls,
                column_top_n: output.column_top_n,
            });
        }

        for input in inputs {
            input.finish()?;
        }

        Ok(outputs)
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_merge_blocks(
        &self,
        schema: TableSchemaRef,
        sources: Vec<Arc<BlockMeta>>,
        key_exprs: &[Expr<usize>],
        key_types: &[DataType],
        key_fields: &HashSet<usize>,
        block_options: Arc<FuseBlockWriteOptions>,
        output_rows: usize,
    ) -> Result<Vec<ExtendedBlockMeta>> {
        let mut total_rows = 0usize;
        let mut source_rows = Vec::with_capacity(sources.len());

        for source in &sources {
            let Ok(rows) = usize::try_from(source.row_count) else {
                return Err(ErrorCode::Internal(format!(
                    "MergeBlocks source row count {} does not fit usize",
                    source.row_count
                )));
            };

            let Some(total_rows) = total_rows.checked_add(rows) else {
                return Err(ErrorCode::Internal(
                    "MergeBlocks total source row count overflowed usize",
                ));
            };

            source_rows.push(rows);
        }

        if total_rows == 0 || total_rows > output_rows {
            return Err(ErrorCode::Internal(format!(
                "MergeBlocks must produce exactly one non-empty output block: {total_rows} rows, limit {output_rows}"
            )));
        }

        let mut source_keys = Vec::with_capacity(sources.len());

        for (source, &rows) in sources.iter().zip(&source_rows) {
            let dependency =
                self.read_projection(schema.clone(), source.clone(), key_fields, rows)?;

            let keys = evaluate_keys(
                dependency,
                key_exprs,
                key_fields,
                &self.ctx.get_function_context()?,
            )?;

            source_keys.push(keys);
        }

        let mapping = merge_sorted_keys(&source_keys, key_types)?;

        let key_blocks = source_keys
            .iter()
            .map(|columns| {
                DataBlock::new(
                    columns.clone().into_iter().map(BlockEntry::from).collect(),
                    columns.first().map_or(0, Column::len),
                )
            })
            .collect::<Vec<_>>();
        let sorted_keys = DataBlockVec::from_blocks(key_blocks)?
            .take(&mapping)
            .take_columns()
            .into_iter()
            .map(|entry| entry.to_column())
            .collect::<Vec<_>>();
        let write_options = block_options.create_low_level_options(
            self.table.get_operator(),
            key_types.to_vec(),
            self.table.cluster_key_id().unwrap(),
            self.task.level + 1,
            total_rows,
        )?;

        let mut block_writer = FuseLowLevelBlockWriter::create(write_options)?;

        {
            let mut key_writer = block_writer.write_cluster_keys()?;
            key_writer.write_columns(&sorted_keys)?;
            block_writer = key_writer.finish()?;

            drop(sorted_keys);
            drop(source_keys);
        }

        let mut data_writer = block_writer.write_data()?;
        for field_idx in 0..schema.num_fields() {
            let mut source_blocks = Vec::with_capacity(sources.len());
            for (source, &rows) in sources.iter().zip(&source_rows) {
                let source_column =
                    self.read_field(schema.clone(), source.clone(), field_idx, rows)?;
                source_blocks.push(DataBlock::new(vec![BlockEntry::from(source_column)], rows));
            }

            let mut columns = DataBlockVec::from_blocks(source_blocks)?
                .take(&mapping)
                .take_columns();
            let output = columns
                .pop()
                .expect("one MergeBlocks payload column")
                .to_column();
            let mut column_writer = data_writer.next_column()?;
            column_writer.write(&output)?;
            data_writer = column_writer.finish()?;
        }

        let output = data_writer.finish()?.finish()?;
        Ok(vec![ExtendedBlockMeta {
            block_meta: output.block_meta,
            draft_virtual_block_meta: output.draft_virtual_block_meta,
            column_hlls: output.column_hlls,
            column_top_n: output.column_top_n,
        }])
    }

    fn create_sort_column_inputs(
        &self,
        schema: TableSchemaRef,
        source: Arc<BlockMeta>,
        batch_size: usize,
    ) -> Result<Vec<SortColumnInput>> {
        let mut default_binder = DefaultExprBinder::try_new(self.ctx.clone())?;
        let mut inputs = Vec::with_capacity(schema.num_fields());
        for field_idx in 0..schema.num_fields() {
            let field = schema.field(field_idx);
            let leaf_ids = field.leaf_column_ids();
            let present = leaf_ids
                .iter()
                .filter(|id| source.col_metas.contains_key(id))
                .count();
            let column_source = if present == leaf_ids.len() {
                let field_schema = Arc::new(schema.project(&[field_idx]));
                let reader = FuseLowLevelBlockReader::create(
                    FuseLowLevelBlockReadOptions::new(
                        self.table.get_operator(),
                        field_schema,
                        source.clone(),
                    )
                    .with_batch_size(batch_size),
                )?;
                SortColumnSource::Physical(reader.next_column()?)
            } else if present != 0 {
                return Err(ErrorCode::ParquetFileInvalid(format!(
                    "field {} has {present} of {} Parquet leaves in block {}",
                    field.name(),
                    leaf_ids.len(),
                    source.location.0
                )));
            } else if is_origin_column(field.column_id()) {
                SortColumnSource::Origin
            } else {
                SortColumnSource::Default {
                    scalar: default_binder.get_scalar(field)?,
                    data_type: field.data_type().into(),
                }
            };
            inputs.push(SortColumnInput {
                source: column_source,
                column_id: field.column_id(),
                source_location: source.location.0.clone(),
                table_version: self.table.get_table_info().ident.seq,
                position: 0,
                expected_rows: source.row_count as usize,
            });
        }
        Ok(inputs)
    }

    fn read_field(
        &self,
        schema: TableSchemaRef,
        meta: Arc<BlockMeta>,
        field_idx: usize,
        rows: usize,
    ) -> Result<Column> {
        Ok(self
            .read_projection(schema, meta, &[field_idx], rows)?
            .get_by_offset(0)
            .to_column())
    }

    fn read_projection(
        &self,
        schema: TableSchemaRef,
        meta: Arc<BlockMeta>,
        fields: &[usize],
        rows: usize,
    ) -> Result<DataBlock> {
        if fields.is_empty() {
            return Ok(DataBlock::new(vec![], rows));
        }

        let mut default_binder = DefaultExprBinder::try_new(self.ctx.clone())?;
        let mut entries = Vec::with_capacity(fields.len());
        for &field_idx in fields {
            let field = schema.field(field_idx);
            let leaf_ids = field.leaf_column_ids();
            let present = leaf_ids
                .iter()
                .filter(|id| meta.col_metas.contains_key(id))
                .count();
            if present != 0 && present != leaf_ids.len() {
                return Err(ErrorCode::ParquetFileInvalid(format!(
                    "field {} has {present} of {} Parquet leaves in block {}",
                    field.name(),
                    leaf_ids.len(),
                    meta.location.0
                )));
            }

            let persisted = if present == leaf_ids.len() {
                let field_schema = Arc::new(schema.project(&[field_idx]));
                let reader = FuseLowLevelBlockReader::create(
                    FuseLowLevelBlockReadOptions::new(
                        self.table.get_operator(),
                        field_schema,
                        meta.clone(),
                    )
                    .with_batch_size(rows.clamp(1, 8192)),
                )?;
                let mut column_reader = reader.next_column()?;
                let column = column_reader.read(rows)?;
                column_reader.finish()?.finish()?;
                Some(column)
            } else {
                None
            };

            let column = if is_origin_column(field.column_id()) {
                materialize_stream_field(
                    field.column_id(),
                    persisted,
                    &meta.location.0,
                    0..rows,
                    self.table.get_table_info().ident.seq,
                )?
            } else if let Some(column) = persisted {
                column
            } else {
                let scalar = default_binder.get_scalar(field)?;
                let data_type: DataType = field.data_type().into();
                databend_common_expression::ColumnBuilder::repeat(
                    &scalar.as_ref(),
                    rows,
                    &data_type,
                )
                .build()
            };
            entries.push(column.into());
        }
        Ok(DataBlock::new(entries, rows))
    }
}

impl SyncSource for VerticalReclusterSource {
    const NAME: &'static str = "VerticalReclusterSource";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        self.execute().map(Some)
    }
}

fn is_origin_column(column_id: u32) -> bool {
    matches!(
        column_id,
        ORIGIN_VERSION_COLUMN_ID | ORIGIN_BLOCK_ID_COLUMN_ID | ORIGIN_BLOCK_ROW_NUM_COLUMN_ID
    )
}

fn materialize_stream_field(
    column_id: u32,
    persisted: Option<Column>,
    source_location: &str,
    source_range: Range<usize>,
    table_version: u64,
) -> Result<Column> {
    let generated = match column_id {
        ORIGIN_VERSION_COLUMN_ID => Scalar::Number(NumberScalar::UInt64(table_version)),
        ORIGIN_BLOCK_ID_COLUMN_ID => Scalar::Decimal(DecimalScalar::Decimal128(
            databend_common_catalog::plan::block_id_from_location(source_location)?,
            databend_common_expression::types::DecimalSize::default_128(),
        )),
        ORIGIN_BLOCK_ROW_NUM_COLUMN_ID => Scalar::Number(NumberScalar::UInt64(0)),
        _ => {
            return Err(ErrorCode::Internal(format!(
                "column {column_id} is not a stream origin column"
            )));
        }
    };
    let data_type = match column_id {
        ORIGIN_VERSION_COLUMN_ID | ORIGIN_BLOCK_ROW_NUM_COLUMN_ID => DataType::Nullable(Box::new(
            DataType::Number(databend_common_expression::types::NumberDataType::UInt64),
        )),
        ORIGIN_BLOCK_ID_COLUMN_ID => DataType::Nullable(Box::new(DataType::Decimal(
            databend_common_expression::types::DecimalSize::default_128(),
        ))),
        _ => unreachable!(),
    };
    let rows = source_range.len();
    let mut builder = databend_common_expression::ColumnBuilder::with_capacity(&data_type, rows);
    for (local_row, source_row) in source_range.enumerate() {
        let persisted_value = persisted.as_ref().and_then(|column| {
            let value = unsafe { column.index_unchecked(local_row) };
            (!matches!(value, databend_common_expression::ScalarRef::Null)).then_some(value)
        });
        if let Some(value) = persisted_value {
            builder.push(value);
        } else if column_id == ORIGIN_BLOCK_ROW_NUM_COLUMN_ID {
            builder.push(databend_common_expression::ScalarRef::Number(
                NumberScalar::UInt64(source_row as u64),
            ));
        } else {
            builder.push(generated.as_ref());
        }
    }
    Ok(builder.build())
}

fn ranges(rows: usize, target: usize) -> impl Iterator<Item = Range<usize>> {
    (0..rows)
        .step_by(target)
        .map(move |start| start..(start + target).min(rows))
}

fn evaluate_keys(
    dependency: &HashMap<usize, Column>,
    exprs: &[Expr<usize>],
    func_ctx: &FunctionContext,
) -> Result<Vec<Column>> {
    let mut entries = Vec::with_capacity(dependency.len());
    let mut positions = HashMap::new();
    for (position, (field, column)) in dependency.iter().enumerate() {
        positions.insert(field, position);
        entries.push(column.clone());
    }

    let data_block = DataBlock::new_from_columns(entries);
    let mut cluster_keys = Vec::with_capacity(exprs.len());

    for key_expr in exprs {
        let key_expr = key_expr.project_column_ref(|field| match positions.get(field) {
            Some(position) => Ok(*position),
            None => Err(ErrorCode::Internal(format!(
                "cluster dependency field {field} is missing"
            ))),
        })?;
        let evaluator = Evaluator::new(&data_block, func_ctx, &BUILTIN_FUNCTIONS);
        let key_value = evaluator.run(&key_expr)?;
        cluster_keys.push(key_value.into_full_column(&key_expr.data_type(), dependency.num_rows()));
    }

    Ok(cluster_keys)
}

fn take_columns(columns: &[Column], permutation: &[u32]) -> Result<Vec<Column>> {
    let block = DataBlock::new(
        columns.iter().cloned().map(BlockEntry::from).collect(),
        columns.first().map_or(0, Column::len),
    );

    Ok(block
        .take(permutation)?
        .take_columns()
        .into_iter()
        .map(|entry| entry.to_column())
        .collect())
}

/// Merge already-sorted key columns into a compressed source-row mapping.
///
/// The heap stores source ids only. Comparisons read typed column values in
/// place, so advancing a cursor performs no key allocation or scalar cloning.
fn merge_sorted_keys(keys: &[Vec<Column>], key_types: &[DataType]) -> Result<ChunkIndex> {
    let total_rows = validate_merge_keys(keys, key_types)?;
    let mut positions = vec![0usize; keys.len()];
    let mut heap = MergeKeyHeap::new(keys, &positions);
    let mut output = ChunkIndex::default();

    while !heap.is_empty() {
        let source = heap.top() as usize;
        let start = positions[source];
        let end = keys[source][0].len();
        let take = match heap.runner_up(keys, &positions) {
            None => end - start,
            Some(runner) => winner_prefix_len(
                keys,
                source,
                start,
                end,
                runner as usize,
                positions[runner as usize],
            ),
        };
        debug_assert!(take > 0);
        output.push_merge_range(
            u32::try_from(source).expect("validated MergeBlocks source index"),
            u32::try_from(start).expect("validated MergeBlocks source row"),
            u32::try_from(take).expect("validated MergeBlocks winner run"),
        );
        positions[source] += take;
        heap.update_top(keys, &positions, positions[source] == end);
    }
    if output.num_rows() != total_rows {
        return Err(ErrorCode::Internal(format!(
            "MergeBlocks mapping has {} rows, expected {total_rows}",
            output.num_rows()
        )));
    }
    Ok(output)
}

fn validate_merge_keys(keys: &[Vec<Column>], key_types: &[DataType]) -> Result<usize> {
    if key_types.is_empty() {
        return Err(ErrorCode::Internal(
            "MergeBlocks requires at least one cluster key",
        ));
    }

    let mut total_rows = 0usize;
    for (source, columns) in keys.iter().enumerate() {
        if source > u32::MAX as usize {
            return Err(ErrorCode::Internal(format!(
                "MergeBlocks source index {source} exceeds ChunkIndex capacity"
            )));
        }
        if columns.len() != key_types.len() {
            return Err(ErrorCode::Internal(format!(
                "MergeBlocks source {source} has {} keys, expected {}",
                columns.len(),
                key_types.len()
            )));
        }
        let rows = columns.first().map_or(0, Column::len);
        if rows > u32::MAX as usize {
            return Err(ErrorCode::Internal(format!(
                "MergeBlocks source {source} has {rows} rows, exceeding ChunkIndex capacity"
            )));
        }
        total_rows = match total_rows.checked_add(rows) {
            Some(total_rows) => total_rows,
            None => {
                return Err(ErrorCode::Internal(
                    "MergeBlocks key row count overflowed usize",
                ));
            }
        };
        for (field, (column, expected)) in columns.iter().zip(key_types).enumerate() {
            if column.len() != rows || column.data_type() != *expected {
                return Err(ErrorCode::Internal(format!(
                    "MergeBlocks source {source} key {field} has type {:?} and {} rows, expected {expected:?} and {rows} rows",
                    column.data_type(),
                    column.len()
                )));
            }
        }
    }
    if total_rows > u32::MAX as usize {
        return Err(ErrorCode::Internal(format!(
            "MergeBlocks has {total_rows} rows, exceeding ChunkIndex capacity"
        )));
    }
    Ok(total_rows)
}

struct MergeKeyHeap {
    sources: Vec<u32>,
}

impl MergeKeyHeap {
    fn new(keys: &[Vec<Column>], positions: &[usize]) -> Self {
        let mut sources = Vec::with_capacity(keys.len());
        for (source, columns) in keys.iter().enumerate() {
            if columns[0].len() != 0 {
                sources.push(source as u32);
            }
        }
        let mut heap = Self { sources };
        if heap.sources.len() > 1 {
            for root in (0..heap.sources.len() / 2).rev() {
                heap.sift_down(root, keys, positions);
            }
        }
        heap
    }

    fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    fn top(&self) -> u32 {
        self.sources[0]
    }

    /// In a min-heap the smaller root child is the minimum of all non-root items.
    fn runner_up(&self, keys: &[Vec<Column>], positions: &[usize]) -> Option<u32> {
        match self.sources.len() {
            0 | 1 => None,
            2 => Some(self.sources[1]),
            _ => Some(
                if compare_sources(keys, positions, self.sources[1], self.sources[2])
                    == Ordering::Less
                {
                    self.sources[1]
                } else {
                    self.sources[2]
                },
            ),
        }
    }

    fn update_top(&mut self, keys: &[Vec<Column>], positions: &[usize], exhausted: bool) {
        if exhausted {
            self.sources.swap_remove(0);
        }
        if !self.sources.is_empty() {
            self.sift_down(0, keys, positions);
        }
    }

    fn sift_down(&mut self, mut root: usize, keys: &[Vec<Column>], positions: &[usize]) {
        loop {
            let left = root * 2 + 1;
            if left >= self.sources.len() {
                break;
            }
            let right = left + 1;
            let child = if right < self.sources.len()
                && compare_sources(keys, positions, self.sources[right], self.sources[left])
                    == Ordering::Less
            {
                right
            } else {
                left
            };
            if compare_sources(keys, positions, self.sources[child], self.sources[root])
                != Ordering::Less
            {
                break;
            }
            self.sources.swap(root, child);
            root = child;
        }
    }
}

fn winner_prefix_len(
    keys: &[Vec<Column>],
    winner: usize,
    start: usize,
    end: usize,
    runner: usize,
    runner_row: usize,
) -> usize {
    let wins = |row| {
        compare_key_rows(
            &keys[winner],
            row,
            winner as u32,
            &keys[runner],
            runner_row,
            runner as u32,
        ) != Ordering::Greater
    };
    debug_assert!(wins(start));
    if wins(end - 1) {
        return end - start;
    }

    // Probe a short local run before binary search, matching the shape used by
    // ClickHouse while avoiding binary-search overhead for interleaved sources.
    let linear_end = (start + 16).min(end);
    for row in start + 1..linear_end {
        if !wins(row) {
            return row - start;
        }
    }
    let mut low = linear_end;
    let mut high = end;
    while low < high {
        let mid = low + (high - low) / 2;
        if wins(mid) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low - start
}

fn compare_sources(keys: &[Vec<Column>], positions: &[usize], left: u32, right: u32) -> Ordering {
    compare_key_rows(
        &keys[left as usize],
        positions[left as usize],
        left,
        &keys[right as usize],
        positions[right as usize],
        right,
    )
}

fn compare_key_rows(
    left: &[Column],
    left_row: usize,
    left_source: u32,
    right: &[Column],
    right_row: usize,
    right_source: u32,
) -> Ordering {
    for (left, right) in left.iter().zip(right) {
        let order = compare_key_column_rows(left, left_row, right, right_row);
        if order != Ordering::Equal {
            return order;
        }
    }
    left_source.cmp(&right_source)
}

fn compare_key_column_rows(
    left: &Column,
    left_row: usize,
    right: &Column,
    right_row: usize,
) -> Ordering {
    use databend_common_expression::types::decimal::DecimalColumn;
    use databend_common_expression::types::number::NumberColumn;

    macro_rules! compare_buffers {
        ($left:expr, $left_row:expr, $right:expr, $right_row:expr) => {{
            unsafe {
                $left
                    .get_unchecked($left_row)
                    .cmp($right.get_unchecked($right_row))
            }
        }};
    }

    match (left, right) {
        (Column::Number(left), Column::Number(right)) => match (left, right) {
            (NumberColumn::UInt8(left), NumberColumn::UInt8(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (NumberColumn::UInt16(left), NumberColumn::UInt16(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (NumberColumn::UInt32(left), NumberColumn::UInt32(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (NumberColumn::UInt64(left), NumberColumn::UInt64(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (NumberColumn::Int8(left), NumberColumn::Int8(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (NumberColumn::Int16(left), NumberColumn::Int16(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (NumberColumn::Int32(left), NumberColumn::Int32(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (NumberColumn::Int64(left), NumberColumn::Int64(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (NumberColumn::Float32(left), NumberColumn::Float32(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (NumberColumn::Float64(left), NumberColumn::Float64(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            _ => unreachable!("validated MergeBlocks number types differ"),
        },
        (Column::Decimal(left), Column::Decimal(right)) => match (left, right) {
            (DecimalColumn::Decimal64(left, _), DecimalColumn::Decimal64(right, _)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (DecimalColumn::Decimal128(left, _), DecimalColumn::Decimal128(right, _)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (DecimalColumn::Decimal256(left, _), DecimalColumn::Decimal256(right, _)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            _ => unreachable!("validated MergeBlocks decimal types differ"),
        },
        (Column::Boolean(left), Column::Boolean(right)) => {
            left.get_bit(left_row).cmp(&right.get_bit(right_row))
        }
        (Column::String(left), Column::String(right)) => {
            databend_common_expression::types::StringColumn::compare(
                left, left_row, right, right_row,
            )
        }
        (Column::Timestamp(left), Column::Timestamp(right)) => {
            compare_buffers!(left, left_row, right, right_row)
        }
        (Column::Date(left), Column::Date(right)) => {
            compare_buffers!(left, left_row, right, right_row)
        }
        (Column::Nullable(left), Column::Nullable(right)) => match (
            left.validity.get_bit(left_row),
            right.validity.get_bit(right_row),
        ) {
            (true, true) => {
                compare_key_column_rows(&left.column, left_row, &right.column, right_row)
            }
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => Ordering::Equal,
        },
        // Reuse the existing borrowed scalar ordering for less common scalar
        // cluster-key types. This keeps the merge allocation-free without
        // changing SortCompare or introducing another public comparator API.
        _ => unsafe {
            left.index_unchecked(left_row)
                .cmp(&right.index_unchecked(right_row))
        },
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::types::BinaryType;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_expression::types::nullable::NullableColumn;
    use databend_common_expression::types::number::Float64Type;
    use databend_common_expression::types::number::Int32Type;
    use databend_common_expression::types::number::UInt64Type;
    use databend_common_expression::types::string::StringType;
    use rand::Rng;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::*;

    #[test]
    fn test_materialize_stream_field_preserves_non_null_and_uses_absolute_rows() {
        let persisted = NullableColumn::new_column(
            UInt64Type::from_data(vec![0, 42, 0]),
            [false, true, false].into_iter().collect(),
        );
        let column = materialize_stream_field(
            ORIGIN_BLOCK_ROW_NUM_COLUMN_ID,
            Some(persisted),
            "unused",
            10..13,
            99,
        )
        .unwrap();
        assert_eq!(
            (0..3)
                .map(|row| unsafe { column.index_unchecked(row) }.to_owned())
                .collect::<Vec<_>>(),
            vec![
                Scalar::Number(NumberScalar::UInt64(10)),
                Scalar::Number(NumberScalar::UInt64(42)),
                Scalar::Number(NumberScalar::UInt64(12)),
            ]
        );
        assert_eq!(
            column.data_type(),
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64)))
        );
    }
    fn mapping_rows(mapping: &ChunkIndex) -> Vec<(u32, u32)> {
        let mut rows = Vec::with_capacity(mapping.num_rows());
        for chunk in mapping.iter_chunk() {
            match chunk {
                databend_common_expression::Chunk::Single { block, rows: chunk } => {
                    rows.extend(chunk.iter().map(|row| (block, *row)));
                }
                databend_common_expression::Chunk::Range { block, row, len } => {
                    rows.extend((row..row + len).map(|row| (block, row)));
                }
                databend_common_expression::Chunk::Repeat {
                    block,
                    rows: repeat,
                } => {
                    rows.extend(std::iter::repeat_n(
                        (block, repeat.row),
                        repeat.count as usize,
                    ));
                }
            }
        }
        rows
    }

    fn key_types(sources: &[Vec<Column>]) -> Vec<DataType> {
        sources[0].iter().map(Column::data_type).collect()
    }

    #[test]
    fn test_merge_sorted_keys_and_compressed_mapping() {
        let sources = vec![
            vec![
                Int32Type::from_data(vec![1, 3, 5]),
                StringType::from_data(vec!["a", "a", "b"]),
            ],
            vec![
                Int32Type::from_data(vec![1, 2, 5]),
                StringType::from_data(vec!["b", "z", "a"]),
            ],
        ];
        let mapping = merge_sorted_keys(&sources, &key_types(&sources)).unwrap();
        assert_eq!(mapping_rows(&mapping), vec![
            (0, 0),
            (1, 0),
            (1, 1),
            (0, 1),
            (1, 2),
            (0, 2)
        ]);
        // Consecutive winner rows are represented as ranges, not dense pairs.
        assert!(mapping.iter_chunk().count() < mapping.num_rows());
        assert!(mapping.iter_chunk().any(
            |chunk| matches!(chunk, databend_common_expression::Chunk::Range { len, .. } if len > 1)
        ));
    }

    #[test]
    fn test_merge_sorted_keys_orders_nullable_keys_nulls_last() {
        let source0 = NullableColumn::new_column(
            Int32Type::from_data(vec![1, 0]),
            [true, false].into_iter().collect(),
        );
        let source1 = NullableColumn::new_column(
            Int32Type::from_data(vec![2, 0]),
            [true, false].into_iter().collect(),
        );
        let sources = vec![vec![source0], vec![source1]];
        let mapping = merge_sorted_keys(&sources, &key_types(&sources)).unwrap();
        assert_eq!(mapping_rows(&mapping), vec![(0, 0), (1, 0), (0, 1), (1, 1)]);
    }

    #[test]
    fn test_vertical_merge_matches_horizontal_recluster_sort_contract() {
        // Every source is already sorted by the existing horizontal comparator.
        // Keys are globally unique, so unstable equal-key ordering is irrelevant.
        let sources = vec![
            vec![
                Int32Type::from_data(vec![1, 3, 5, 9]),
                StringType::from_data(vec!["z", "a", "m", "a"]),
            ],
            vec![
                Int32Type::from_data(vec![1, 2, 5, 8]),
                StringType::from_data(vec!["a", "x", "z", "q"]),
            ],
            vec![
                Int32Type::from_data(vec![0, 4, 6, 10]),
                StringType::from_data(vec!["k", "b", "c", "x"]),
            ],
        ];
        let types = key_types(&sources);
        let mapping = merge_sorted_keys(&sources, &types).unwrap();
        let source_blocks = sources
            .iter()
            .map(|columns| DataBlock::new_from_columns(columns.clone()))
            .collect::<Vec<_>>();
        let vertical = databend_common_expression::DataBlockVec::from_blocks(source_blocks.clone())
            .unwrap()
            .take(&mapping);

        let all_columns = (0..types.len())
            .map(|field| {
                Column::concat_columns(
                    source_blocks
                        .iter()
                        .map(|block| block.get_by_offset(field).to_column()),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let horizontal_order = compare_columns(all_columns.clone(), vertical.num_rows()).unwrap();
        let horizontal = take_columns(&all_columns, &horizontal_order).unwrap();
        let vertical_columns = vertical
            .take_columns()
            .into_iter()
            .map(|entry| entry.to_column())
            .collect::<Vec<_>>();
        assert_eq!(vertical_columns, horizontal);
    }

    #[test]
    fn test_vertical_merge_matches_horizontal_float_order() {
        // The second key makes -0/+0 and NaN pairs globally unique while the
        // first key still exercises the horizontal Float ordering contract.
        let sources = vec![
            vec![
                Float64Type::from_data(vec![f64::NEG_INFINITY, -0.0, 1.0, f64::NAN]),
                Int32Type::from_data(vec![0, 0, 0, 0]),
            ],
            vec![
                Float64Type::from_data(vec![-1.0, 0.0, 2.0, f64::NAN]),
                Int32Type::from_data(vec![1, 1, 1, 1]),
            ],
        ];
        let types = key_types(&sources);
        let mapping = merge_sorted_keys(&sources, &types).unwrap();
        let source_blocks = sources
            .iter()
            .map(|columns| DataBlock::new_from_columns(columns.clone()))
            .collect::<Vec<_>>();
        let vertical = databend_common_expression::DataBlockVec::from_blocks(source_blocks.clone())
            .unwrap()
            .take(&mapping)
            .take_columns()
            .into_iter()
            .map(|entry| entry.to_column())
            .collect::<Vec<_>>();
        let all_columns = (0..types.len())
            .map(|field| {
                Column::concat_columns(
                    source_blocks
                        .iter()
                        .map(|block| block.get_by_offset(field).to_column()),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let horizontal_order = compare_columns(all_columns.clone(), mapping.num_rows()).unwrap();
        let horizontal = take_columns(&all_columns, &horizontal_order).unwrap();
        assert_eq!(vertical, horizontal);
    }

    #[test]
    fn test_vertical_merge_matches_horizontal_binary_order() {
        let sources = vec![
            vec![
                BinaryType::from_data(vec![vec![0], vec![1, 0], vec![2]]),
                Int32Type::from_data(vec![0, 0, 0]),
            ],
            vec![
                BinaryType::from_data(vec![vec![0, 1], vec![1, 1], vec![3]]),
                Int32Type::from_data(vec![1, 1, 1]),
            ],
        ];
        let types = key_types(&sources);
        let mapping = merge_sorted_keys(&sources, &types).unwrap();
        let source_blocks = sources
            .iter()
            .map(|columns| DataBlock::new_from_columns(columns.clone()))
            .collect::<Vec<_>>();
        let vertical = databend_common_expression::DataBlockVec::from_blocks(source_blocks.clone())
            .unwrap()
            .take(&mapping)
            .take_columns()
            .into_iter()
            .map(|entry| entry.to_column())
            .collect::<Vec<_>>();
        let all_columns = (0..types.len())
            .map(|field| {
                Column::concat_columns(
                    source_blocks
                        .iter()
                        .map(|block| block.get_by_offset(field).to_column()),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let horizontal_order = compare_columns(all_columns.clone(), mapping.num_rows()).unwrap();
        let horizontal = take_columns(&all_columns, &horizontal_order).unwrap();
        assert_eq!(vertical, horizontal);
    }

    #[test]
    fn test_randomized_vertical_merge_matches_horizontal_sort() {
        let mut rng = StdRng::seed_from_u64(0x5eed_cafe_d15c_a11e);
        for round in 0..256 {
            let source_count = rng.gen_range(1..=8);
            let mut next_id = 0i32;
            let mut sources = Vec::with_capacity(source_count);
            for _ in 0..source_count {
                let rows = rng.gen_range(0..=24);
                let mut nullable = Vec::with_capacity(rows);
                let mut strings = Vec::with_capacity(rows);
                let mut ids = Vec::with_capacity(rows);
                for _ in 0..rows {
                    nullable.push(if rng.gen_ratio(1, 5) {
                        None
                    } else {
                        Some(rng.gen_range(-4..=4))
                    });
                    strings.push(match rng.gen_range(0..4) {
                        0 => "",
                        1 => "a",
                        2 => "aa",
                        _ => "z",
                    });
                    ids.push(next_id);
                    next_id += 1;
                }
                let columns = vec![
                    Int32Type::from_opt_data(nullable),
                    StringType::from_data(strings),
                    Int32Type::from_data(ids),
                ];
                let permutation = compare_columns(columns.clone(), rows).unwrap();
                sources.push(take_columns(&columns, &permutation).unwrap());
            }

            let types = key_types(&sources);
            let mapping = merge_sorted_keys(&sources, &types).unwrap();
            let total_rows = sources
                .iter()
                .map(|columns| columns[0].len())
                .sum::<usize>();
            assert_eq!(mapping.num_rows(), total_rows, "round {round}");

            let source_blocks = sources
                .iter()
                .map(|columns| DataBlock::new_from_columns(columns.clone()))
                .collect::<Vec<_>>();
            let vertical =
                databend_common_expression::DataBlockVec::from_blocks(source_blocks.clone())
                    .unwrap()
                    .take(&mapping)
                    .take_columns()
                    .into_iter()
                    .map(|entry| entry.to_column())
                    .collect::<Vec<_>>();
            let all_columns = (0..types.len())
                .map(|field| {
                    Column::concat_columns(
                        source_blocks
                            .iter()
                            .map(|block| block.get_by_offset(field).to_column()),
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>();
            let horizontal_order = compare_columns(all_columns.clone(), total_rows).unwrap();
            let horizontal = take_columns(&all_columns, &horizontal_order).unwrap();
            assert_eq!(vertical, horizontal, "round {round}");
        }
    }

    #[test]
    fn test_equal_keys_use_source_order_and_replay_payload_losslessly() {
        let sources = vec![vec![Int32Type::from_data(vec![1, 1, 1])], vec![
            Int32Type::from_data(vec![1, 1]),
        ]];
        let mapping = merge_sorted_keys(&sources, &key_types(&sources)).unwrap();
        assert_eq!(mapping_rows(&mapping), vec![
            (0, 0),
            (0, 1),
            (0, 2),
            (1, 0),
            (1, 1)
        ]);

        let payload = vec![
            DataBlock::new_from_columns(vec![StringType::from_data(vec!["a", "b", "c"])]),
            DataBlock::new_from_columns(vec![StringType::from_data(vec!["d", "e"])]),
        ];
        let replayed = databend_common_expression::DataBlockVec::from_blocks(payload)
            .unwrap()
            .take(&mapping)
            .get_by_offset(0)
            .to_column();
        assert_eq!(
            replayed,
            StringType::from_data(vec!["a", "b", "c", "d", "e"])
        );
    }
}
