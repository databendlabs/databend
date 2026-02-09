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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::mem;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use chrono::Utc;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::VIRTUAL_COLUMN_ID_START;
use databend_common_expression::VariantDataType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use databend_common_meta_app::schema::TableIndex;
use databend_common_metrics::storage::metrics_inc_variant_shredding_inline_columns;
use databend_common_metrics::storage::metrics_inc_variant_shredding_inline_value_all_null_columns;
use databend_common_native::write::NativeWriter;
use databend_common_native::write::WriteOptions;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_storages_common_blocks::MAX_BATCH_MEMORY_SIZE;
use databend_storages_common_blocks::NdvProvider;
use databend_storages_common_blocks::build_parquet_writer_properties;
use databend_storages_common_blocks::write_batch_with_page_limit;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::BloomIndexBuilder;
use databend_storages_common_index::Index;
use databend_storages_common_index::NgramArgs;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_table_meta::meta::BlockHLLState;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::DraftVirtualBlockMeta;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::VariantEncoding;
use databend_storages_common_table_meta::table::TableCompression;
use parquet::arrow::ArrowWriter;
use parquet::format::FileMetaData;

use crate::FuseStorageFormat;
use crate::FuseTable;
use crate::io::BlockSerialization;
use crate::io::BloomIndexState;
use crate::io::InvertedIndexBuilder;
use crate::io::InvertedIndexWriter;
use crate::io::TableMetaLocationGenerator;
use crate::io::VariantShreddedColumn;
use crate::io::VectorIndexBuilder;
use crate::io::VirtualColumnBuilder;
use crate::io::WriteSettings;
use crate::io::arrow_schema_with_parquet_variant;
use crate::io::arrow_schema_with_parquet_variant_and_shredding;
use crate::io::build_parquet_variant_record_batch_with_arrow_schema;
use crate::io::build_parquet_variant_record_batch_with_inline_shredding;
use crate::io::create_inverted_index_builders;
use crate::io::parquet_variant_leaf_column_ids;
use crate::io::parquet_variant_leaf_column_ids_with_shredding;
use crate::io::variant_data_type_to_arrow;
use crate::io::write::BlockStatsBuilder;
use crate::io::write::InvertedIndexState;
use crate::io::write::stream::ColumnStatisticsState;
use crate::io::write::stream::block_builder::ArrowParquetWriter::Initialized;
use crate::io::write::stream::cluster_statistics::ClusterStatisticsBuilder;
use crate::io::write::stream::cluster_statistics::ClusterStatisticsState;
use crate::io::write::virtual_column_builder::InlineVirtualColumn;
use crate::io::write::virtual_column_builder::VirtualColumnState;
use crate::operations::column_parquet_metas;
use crate::operations::column_parquet_metas_with_leaf_ids;

fn inline_virtual_table_type(data_type: &VariantDataType) -> Option<TableDataType> {
    match data_type {
        VariantDataType::Boolean => Some(TableDataType::Nullable(Box::new(TableDataType::Boolean))),
        VariantDataType::UInt64 => Some(TableDataType::Nullable(Box::new(TableDataType::Number(
            NumberDataType::UInt64,
        )))),
        VariantDataType::Int64 => Some(TableDataType::Nullable(Box::new(TableDataType::Number(
            NumberDataType::Int64,
        )))),
        VariantDataType::Float64 => Some(TableDataType::Nullable(Box::new(TableDataType::Number(
            NumberDataType::Float64,
        )))),
        VariantDataType::String => Some(TableDataType::Nullable(Box::new(TableDataType::String))),
        _ => None,
    }
}

fn build_inline_virtual_schema(columns: &[InlineVirtualColumn]) -> Result<TableSchemaRef> {
    let mut fields = Vec::with_capacity(columns.len());
    for (idx, column) in columns.iter().enumerate() {
        let column_id = VIRTUAL_COLUMN_ID_START + idx as u32;
        let table_type = inline_virtual_table_type(&column.data_type).ok_or_else(|| {
            ErrorCode::Internal("unsupported virtual column type for inline shredding".to_string())
        })?;
        fields.push(TableField::new_from_column_id(
            &column.key_name,
            table_type,
            column_id,
        ));
    }
    Ok(TableSchemaRefExt::create(fields))
}

pub struct UninitializedArrowWriter {
    write_settings: WriteSettings,
    arrow_schema: Arc<Schema>,
    table_schema: TableSchemaRef,
    use_variant_struct: bool,
    leaf_column_ids: Vec<ColumnId>,
    shredded_columns: Option<Vec<VariantShreddedColumn>>,
}
impl UninitializedArrowWriter {
    fn init(&self, cols_ndv_info: ColumnsNdvInfo) -> Result<ArrowWriter<Vec<u8>>> {
        let write_settings = &self.write_settings;
        let num_rows = cols_ndv_info.num_rows;

        let ndv_info = if self.use_variant_struct {
            None::<ColumnsNdvInfo>
        } else {
            Some(cols_ndv_info)
        };
        let writer_properties = build_parquet_writer_properties(
            write_settings.table_compression,
            write_settings.enable_parquet_dictionary,
            ndv_info,
            None,
            num_rows,
            self.table_schema.as_ref(),
        );
        let buffer = Vec::with_capacity(DEFAULT_BLOCK_BUFFER_SIZE);
        let writer =
            ArrowWriter::try_new(buffer, self.arrow_schema.clone(), Some(writer_properties))?;
        Ok(writer)
    }
}

pub struct InitializedArrowWriter {
    inner: ArrowWriter<Vec<u8>>,
    arrow_schema: Arc<Schema>,
    use_variant_struct: bool,
    leaf_column_ids: Vec<ColumnId>,
    shredded_columns: Option<Vec<VariantShreddedColumn>>,
}
pub enum ArrowParquetWriter {
    Uninitialized(UninitializedArrowWriter),
    Initialized(InitializedArrowWriter),
}
impl ArrowParquetWriter {
    fn new_uninitialized(write_settings: WriteSettings, table_schema: TableSchemaRef) -> Self {
        let has_variant = table_schema
            .fields()
            .iter()
            .any(|field| field.data_type().remove_nullable() == TableDataType::Variant);
        let use_variant_struct =
            write_settings.variant_encoding == VariantEncoding::ParquetVariant && has_variant;
        let arrow_schema = if use_variant_struct {
            Arc::new(arrow_schema_with_parquet_variant(table_schema.as_ref()))
        } else {
            Arc::new(table_schema.as_ref().into())
        };
        let leaf_column_ids = if use_variant_struct {
            parquet_variant_leaf_column_ids(table_schema.as_ref())
        } else {
            table_schema.to_leaf_column_ids()
        };
        ArrowParquetWriter::Uninitialized(UninitializedArrowWriter {
            write_settings,
            arrow_schema,
            table_schema,
            use_variant_struct,
            leaf_column_ids,
            shredded_columns: None,
        })
    }
    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let Initialized(writer) = self else {
            unreachable!("ArrowParquetWriter::write called before initialization");
        };
        write_batch_with_page_limit(&mut writer.inner, batch, MAX_BATCH_MEMORY_SIZE)?;
        Ok(())
    }

    fn finish(&mut self) -> Result<FileMetaData> {
        let Initialized(writer) = self else {
            unreachable!("ArrowParquetWriter::finish called before initialization");
        };
        let file_meta = writer.inner.finish()?;
        Ok(file_meta)
    }

    fn inner_mut(&mut self) -> &mut Vec<u8> {
        let Initialized(writer) = self else {
            unreachable!("ArrowParquetWriter::inner_mut called before initialization");
        };
        writer.inner.inner_mut()
    }

    fn in_progress_size(&self) -> usize {
        match self {
            ArrowParquetWriter::Uninitialized(_) => 0,
            Initialized(writer) => writer.inner.in_progress_size(),
        }
    }

    fn use_variant_struct(&self) -> bool {
        match self {
            ArrowParquetWriter::Uninitialized(inner) => inner.use_variant_struct,
            ArrowParquetWriter::Initialized(inner) => inner.use_variant_struct,
        }
    }

    fn arrow_schema(&self) -> &Arc<Schema> {
        match self {
            ArrowParquetWriter::Uninitialized(inner) => &inner.arrow_schema,
            ArrowParquetWriter::Initialized(inner) => &inner.arrow_schema,
        }
    }

    fn shredded_columns(&self) -> Option<&[VariantShreddedColumn]> {
        match self {
            ArrowParquetWriter::Uninitialized(inner) => inner.shredded_columns.as_deref(),
            ArrowParquetWriter::Initialized(inner) => inner.shredded_columns.as_deref(),
        }
    }

    fn set_shredded_columns(
        &mut self,
        table_schema: &TableSchema,
        shredded_columns: Vec<VariantShreddedColumn>,
    ) -> Result<()> {
        let ArrowParquetWriter::Uninitialized(inner) = self else {
            return Err(ErrorCode::Internal(
                "cannot set shredded columns after writer initialization".to_string(),
            ));
        };
        if !inner.use_variant_struct {
            return Err(ErrorCode::Internal(
                "shredded columns require parquet variant encoding".to_string(),
            ));
        }
        inner.shredded_columns = Some(shredded_columns);
        inner.arrow_schema = Arc::new(arrow_schema_with_parquet_variant_and_shredding(
            table_schema,
            inner.shredded_columns.as_deref(),
        ));
        inner.leaf_column_ids = parquet_variant_leaf_column_ids_with_shredding(
            table_schema,
            inner.shredded_columns.as_deref(),
        );
        Ok(())
    }

    fn leaf_column_ids(&self) -> &[ColumnId] {
        match self {
            ArrowParquetWriter::Uninitialized(inner) => inner.leaf_column_ids.as_slice(),
            ArrowParquetWriter::Initialized(inner) => inner.leaf_column_ids.as_slice(),
        }
    }
}

pub struct ColumnsNdvInfo {
    cols_ndv: HashMap<ColumnId, usize>,
    num_rows: usize,
}

impl ColumnsNdvInfo {
    fn new(num_rows: usize, cols_ndv: HashMap<ColumnId, usize>) -> Self {
        Self { cols_ndv, num_rows }
    }
}
impl NdvProvider for ColumnsNdvInfo {
    fn column_ndv(&self, column_id: &ColumnId) -> Option<u64> {
        self.cols_ndv.get(column_id).map(|v| *v as u64)
    }
}

pub enum BlockWriterImpl {
    Parquet(ArrowParquetWriter),
    // Native format doesnot support stream write.
    Native(NativeWriter<Vec<u8>>),
}

pub trait BlockWriter {
    fn start(&mut self, cols_ndv: ColumnsNdvInfo) -> Result<()>;

    fn write(
        &mut self,
        block: DataBlock,
        schema: &TableSchema,
        inline_virtual_block: Option<&DataBlock>,
    ) -> Result<Option<HashSet<ColumnId>>>;

    fn finish(&mut self, schema: &TableSchemaRef) -> Result<HashMap<ColumnId, ColumnMeta>>;

    fn compressed_size(&self) -> usize;

    fn inner_mut(&mut self) -> &mut Vec<u8>;
}

impl BlockWriter for BlockWriterImpl {
    fn start(&mut self, cols_ndv_info: ColumnsNdvInfo) -> Result<()> {
        match self {
            BlockWriterImpl::Parquet(arrow_writer) => {
                let ArrowParquetWriter::Uninitialized(uninitialized) = arrow_writer else {
                    unreachable!(
                        "Unexpected writer state: ArrowWriterImpl::Parquet has been initialized"
                    );
                };

                let inner = uninitialized.init(cols_ndv_info)?;
                *arrow_writer = ArrowParquetWriter::Initialized(InitializedArrowWriter {
                    inner,
                    arrow_schema: uninitialized.arrow_schema.clone(),
                    use_variant_struct: uninitialized.use_variant_struct,
                    leaf_column_ids: uninitialized.leaf_column_ids.clone(),
                    shredded_columns: uninitialized.shredded_columns.clone(),
                });
                Ok(())
            }
            BlockWriterImpl::Native(native_writer) => Ok(native_writer.start()?),
        }
    }

    fn write(
        &mut self,
        block: DataBlock,
        schema: &TableSchema,
        inline_virtual_block: Option<&DataBlock>,
    ) -> Result<Option<HashSet<ColumnId>>> {
        match self {
            BlockWriterImpl::Parquet(writer) => {
                let (batch, batch_value_all_null_sources) = if writer.use_variant_struct() {
                    if let Some(shredded_columns) = writer.shredded_columns() {
                        let inline_block = inline_virtual_block.ok_or_else(|| {
                            ErrorCode::Internal(
                                "inline virtual block is required for variant shredding"
                                    .to_string(),
                            )
                        })?;
                        let (record_batch, value_all_null_sources) =
                            build_parquet_variant_record_batch_with_inline_shredding(
                                schema,
                                writer.arrow_schema(),
                                block,
                                shredded_columns,
                                inline_block,
                            )?;
                        (record_batch, Some(value_all_null_sources))
                    } else {
                        (
                            build_parquet_variant_record_batch_with_arrow_schema(
                                schema,
                                writer.arrow_schema(),
                                block,
                            )?,
                            None,
                        )
                    }
                } else {
                    (block.to_record_batch(schema)?, None)
                };
                writer.write(&batch)?;
                Ok(batch_value_all_null_sources)
            }
            BlockWriterImpl::Native(writer) => {
                let block = block.consume_convert_to_full();
                let batch: Vec<Column> = block
                    .take_columns()
                    .into_iter()
                    .map(|x| x.into_column().unwrap())
                    .collect();
                writer.write(&batch)?;
                Ok(None)
            }
        }
    }

    fn finish(&mut self, schema: &TableSchemaRef) -> Result<HashMap<ColumnId, ColumnMeta>> {
        match self {
            BlockWriterImpl::Parquet(writer) => {
                let leaf_column_ids = writer.leaf_column_ids().to_vec();
                let file_meta = writer.finish()?;
                if writer.use_variant_struct() {
                    column_parquet_metas_with_leaf_ids(&file_meta, &leaf_column_ids)
                } else {
                    column_parquet_metas(&file_meta, schema)
                }
            }
            BlockWriterImpl::Native(writer) => {
                writer.finish()?;
                let mut metas = HashMap::with_capacity(writer.metas.len());
                let leaf_column_ids = schema.to_leaf_column_ids();
                for (idx, meta) in writer.metas.iter().enumerate() {
                    // use column id as key instead of index
                    let column_id = leaf_column_ids.get(idx).unwrap();
                    metas.insert(*column_id, ColumnMeta::Native(meta.clone()));
                }
                Ok(metas)
            }
        }
    }

    fn inner_mut(&mut self) -> &mut Vec<u8> {
        match self {
            BlockWriterImpl::Parquet(writer) => writer.inner_mut(),
            BlockWriterImpl::Native(writer) => writer.inner_mut(),
        }
    }

    fn compressed_size(&self) -> usize {
        match self {
            BlockWriterImpl::Parquet(writer) => writer.in_progress_size(),
            BlockWriterImpl::Native(writer) => writer.total_size(),
        }
    }
}

impl BlockWriterImpl {
    fn set_shredded_columns(
        &mut self,
        table_schema: &TableSchema,
        shredded_columns: Vec<VariantShreddedColumn>,
    ) -> Result<()> {
        match self {
            BlockWriterImpl::Parquet(writer) => {
                writer.set_shredded_columns(table_schema, shredded_columns)
            }
            BlockWriterImpl::Native(_) => Ok(()),
        }
    }
}

pub struct StreamBlockBuilder {
    properties: Arc<StreamBlockProperties>,
    block_writer: BlockWriterImpl,
    inverted_index_writers: Vec<InvertedIndexWriter>,
    bloom_index_builder: BloomIndexBuilder,
    virtual_column_builder: Option<VirtualColumnBuilder>,
    inline_virtual_columns: Option<Vec<InlineVirtualColumn>>,
    inline_virtual_schema: Option<TableSchemaRef>,
    inline_virtual_stats_state: Option<ColumnStatisticsState>,
    inline_shredded_columns: Option<Vec<VariantShreddedColumn>>,
    inline_value_all_null_sources: Option<HashSet<ColumnId>>,
    vector_index_builder: Option<VectorIndexBuilder>,
    block_stats_builder: BlockStatsBuilder,

    cluster_stats_state: ClusterStatisticsState,
    column_stats_state: ColumnStatisticsState,

    row_count: usize,
    block_size: usize,
}

impl StreamBlockBuilder {
    pub fn try_new_with_config(properties: Arc<StreamBlockProperties>) -> Result<Self> {
        let buffer = Vec::with_capacity(DEFAULT_BLOCK_BUFFER_SIZE);
        let block_writer = match properties.write_settings.storage_format {
            FuseStorageFormat::Parquet => {
                BlockWriterImpl::Parquet(ArrowParquetWriter::new_uninitialized(
                    properties.write_settings.clone(),
                    properties.source_schema.clone(),
                ))
            }
            FuseStorageFormat::Native => {
                let mut default_compress_ratio = Some(2.10f64);
                if matches!(
                    properties.write_settings.table_compression,
                    TableCompression::Zstd
                ) {
                    default_compress_ratio = Some(3.72f64);
                }

                let writer = NativeWriter::new(
                    buffer,
                    properties.source_schema.as_ref().clone(),
                    WriteOptions {
                        default_compression: properties.write_settings.table_compression.into(),
                        max_page_size: Some(properties.write_settings.max_page_size),
                        default_compress_ratio,
                        forbidden_compressions: vec![],
                    },
                )?;
                BlockWriterImpl::Native(writer)
            }
        };

        let inverted_index_writers = properties
            .inverted_index_builders
            .iter()
            .map(|builder| {
                InvertedIndexWriter::try_create(Arc::new(builder.schema.clone()), &builder.options)
            })
            .collect::<Result<Vec<_>>>()?;

        let bloom_index_builder = BloomIndexBuilder::create(
            properties.ctx.get_function_context()?,
            properties.bloom_columns_map.clone(),
            &properties.ngram_args,
        )?;

        let virtual_column_builder = properties.virtual_column_builder.clone();
        let vector_index_builder = VectorIndexBuilder::try_create(
            &properties.table_indexes,
            properties.source_schema.clone(),
            true,
        );
        let block_stats_builder = BlockStatsBuilder::new(&properties.ndv_columns_map);
        let cluster_stats_state =
            ClusterStatisticsState::new(properties.cluster_stats_builder.clone());
        let column_stats_state =
            ColumnStatisticsState::new(&properties.stats_columns, &properties.distinct_columns);

        Ok(StreamBlockBuilder {
            properties,
            block_writer,
            inverted_index_writers,
            bloom_index_builder,
            virtual_column_builder,
            inline_virtual_columns: None,
            inline_virtual_schema: None,
            inline_virtual_stats_state: None,
            inline_shredded_columns: None,
            inline_value_all_null_sources: None,
            vector_index_builder,
            block_stats_builder,
            row_count: 0,
            block_size: 0,
            column_stats_state,
            cluster_stats_state,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    pub fn need_flush(&self) -> bool {
        let file_size = self.block_writer.compressed_size();
        self.row_count >= self.properties.block_thresholds.min_rows_per_block
            || self.block_size >= self.properties.block_thresholds.min_bytes_per_block * 2
            || (file_size >= self.properties.block_thresholds.min_compressed_per_block
                && self.block_size >= self.properties.block_thresholds.min_bytes_per_block)
    }

    pub fn write(&mut self, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }

        let had_existing_rows = self.row_count > 0;
        let enable_inline = self.properties.write_settings.variant_encoding
            == VariantEncoding::ParquetVariant
            && matches!(
                self.properties.write_settings.storage_format,
                FuseStorageFormat::Parquet
            );

        let block = self.cluster_stats_state.add_block(block)?;
        self.column_stats_state
            .add_block(&self.properties.source_schema, &block)?;
        self.bloom_index_builder.add_block(&block)?;
        self.block_stats_builder.add_block(&block)?;
        for writer in self.inverted_index_writers.iter_mut() {
            writer.add_block(&self.properties.source_schema, &block)?;
        }
        if let Some(ref mut virtual_column_builder) = self.virtual_column_builder {
            virtual_column_builder.add_block(&block)?;
        }
        if let Some(ref mut vector_index_builder) = self.vector_index_builder {
            vector_index_builder.add_block(&block)?;
        }
        self.row_count += block.num_rows();
        self.block_size += block.estimate_block_size();

        if !had_existing_rows {
            if enable_inline && self.inline_shredded_columns.is_none() {
                // Inline shredding selects a fixed typed_value schema once (based on the first
                // block/sample) and reuses it for all subsequent blocks. This keeps the Parquet
                // layout stable, but later blocks can legitimately have all-NULL typed_value
                // columns when the keypath is absent in that block.
                if let Some(ref virtual_column_builder) = self.virtual_column_builder {
                    let mut builder = virtual_column_builder.clone();
                    if let Some(inline) = builder.build_inline_virtual_columns()? {
                        let mut shredded_columns = Vec::with_capacity(inline.columns.len());
                        for (idx, column) in inline.columns.iter().enumerate() {
                            let data_type = variant_data_type_to_arrow(&column.data_type)
                                .ok_or_else(|| {
                                    ErrorCode::Internal(
                                        "unsupported virtual column type for inline shredding"
                                            .to_string(),
                                    )
                                })?;
                            let column_id = VIRTUAL_COLUMN_ID_START + idx as u32;
                            shredded_columns.push(VariantShreddedColumn {
                                source_column_id: column.source_column_id,
                                column_id,
                                key_paths: column.key_paths.clone(),
                                data_type,
                            });
                        }
                        if !shredded_columns.is_empty() {
                            let inline_schema =
                                build_inline_virtual_schema(inline.columns.as_slice())?;
                            let stats_columns = inline_schema
                                .fields()
                                .iter()
                                .map(|field| (field.column_id(), DataType::from(field.data_type())))
                                .collect::<Vec<_>>();
                            self.inline_virtual_columns = Some(inline.columns.clone());
                            self.inline_virtual_schema = Some(inline_schema);
                            self.inline_virtual_stats_state =
                                Some(ColumnStatisticsState::new(&stats_columns, &[]));
                            self.inline_shredded_columns = Some(shredded_columns.clone());
                            self.block_writer.set_shredded_columns(
                                self.properties.source_schema.as_ref(),
                                shredded_columns,
                            )?;
                            self.virtual_column_builder = None;
                        }
                    }
                }
            }
            // Writer properties must be fixed before the ArrowWriter starts, so we rely on the first
            // block's NDV stats to heuristically configure the parquet writer.
            let mut cols_ndv = self.column_stats_state.peek_cols_ndv();
            cols_ndv.extend(self.block_stats_builder.peek_cols_ndv());
            self.block_writer
                .start(ColumnsNdvInfo::new(block.num_rows(), cols_ndv))?;
        }

        let inline_virtual_block = if let Some(ref columns) = self.inline_virtual_columns {
            Some(
                VirtualColumnBuilder::build_inline_virtual_block_for_columns(
                    &block,
                    &self.properties.source_schema,
                    columns,
                )?,
            )
        } else {
            None
        };

        if let (Some(schema), Some(stats_state), Some(inline_block)) = (
            self.inline_virtual_schema.as_ref(),
            self.inline_virtual_stats_state.as_mut(),
            inline_virtual_block.as_ref(),
        ) {
            stats_state.add_block(schema, inline_block)?;
        }

        let value_all_null_sources = self.block_writer.write(
            block,
            &self.properties.source_schema,
            inline_virtual_block.as_ref(),
        )?;
        if let Some(value_all_null_sources) = value_all_null_sources {
            if let Some(shredded_columns) = self.inline_shredded_columns.as_ref() {
                metrics_inc_variant_shredding_inline_columns(shredded_columns.len() as u64);
                metrics_inc_variant_shredding_inline_value_all_null_columns(
                    value_all_null_sources.len() as u64,
                );
            }
            self.inline_value_all_null_sources =
                Some(match self.inline_value_all_null_sources.take() {
                    None => value_all_null_sources,
                    Some(mut existing) => {
                        existing.retain(|column_id| value_all_null_sources.contains(column_id));
                        existing
                    }
                });
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<BlockSerialization> {
        let (block_location, block_id) = self
            .properties
            .meta_locations
            .gen_block_location(self.properties.table_meta_timestamps);

        let bloom_index_location = self
            .properties
            .meta_locations
            .block_bloom_index_location(&block_id);
        let bloom_index = self.bloom_index_builder.finalize()?;
        let bloom_index_state = if let Some(bloom_index) = bloom_index {
            Some(BloomIndexState::from_bloom_index(
                &bloom_index,
                bloom_index_location,
            )?)
        } else {
            None
        };
        let mut column_distinct_count = bloom_index_state
            .as_ref()
            .map(|i| i.column_distinct_count.clone())
            .unwrap_or_default();
        let column_hlls = self.block_stats_builder.finalize()?;
        if let Some(hlls) = &column_hlls {
            for (key, val) in hlls {
                if let Entry::Vacant(entry) = column_distinct_count.entry(*key) {
                    entry.insert(val.count());
                }
            }
        }
        let col_stats = self.column_stats_state.finalize(column_distinct_count)?;

        let mut inverted_index_states = Vec::with_capacity(self.inverted_index_writers.len());
        for (i, inverted_index_writer) in std::mem::take(&mut self.inverted_index_writers)
            .into_iter()
            .enumerate()
        {
            let inverted_index_location = self.properties.inverted_index_builders[i]
                .gen_inverted_index_location(&block_location);
            let data = inverted_index_writer.finalize()?;
            let inverted_index_state =
                InvertedIndexState::try_create(data, inverted_index_location)?;
            inverted_index_states.push(inverted_index_state);
        }
        let vector_index_state =
            if let Some(ref mut vector_index_builder) = self.vector_index_builder {
                let vector_index_location =
                    self.properties.meta_locations.block_vector_index_location();
                let vector_index_state = vector_index_builder.finalize(&vector_index_location)?;
                Some(vector_index_state)
            } else {
                None
            };
        let vector_index_size = vector_index_state.as_ref().map(|v| v.size);
        let vector_index_location = vector_index_state.as_ref().map(|v| v.location.clone());

        let col_metas = self.block_writer.finish(&self.properties.source_schema)?;
        let block_raw_data = mem::take(self.block_writer.inner_mut());

        let virtual_column_state = if let Some(ref columns) = self.inline_virtual_columns {
            let columns_statistics = self
                .inline_virtual_stats_state
                .take()
                .map(|stats| stats.finalize(HashMap::new()))
                .transpose()?
                .unwrap_or_else(StatisticsOfColumns::new);
            let draft_virtual_column_metas =
                VirtualColumnBuilder::column_metas_to_virtual_column_metas(
                    &col_metas,
                    columns.clone(),
                    columns_statistics,
                    VIRTUAL_COLUMN_ID_START,
                    self.inline_value_all_null_sources.as_ref(),
                )?;
            let draft_virtual_block_meta = DraftVirtualBlockMeta {
                virtual_column_metas: draft_virtual_column_metas,
                virtual_column_size: 0,
                virtual_location: block_location.clone(),
            };
            Some(VirtualColumnState {
                data: vec![],
                draft_virtual_block_meta,
            })
        } else if let Some(ref mut virtual_column_builder) = self.virtual_column_builder {
            let virtual_column_state = virtual_column_builder
                .finalize(&self.properties.write_settings, &block_location)?;
            Some(virtual_column_state)
        } else {
            None
        };

        let file_size = block_raw_data.len();
        let inverted_index_size = inverted_index_states
            .iter()
            .map(|v| v.size)
            .reduce(|a, b| a + b);
        let perfect = self.properties.block_thresholds.check_perfect_block(
            self.row_count,
            self.block_size,
            file_size,
        );
        let cluster_stats = self.cluster_stats_state.finalize(perfect)?;
        let block_meta = BlockMeta {
            row_count: self.row_count as u64,
            block_size: self.block_size as u64,
            file_size: file_size as u64,
            col_stats,
            col_metas,
            cluster_stats,
            location: block_location,
            bloom_filter_index_location: bloom_index_state.as_ref().map(|v| v.location.clone()),
            bloom_filter_index_size: bloom_index_state
                .as_ref()
                .map(|v| v.size)
                .unwrap_or_default(),
            compression: self.properties.write_settings.table_compression.into(),
            variant_encoding: self.properties.write_settings.variant_encoding,
            inverted_index_size,
            vector_index_size,
            vector_index_location,
            create_on: Some(Utc::now()),
            ngram_filter_index_size: bloom_index_state
                .as_ref()
                .map(|v| v.ngram_size)
                .unwrap_or_default(),
            virtual_block_meta: None,
        };
        let serialized = BlockSerialization {
            block_raw_data,
            block_meta,
            bloom_index_state,
            inverted_index_states,
            virtual_column_state,
            vector_index_state,
            column_hlls: column_hlls.map(BlockHLLState::Deserialized),
        };
        Ok(serialized)
    }
}

pub struct StreamBlockProperties {
    pub(crate) ctx: Arc<dyn TableContext>,
    pub(crate) write_settings: WriteSettings,
    pub(crate) block_thresholds: BlockThresholds,

    meta_locations: TableMetaLocationGenerator,
    source_schema: TableSchemaRef,

    cluster_stats_builder: Arc<ClusterStatisticsBuilder>,
    stats_columns: Vec<(ColumnId, DataType)>,
    distinct_columns: Vec<(ColumnId, DataType)>,
    bloom_columns_map: BTreeMap<FieldIndex, TableField>,
    ndv_columns_map: BTreeMap<FieldIndex, TableField>,
    ngram_args: Vec<NgramArgs>,
    inverted_index_builders: Vec<InvertedIndexBuilder>,
    virtual_column_builder: Option<VirtualColumnBuilder>,
    table_meta_timestamps: TableMetaTimestamps,
    table_indexes: BTreeMap<String, TableIndex>,
}

impl StreamBlockProperties {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        table: &FuseTable,
        kind: MutationKind,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<Arc<Self>> {
        let schema = table.schema();
        // remove virtual computed fields.
        let mut fields = schema
            .fields()
            .iter()
            .filter(|f| !matches!(f.computed_expr(), Some(ComputedExpr::Virtual(_))))
            .cloned()
            .collect::<Vec<_>>();
        if !matches!(kind, MutationKind::Insert | MutationKind::Replace) {
            // add stream fields.
            for stream_column in table.stream_columns().iter() {
                fields.push(stream_column.table_field());
            }
        }

        let source_schema = Arc::new(TableSchema {
            fields,
            ..schema.as_ref().clone()
        });

        let write_settings = table.get_write_settings_with_variant(ctx.as_ref());

        let bloom_columns_map = table
            .bloom_index_cols
            .bloom_index_fields(source_schema.clone(), BloomIndex::supported_type)?;
        let ngram_args =
            FuseTable::create_ngram_index_args(&table.table_info.meta.indexes, &schema, true)?;
        let ndv_columns_map = table
            .approx_distinct_cols
            .distinct_column_fields(source_schema.clone(), RangeIndex::supported_table_type)?;
        let bloom_ndv_columns = bloom_columns_map
            .values()
            .chain(ndv_columns_map.values())
            .map(|v| v.column_id())
            .collect::<HashSet<_>>();

        let inverted_index_builders =
            create_inverted_index_builders(&table.table_info.meta.indexes, &schema);
        let virtual_column_builder = if table.support_virtual_columns() {
            VirtualColumnBuilder::try_create(ctx.clone(), source_schema.clone()).ok()
        } else {
            None
        };

        let cluster_stats_builder =
            ClusterStatisticsBuilder::try_create(table, ctx.clone(), &source_schema)?;

        let mut stats_columns = vec![];
        let mut distinct_columns = vec![];
        let leaf_fields = source_schema.leaf_fields();
        for field in leaf_fields.iter() {
            let column_id = field.column_id();
            let data_type = DataType::from(field.data_type());
            if RangeIndex::supported_type(&data_type) {
                stats_columns.push((column_id, data_type.clone()));
                if !bloom_ndv_columns.contains(&column_id) {
                    distinct_columns.push((column_id, data_type));
                }
            }
        }
        let table_indexes = table.table_info.meta.indexes.clone();
        Ok(Arc::new(StreamBlockProperties {
            ctx,
            meta_locations: table.meta_location_generator().clone(),
            block_thresholds: table.get_block_thresholds(),
            source_schema,
            write_settings,
            cluster_stats_builder,
            virtual_column_builder,
            stats_columns,
            distinct_columns,
            bloom_columns_map,
            ngram_args,
            inverted_index_builders,
            table_meta_timestamps,
            table_indexes,
            ndv_columns_map,
        }))
    }
}
