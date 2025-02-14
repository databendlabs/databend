// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::infer_schema_type;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use databend_common_meta_app::schema::VirtualField;
use databend_common_metrics::storage::metrics_inc_block_virtual_column_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_virtual_column_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_virtual_column_write_nums;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sinks::AsyncSink;
use databend_common_pipeline_sinks::AsyncSinker;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::parse_computed_expr;
use databend_common_storage::read_parquet_schema_async_rs;
use databend_common_storages_fuse::io::serialize_block;
use databend_common_storages_fuse::io::write_data;
use databend_common_storages_fuse::io::BlockReader;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::io::WriteSettings;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::Location;
use opendal::Operator;

// The big picture of refresh virtual column into pipeline:
//
//                                    ┌─────────────────────────┐
//                             ┌────> │ VirtualColumnTransform1 │ ────┐
//                             │      └─────────────────────────┘     │
//                             │                  ...                 │
// ┌─────────────────────┐     │      ┌─────────────────────────┐     │      ┌───────────────────┐
// │ VirtualColumnSource │ ────┼────> │ VirtualColumnTransformN │ ────┼────> │ VirtualColumnSink │
// └─────────────────────┘     │      └─────────────────────────┘     │      └───────────────────┘
//                             │                  ...                 │
//                             │      ┌─────────────────────────┐     │
//                             └────> │ VirtualColumnTransformZ │ ────┘
//                                    └─────────────────────────┘
//
#[async_backtrace::framed]
pub async fn do_refresh_virtual_column(
    ctx: Arc<dyn TableContext>,
    fuse_table: &FuseTable,
    virtual_columns: Vec<VirtualField>,
    segment_locs: Option<Vec<Location>>,
    pipeline: &mut Pipeline,
) -> Result<()> {
    if virtual_columns.is_empty() {
        return Ok(());
    }

    let snapshot_opt = fuse_table.read_table_snapshot().await?;
    let snapshot = if let Some(val) = snapshot_opt {
        val
    } else {
        // no snapshot
        return Ok(());
    };

    let table_schema = &fuse_table.get_table_info().meta.schema;

    // Collect source fields used by virtual columns.
    let mut field_indices = Vec::new();
    for (i, f) in table_schema.fields().iter().enumerate() {
        if f.data_type().remove_nullable() != TableDataType::Variant {
            continue;
        }
        let is_src_field = virtual_columns.iter().any(|v| v.expr.starts_with(f.name()));
        if is_src_field {
            field_indices.push(i);
        }
    }

    if field_indices.is_empty() {
        // no source variant column
        return Ok(());
    }
    let projected_schema = table_schema.project(&field_indices);
    let source_schema = Arc::new(DataSchema::from(&projected_schema));

    let projection = Projection::Columns(field_indices);
    let block_reader =
        fuse_table.create_block_reader(ctx.clone(), projection, false, false, false)?;

    let segment_reader =
        MetaReaders::segment_info_reader(fuse_table.get_operator(), table_schema.clone());

    let write_settings = fuse_table.get_write_settings();
    let storage_format = write_settings.storage_format;

    let operator = fuse_table.get_operator_ref();

    // If no segment locations are specified, iterates through all segments
    let segment_locs = if let Some(segment_locs) = segment_locs {
        segment_locs
    } else {
        snapshot.segments.clone()
    };

    // Read source variant columns and extract inner fields as virtual columns.
    let mut block_metas = VecDeque::new();
    for (location, ver) in segment_locs {
        let segment_info = segment_reader
            .read(&LoadParams {
                location: location.to_string(),
                len_hint: None,
                ver,
                put_cache: false,
            })
            .await?;

        for block_meta in segment_info.block_metas()? {
            let virtual_loc =
                TableMetaLocationGenerator::gen_virtual_block_location(&block_meta.location.0);

            let arrow_schema = match storage_format {
                FuseStorageFormat::Parquet => {
                    read_parquet_schema_async_rs(operator, &virtual_loc, None)
                        .await
                        .ok()
                }
                FuseStorageFormat::Native => {
                    BlockReader::async_read_native_schema(operator, &virtual_loc)
                        .await
                        .map(|(_, schema)| schema)
                }
            };

            // if all virtual columns has be generated, we can ignore this block
            let all_generated = if let Some(arrow_schema) = arrow_schema {
                let virtual_table_schema = TableSchema::try_from(&arrow_schema)?;
                virtual_columns.iter().all(|v| {
                    virtual_table_schema
                        .fields
                        .iter()
                        .any(|f| *f.name() == v.expr && *f.data_type() == v.data_type)
                })
            } else {
                false
            };
            if all_generated {
                continue;
            }

            block_metas.push_back(block_meta);
        }
    }

    if block_metas.is_empty() {
        return Ok(());
    }

    let settings = ReadSettings::from_ctx(&ctx)?;
    pipeline.add_source(
        |output| {
            let inner = VirtualColumnSource::new(
                settings,
                storage_format,
                block_reader.clone(),
                block_metas.clone(),
            );
            AsyncSourcer::create(ctx.clone(), output, inner)
        },
        1,
    )?;

    let mut virtual_fields = Vec::with_capacity(virtual_columns.len());
    let mut virtual_exprs = Vec::with_capacity(virtual_columns.len());
    for virtual_column_field in virtual_columns {
        let mut virtual_expr = parse_computed_expr(
            ctx.clone(),
            source_schema.clone(),
            &virtual_column_field.expr,
        )?;

        if virtual_column_field.data_type.remove_nullable() != TableDataType::Variant {
            virtual_expr = Expr::Cast {
                span: None,
                is_try: true,
                expr: Box::new(virtual_expr),
                dest_type: (&virtual_column_field.data_type).into(),
            }
        }
        let virtual_field = TableField::new(
            &virtual_column_field.expr,
            infer_schema_type(virtual_expr.data_type())?,
        );
        virtual_exprs.push(virtual_expr);
        virtual_fields.push(virtual_field);
    }
    let virtual_schema = TableSchemaRefExt::create(virtual_fields);

    let block_nums = block_metas.len();
    let max_threads = ctx.get_settings().get_max_threads()? as usize;
    let max_threads = std::cmp::min(block_nums, max_threads);
    pipeline.try_resize(max_threads)?;
    pipeline.add_async_transformer(|| {
        VirtualColumnTransform::new(
            ctx.clone(),
            write_settings.clone(),
            virtual_exprs.clone(),
            virtual_schema.clone(),
            operator.clone(),
        )
    });

    pipeline.try_resize(1)?;
    pipeline.add_sink(|input| VirtualColumnSink::try_create(input, block_nums))?;

    Ok(())
}

/// `VirtualColumnSource` is used to read data blocks that need generate virtual columns.
pub struct VirtualColumnSource {
    settings: ReadSettings,
    storage_format: FuseStorageFormat,
    block_reader: Arc<BlockReader>,
    block_metas: VecDeque<Arc<BlockMeta>>,
    is_finished: bool,
}

impl VirtualColumnSource {
    pub fn new(
        settings: ReadSettings,
        storage_format: FuseStorageFormat,
        block_reader: Arc<BlockReader>,
        block_metas: VecDeque<Arc<BlockMeta>>,
    ) -> Self {
        Self {
            settings,
            storage_format,
            block_reader,
            block_metas,
            is_finished: false,
        }
    }
}

#[async_trait::async_trait]
impl AsyncSource for VirtualColumnSource {
    const NAME: &'static str = "VirtualColumnSource";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }

        match self.block_metas.pop_front() {
            Some(block_meta) => {
                let block = self
                    .block_reader
                    .read_by_meta(&self.settings, &block_meta, &self.storage_format)
                    .await?;
                let block = block.add_meta(Some(Box::new(Arc::unwrap_or_clone(block_meta))))?;
                Ok(Some(block))
            }
            None => {
                self.is_finished = true;
                Ok(None)
            }
        }
    }
}

/// `VirtualColumnTransform` is used to generate virtual columns for each blocks.
pub struct VirtualColumnTransform {
    ctx: Arc<dyn TableContext>,
    write_settings: WriteSettings,
    virtual_exprs: Vec<Expr>,
    virtual_schema: TableSchemaRef,
    operator: Operator,
}

impl VirtualColumnTransform {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        write_settings: WriteSettings,
        virtual_exprs: Vec<Expr>,
        virtual_schema: TableSchemaRef,
        operator: Operator,
    ) -> Self {
        Self {
            ctx,
            write_settings,
            virtual_exprs,
            virtual_schema,
            operator,
        }
    }
}

#[async_trait::async_trait]
impl AsyncTransform for VirtualColumnTransform {
    const NAME: &'static str = "VirtualColumnTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        let block_meta = data_block
            .get_meta()
            .and_then(BlockMeta::downcast_ref_from)
            .unwrap();

        let virtual_location =
            TableMetaLocationGenerator::gen_virtual_block_location(&block_meta.location.0);

        let start = Instant::now();

        let len = data_block.num_rows();
        let func_ctx = self.ctx.get_function_context()?;
        let evaluator = Evaluator::new(&data_block, &func_ctx, &BUILTIN_FUNCTIONS);
        let mut virtual_entries = Vec::with_capacity(self.virtual_exprs.len());
        for virtual_expr in &self.virtual_exprs {
            let value = evaluator.run(virtual_expr)?;
            let virtual_entry = BlockEntry::new(virtual_expr.data_type().clone(), value);
            virtual_entries.push(virtual_entry);
        }
        let virtual_block = DataBlock::new(virtual_entries, len);

        let mut buffer = Vec::with_capacity(DEFAULT_BLOCK_BUFFER_SIZE);

        let _ = serialize_block(
            &self.write_settings,
            &self.virtual_schema,
            virtual_block,
            &mut buffer,
        )?;

        let virtual_column_size = buffer.len() as u64;
        write_data(buffer, &self.operator, &virtual_location).await?;

        // Perf.
        {
            metrics_inc_block_virtual_column_write_nums(1);
            metrics_inc_block_virtual_column_write_bytes(virtual_column_size);
            metrics_inc_block_virtual_column_write_milliseconds(start.elapsed().as_millis() as u64);
        }

        let new_block = DataBlock::new(vec![], 0);
        Ok(new_block)
    }
}

/// `VirtualColumnSink` is used to finish build virtual column pipeline.
pub struct VirtualColumnSink {
    block_nums: AtomicUsize,
}

impl VirtualColumnSink {
    pub fn try_create(input: Arc<InputPort>, block_nums: usize) -> Result<ProcessorPtr> {
        let sinker = AsyncSinker::create(input, VirtualColumnSink {
            block_nums: AtomicUsize::new(block_nums),
        });
        Ok(ProcessorPtr::create(sinker))
    }
}

#[async_trait]
impl AsyncSink for VirtualColumnSink {
    const NAME: &'static str = "VirtualColumnSink";

    #[async_backtrace::framed]
    async fn consume(&mut self, _data_block: DataBlock) -> Result<bool> {
        let num = self.block_nums.fetch_sub(1, Ordering::SeqCst);
        if num > 1 {
            return Ok(false);
        }

        Ok(true)
    }
}
