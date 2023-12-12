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

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_catalog::plan::gen_mutation_stream_meta;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::{BlockMetaInfoDowncast, Expr};
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Scalar;
use databend_common_metrics::storage::*;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_sql::IndexType;

use super::fuse_source::fill_internal_column_meta;
use super::parquet_data_source::ParquetDataSource;
use crate::fuse_part::FusePartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::UncompressedBuffer;
use crate::io::VirtualColumnReader;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;

pub struct DeserializeDataTransform {
    ctx: Arc<dyn TableContext>,
    table_index: IndexType,
    scan_progress: Arc<Progress>,
    block_reader: Arc<BlockReader>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    src_schema: DataSchema,
    output_schema: DataSchema,
    parts: Vec<PartInfoPtr>,
    chunks: Vec<ParquetDataSource>,
    uncompressed_buffer: Arc<UncompressedBuffer>,

    index_reader: Arc<Option<AggIndexReader>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,

    base_block_ids: Option<Scalar>,
    cached_runtime_filter: Option<Expr>,
}

#[macro_export]
macro_rules! impl_runtime_filter {
    () => {
        fn runtime_filter(&mut self, data_block: DataBlock) -> Result<DataBlock> {
            // Check if already cached runtime filters
            if self.cached_runtime_filter.is_none() {
                let runtime_filters = self.ctx.get_runtime_filter_with_id(self.table_index);
                let filter = runtime_filters
                    .iter()
                    .map(|filter| {
                        cast_expr_to_non_null_boolean(
                            filter
                                .project_column_ref(|name| self.src_schema.index_of(name).unwrap()),
                        )
                        .unwrap()
                    })
                    .try_reduce(|lhs, rhs| {
                        check_function(None, "and_filters", &[], &[lhs, rhs], &BUILTIN_FUNCTIONS)
                    })
                    .transpose()
                    .unwrap_or_else(|| {
                        Err(ErrorCode::Internal(
                            "Invalid empty predicate list".to_string(),
                        ))
                    })?;
                self.cached_runtime_filter = Some(filter);
            }
            // Using runtime filter to filter data_block
            let func_ctx = self.ctx.get_function_context()?;
            let evaluator = Evaluator::new(&data_block, &func_ctx, &BUILTIN_FUNCTIONS);
            let filter = evaluator
                .run(self.cached_runtime_filter.as_ref().unwrap())?
                .try_downcast::<BooleanType>()
                .unwrap();
            data_block.filter_boolean_value(&filter)
        }
    };
}

unsafe impl Send for DeserializeDataTransform {}

impl DeserializeDataTransform {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        block_reader: Arc<BlockReader>,
        plan: &DataSourcePlan,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
    ) -> Result<ProcessorPtr> {
        let buffer_size = ctx.get_settings().get_parquet_uncompressed_buffer_size()? as usize;
        let scan_progress = ctx.get_scan_progress();

        let mut src_schema: DataSchema = (block_reader.schema().as_ref()).into();
        if let Some(virtual_reader) = virtual_reader.as_ref() {
            let mut fields = src_schema.fields().clone();
            for virtual_column in &virtual_reader.virtual_column_infos {
                let field = DataField::new(
                    &virtual_column.name,
                    DataType::from(&*virtual_column.data_type),
                );
                fields.push(field);
            }
            src_schema = DataSchema::new(fields);
        }

        let mut output_schema = plan.schema().as_ref().clone();
        output_schema.remove_internal_fields();
        let output_schema: DataSchema = (&output_schema).into();

        Ok(ProcessorPtr::create(Box::new(DeserializeDataTransform {
            ctx,
            table_index: plan.table_index,
            scan_progress,
            block_reader,
            input,
            output,
            output_data: None,
            src_schema,
            output_schema,
            parts: vec![],
            chunks: vec![],
            uncompressed_buffer: UncompressedBuffer::new(buffer_size),
            index_reader,
            virtual_reader,
            base_block_ids: plan.base_block_ids.clone(),
            cached_runtime_filter: None,
        })))
    }

    impl_runtime_filter!();
}

#[async_trait::async_trait]
impl Processor for DeserializeDataTransform {
    fn name(&self) -> String {
        String::from("DeserializeDataTransform")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            self.uncompressed_buffer.clear();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if !self.chunks.is_empty() {
            if !self.input.has_data() {
                self.input.set_need_data();
            }

            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;
            if let Some(source_meta) = data_block.take_meta() {
                if let Some(source_meta) = DataSourceWithMeta::downcast_from(source_meta) {
                    self.parts = source_meta.meta;
                    self.chunks = source_meta.data;
                    return Ok(Event::Sync);
                }
            }

            unreachable!();
        }

        if self.input.is_finished() {
            self.output.finish();
            self.uncompressed_buffer.clear();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        let part = self.parts.pop();
        let chunks = self.chunks.pop();
        if let Some((part, read_res)) = part.zip(chunks) {
            match read_res {
                ParquetDataSource::AggIndex((actual_part, data)) => {
                    let agg_index_reader = self.index_reader.as_ref().as_ref().unwrap();
                    let block = agg_index_reader.deserialize_parquet_data(
                        actual_part,
                        data,
                        self.uncompressed_buffer.clone(),
                    )?;

                    let progress_values = ProgressValues {
                        rows: block.num_rows(),
                        bytes: block.memory_size(),
                    };
                    self.scan_progress.incr(&progress_values);

                    self.output_data = Some(block);
                }
                ParquetDataSource::Normal((data, virtual_data)) => {
                    let start = Instant::now();
                    let columns_chunks = data.columns_chunks()?;
                    let part = FusePartInfo::from_part(&part)?;

                    let mut data_block = self.block_reader.deserialize_parquet_chunks_with_buffer(
                        &part.location,
                        part.nums_rows,
                        &part.compression,
                        &part.columns_meta,
                        columns_chunks,
                        Some(self.uncompressed_buffer.clone()),
                    )?;

                    if self.ctx.has_runtime_filters(self.table_index)
                        && data_block.num_rows() < 1024
                    {
                        data_block = self.runtime_filter(data_block)?;
                    }

                    // Add optional virtual columns
                    if let Some(virtual_reader) = self.virtual_reader.as_ref() {
                        data_block = virtual_reader.deserialize_virtual_columns(
                            data_block,
                            virtual_data,
                            Some(self.uncompressed_buffer.clone()),
                        )?;
                    }

                    // Perf.
                    {
                        metrics_inc_remote_io_deserialize_milliseconds(
                            start.elapsed().as_millis() as u64
                        );
                    }

                    let progress_values = ProgressValues {
                        rows: data_block.num_rows(),
                        bytes: data_block.memory_size(),
                    };
                    self.scan_progress.incr(&progress_values);

                    let mut data_block =
                        data_block.resort(&self.src_schema, &self.output_schema)?;

                    // Fill `BlockMetaIndex` as `DataBlock.meta` if query internal columns,
                    // `TransformAddInternalColumns` will generate internal columns using `BlockMetaIndex` in next pipeline.
                    if self.block_reader.query_internal_columns() {
                        data_block = fill_internal_column_meta(
                            data_block,
                            part,
                            None,
                            self.base_block_ids.clone(),
                        )?;
                    }

                    if self.block_reader.update_stream_columns() {
                        let inner_meta = data_block.take_meta();
                        let meta = gen_mutation_stream_meta(inner_meta, &part.location)?;
                        data_block = data_block.add_meta(Some(Box::new(meta)))?;
                    }

                    self.output_data = Some(data_block);
                }
            }
        }

        Ok(())
    }
}
