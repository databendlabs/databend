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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_metrics::storage::*;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

use super::parquet_data_source::ParquetDataSource;
use super::util::add_data_block_meta;
use super::util::need_reserve_block_info;
use crate::fuse_part::FuseBlockPartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::VirtualColumnReader;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;

pub struct DeserializeDataTransform {
    block_reader: Arc<BlockReader>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    src_schema: DataSchema,
    output_schema: DataSchema,
    parts: Vec<PartInfoPtr>,
    chunks: Vec<ParquetDataSource>,

    index_reader: Arc<Option<AggIndexReader>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,

    base_block_ids: Option<Scalar>,
    need_reserve_block_info: bool,
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
        let mut src_schema: DataSchema = (block_reader.schema().as_ref()).into();
        if let Some(virtual_reader) = virtual_reader.as_ref() {
            let mut fields = src_schema.fields().clone();
            for virtual_column_field in &virtual_reader.virtual_column_info.virtual_column_fields {
                let field = DataField::new(
                    &virtual_column_field.name,
                    DataType::from(&*virtual_column_field.data_type),
                );
                fields.push(field);
            }
            src_schema = DataSchema::new(fields);
        }

        let mut output_schema = plan.schema().as_ref().clone();
        output_schema.remove_internal_fields();
        let output_schema: DataSchema = (&output_schema).into();
        let (need_reserve_block_info, _) = need_reserve_block_info(ctx.clone(), plan.table_index);
        Ok(ProcessorPtr::create(Box::new(DeserializeDataTransform {
            block_reader,
            input,
            output,
            output_data: None,
            src_schema,
            output_schema,
            parts: vec![],
            chunks: vec![],
            index_reader,
            virtual_reader,
            base_block_ids: plan.base_block_ids.clone(),
            need_reserve_block_info,
        })))
    }
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
                    let block = agg_index_reader.deserialize_parquet_data(actual_part, data)?;

                    self.output_data = Some(block);
                }
                ParquetDataSource::Normal((data, virtual_data)) => {
                    let start = Instant::now();
                    let columns_chunks = data.columns_chunks()?;
                    let part = FuseBlockPartInfo::from_part(&part)?;

                    let mut data_block = self.block_reader.deserialize_parquet_chunks(
                        part.nums_rows,
                        &part.columns_meta,
                        columns_chunks,
                        &part.compression,
                        &part.location,
                    )?;

                    // Add optional virtual columns
                    if let Some(virtual_reader) = self.virtual_reader.as_ref() {
                        data_block = virtual_reader
                            .deserialize_virtual_columns(data_block.clone(), virtual_data)?;
                    }

                    // Perf.
                    {
                        metrics_inc_remote_io_deserialize_milliseconds(
                            start.elapsed().as_millis() as u64
                        );
                    }

                    let mut data_block =
                        data_block.resort(&self.src_schema, &self.output_schema)?;

                    // Fill `BlockMetaIndex` as `DataBlock.meta` if query internal columns,
                    // `TransformAddInternalColumns` will generate internal columns using `BlockMetaIndex` in next pipeline.
                    let offsets = None;

                    data_block = add_data_block_meta(
                        data_block,
                        part,
                        offsets,
                        self.base_block_ids.clone(),
                        self.block_reader.update_stream_columns(),
                        self.block_reader.query_internal_columns(),
                        self.need_reserve_block_info,
                    )?;

                    self.output_data = Some(data_block);
                }
            }
        }

        Ok(())
    }
}
