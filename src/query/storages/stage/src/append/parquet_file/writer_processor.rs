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
use std::collections::VecDeque;
use std::mem;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_config::QUERY_SEMVER;
use databend_common_exception::Result;
use databend_common_expression::converts::arrow::table_schema_to_arrow_schema;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_storages_common_table_meta::table::TableCompression;
use opendal::Operator;
use parquet::arrow::ArrowWriter;
use parquet::basic::Encoding;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;

use super::block_batch::BlockBatch;
use crate::append::output::DataSummary;
use crate::append::path::unload_path;
use crate::append::UnloadOutput;

pub struct ParquetFileWriter {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    table_info: StageTableInfo,
    arrow_schema: Arc<ArrowSchema>,

    input_data: Vec<DataBlock>,

    input_bytes: usize,
    row_counts: usize,
    writer: ArrowWriter<Vec<u8>>,

    file_to_write: Option<(Vec<u8>, DataSummary)>,
    data_accessor: Operator,

    // the result of statement
    unload_output: UnloadOutput,
    unload_output_blocks: Option<VecDeque<DataBlock>>,

    uuid: String,
    group_id: usize,
    batch_id: usize,

    targe_file_size: Option<usize>,
}

const MAX_BUFFER_SIZE: usize = 64 * 1024 * 1024;
// this is number of rows, not size
const MAX_ROW_GROUP_SIZE: usize = 1024 * 1024;

fn create_writer(
    arrow_schema: Arc<ArrowSchema>,
    targe_file_size: Option<usize>,
) -> Result<ArrowWriter<Vec<u8>>> {
    let props = WriterProperties::builder()
        .set_compression(TableCompression::Zstd.into())
        .set_max_row_group_size(MAX_ROW_GROUP_SIZE)
        .set_encoding(Encoding::PLAIN)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_bloom_filter_enabled(false)
        .set_created_by(format!("Databend {}", *QUERY_SEMVER))
        .build();
    let buf_size = match targe_file_size {
        Some(n) if n < MAX_BUFFER_SIZE => n,
        _ => MAX_BUFFER_SIZE,
    };
    let writer = ArrowWriter::try_new(Vec::with_capacity(buf_size), arrow_schema, Some(props))?;
    Ok(writer)
}

impl ParquetFileWriter {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table_info: StageTableInfo,
        data_accessor: Operator,
        uuid: String,
        group_id: usize,
        targe_file_size: Option<usize>,
    ) -> Result<ProcessorPtr> {
        let unload_output =
            UnloadOutput::create(table_info.stage_info.copy_options.detailed_output);

        let arrow_schema = Arc::new(table_schema_to_arrow_schema(&table_info.schema));
        let writer = create_writer(arrow_schema.clone(), targe_file_size)?;

        Ok(ProcessorPtr::create(Box::new(ParquetFileWriter {
            input,
            output,
            table_info,
            arrow_schema,
            unload_output,
            unload_output_blocks: None,
            writer,
            input_data: Vec::new(),
            input_bytes: 0,
            file_to_write: None,
            data_accessor,
            uuid,
            group_id,
            batch_id: 0,
            targe_file_size,
            row_counts: 0,
        })))
    }
    pub fn reinit_writer(&mut self) -> Result<()> {
        self.writer = create_writer(self.arrow_schema.clone(), self.targe_file_size)?;
        self.row_counts = 0;
        self.input_bytes = 0;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        _ = self.writer.finish();
        let buf = mem::take(self.writer.inner_mut());
        let output_bytes = buf.len();
        self.file_to_write = Some((buf, DataSummary {
            row_counts: self.row_counts,
            input_bytes: self.input_bytes,
            output_bytes,
        }));
        self.reinit_writer()?;
        Ok(())
    }
}

#[async_trait]
impl Processor for ParquetFileWriter {
    fn name(&self) -> String {
        "ParquetFileWriter".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            Ok(Event::Finished)
        } else if self.file_to_write.is_some() {
            self.input.set_not_need_data();
            Ok(Event::Async)
        } else if !self.input_data.is_empty() {
            self.input.set_not_need_data();
            Ok(Event::Sync)
        } else if self.input.is_finished() {
            if self.row_counts > 0 {
                return Ok(Event::Sync);
            }
            if self.unload_output.is_empty() {
                self.output.finish();
                return Ok(Event::Finished);
            }
            if self.unload_output_blocks.is_none() {
                self.unload_output_blocks = Some(self.unload_output.to_block_partial().into());
            }
            if self.output.can_push() {
                if let Some(block) = self.unload_output_blocks.as_mut().unwrap().pop_front() {
                    self.output.push_data(Ok(block));
                    Ok(Event::NeedConsume)
                } else {
                    self.output.finish();
                    Ok(Event::Finished)
                }
            } else {
                Ok(Event::NeedConsume)
            }
        } else if self.input.has_data() {
            let block = self.input.pull_data().unwrap()?;
            if self.targe_file_size.is_none() {
                self.input_data.push(block);
            } else {
                let block_meta = block.get_owned_meta().unwrap();
                let blocks = BlockBatch::downcast_from(block_meta).unwrap();
                self.input_data.extend_from_slice(&blocks.blocks);
            }

            self.input.set_not_need_data();
            Ok(Event::Sync)
        } else {
            self.input.set_need_data();
            Ok(Event::NeedData)
        }
    }

    fn process(&mut self) -> Result<()> {
        while let Some(b) = self.input_data.pop() {
            self.input_bytes += b.memory_size();
            self.row_counts += b.num_rows();
            let batch = b.to_record_batch(&self.table_info.schema)?;
            self.writer.write(&batch)?;

            if let Some(target) = self.targe_file_size {
                if self.row_counts > 0 {
                    // written row groups: compressed, controlled by MAX_ROW_GROUP_SIZE
                    let file_size = self.writer.bytes_written();
                    // in_progress row group: each column leaf has an at most 1MB uncompressed buffer and multi compressed pages
                    // may result in small file for schema with many columns
                    let in_progress = self.writer.in_progress_size();
                    if file_size + in_progress >= target {
                        self.flush()?;
                        return Ok(());
                    }
                }
            }
        }
        if self.input.is_finished() && self.row_counts > 0 {
            self.flush()?;
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        assert!(self.file_to_write.is_some());
        let path = unload_path(
            &self.table_info,
            &self.uuid,
            self.group_id,
            self.batch_id,
            None,
        );
        let (data, summary) = mem::take(&mut self.file_to_write).unwrap();
        self.unload_output.add_file(&path, summary);
        self.data_accessor.write(&path, data).await?;
        self.batch_id += 1;
        Ok(())
    }
}
