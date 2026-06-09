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

use arrow_ipc::writer::FileWriter;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;
use async_trait::async_trait;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_storages_common_stage::CopyIntoLocationInfo;
use opendal::Operator;

use crate::append::UnloadOutput;
use crate::append::column_based::block_batch::BlockBatch;
use crate::append::output::DataSummary;
use crate::append::partition::partition_from_block;
use crate::append::path::unload_path;
use crate::read::arrow::ArrowIpcMode;

enum ArrowIpcWriter {
    File(FileWriter<Vec<u8>>),
    Stream(StreamWriter<Vec<u8>>),
}

impl ArrowIpcWriter {
    fn create(mode: ArrowIpcMode, arrow_schema: &Schema) -> Result<Self> {
        match mode {
            ArrowIpcMode::File => Ok(Self::File(FileWriter::try_new(vec![], arrow_schema)?)),
            ArrowIpcMode::Stream => Ok(Self::Stream(StreamWriter::try_new(vec![], arrow_schema)?)),
        }
    }

    fn write(&mut self, batch: &arrow_array::RecordBatch) -> Result<()> {
        match self {
            Self::File(writer) => writer.write(batch)?,
            Self::Stream(writer) => writer.write(batch)?,
        }
        Ok(())
    }

    fn bytes_written(&self) -> usize {
        match self {
            Self::File(writer) => writer.get_ref().len(),
            Self::Stream(writer) => writer.get_ref().len(),
        }
    }

    fn finish(self) -> Result<Vec<u8>> {
        match self {
            Self::File(writer) => Ok(writer.into_inner()?),
            Self::Stream(writer) => Ok(writer.into_inner()?),
        }
    }
}

pub struct ArrowFileWriter {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    info: CopyIntoLocationInfo,
    schema: TableSchemaRef,
    arrow_schema: Arc<Schema>,
    mode: ArrowIpcMode,

    input_data: VecDeque<DataBlock>,

    input_bytes: usize,
    row_counts: usize,
    writer: Option<ArrowIpcWriter>,

    file_to_write: Option<(Vec<u8>, DataSummary, Option<Arc<str>>)>,
    data_accessor: Operator,

    unload_output: UnloadOutput,
    unload_output_blocks: Option<VecDeque<DataBlock>>,

    query_id: String,
    group_id: usize,
    batch_id: usize,

    target_file_size: Option<usize>,
    current_partition: Option<Option<Arc<str>>>,
}

impl ArrowFileWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        info: CopyIntoLocationInfo,
        schema: TableSchemaRef,
        data_accessor: Operator,
        query_id: String,
        group_id: usize,
        target_file_size: Option<usize>,
        mode: ArrowIpcMode,
    ) -> Result<ProcessorPtr> {
        let unload_output = UnloadOutput::create(info.options.detailed_output);
        let arrow_schema = Arc::new(Schema::from(schema.as_ref()));
        let writer = ArrowIpcWriter::create(mode, &arrow_schema)?;

        Ok(ProcessorPtr::create(Box::new(ArrowFileWriter {
            input,
            output,
            schema,
            arrow_schema,
            mode,
            info,
            unload_output,
            unload_output_blocks: None,
            writer: Some(writer),
            input_data: VecDeque::new(),
            input_bytes: 0,
            file_to_write: None,
            data_accessor,
            query_id,
            group_id,
            batch_id: 0,
            target_file_size,
            row_counts: 0,
            current_partition: None,
        })))
    }

    fn reinit_writer(&mut self) -> Result<()> {
        self.writer = Some(ArrowIpcWriter::create(self.mode, &self.arrow_schema)?);
        self.row_counts = 0;
        self.input_bytes = 0;
        Ok(())
    }

    fn flush_writer(&mut self) -> Result<()> {
        let writer = self.writer.take().unwrap();
        let buf = writer.finish()?;
        let output_bytes = buf.len();
        self.file_to_write = Some((
            buf,
            DataSummary {
                row_counts: self.row_counts,
                input_bytes: self.input_bytes,
                output_bytes,
            },
            self.current_partition.clone().flatten(),
        ));
        self.reinit_writer()?;
        self.current_partition = None;
        Ok(())
    }
}

#[async_trait]
impl Processor for ArrowFileWriter {
    fn name(&self) -> String {
        "ArrowFileWriter".to_string()
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
            if self.target_file_size.is_none() {
                self.input_data.push_back(block);
            } else if block.get_meta().is_some() {
                let block_meta = block.get_owned_meta().unwrap();
                let block_batch = BlockBatch::downcast_from(block_meta).unwrap();
                for b in block_batch.blocks {
                    self.input_data.push_back(b);
                }
            } else {
                self.input_data.push_back(block);
            }

            self.input.set_not_need_data();
            Ok(Event::Sync)
        } else {
            self.input.set_need_data();
            Ok(Event::NeedData)
        }
    }

    fn process(&mut self) -> Result<()> {
        while let Some(block) = self.input_data.pop_front() {
            let partition = partition_from_block(&block);
            if self.current_partition.as_ref() != Some(&partition) {
                if self.row_counts > 0 {
                    self.flush_writer()?;
                    self.input_data.push_front(block);
                    return Ok(());
                }
                self.current_partition = Some(partition.clone());
            }

            self.input_bytes += block.memory_size();
            self.row_counts += block.num_rows();
            let batch = block.to_record_batch(&self.schema)?;
            let writer = self.writer.as_mut().unwrap();
            writer.write(&batch)?;

            if let Some(target) = self.target_file_size {
                if self.row_counts > 0 && writer.bytes_written() >= target {
                    self.flush_writer()?;
                    return Ok(());
                }
            }
        }

        if self.input.is_finished() && self.row_counts > 0 {
            self.flush_writer()?;
            return Ok(());
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        assert!(self.file_to_write.is_some());
        let (data, summary, partition) = mem::take(&mut self.file_to_write).unwrap();
        let path = unload_path(
            &self.info,
            &self.query_id,
            self.group_id,
            self.batch_id,
            None,
            partition.as_deref(),
        );
        self.unload_output.add_file(&path, summary);
        self.data_accessor.write(&path, data).await?;
        self.batch_id += 1;
        Ok(())
    }
}
