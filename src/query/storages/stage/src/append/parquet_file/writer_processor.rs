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

use arrow_schema::DataType;
use arrow_schema::Schema;
use async_trait::async_trait;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::principal::StageFileCompression;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_storages_common_stage::CopyIntoLocationInfo;
use opendal::Operator;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::basic::Encoding;
use parquet::basic::ZstdLevel;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;
use parquet::file::properties::WriterVersion;
use parquet::schema::types::ColumnPath;

use super::block_batch::BlockBatch;
use crate::append::UnloadOutput;
use crate::append::output::DataSummary;
use crate::append::partition::partition_from_block;
use crate::append::path::unload_path;

pub struct ParquetFileWriter {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    info: CopyIntoLocationInfo,
    schema: TableSchemaRef,
    arrow_schema: Arc<Schema>,
    compression: Compression,
    create_by: String,

    input_data: VecDeque<DataBlock>,

    input_bytes: usize,
    row_counts: usize,
    writer: ArrowWriter<Vec<u8>>,

    file_to_write: Option<(Vec<u8>, DataSummary, Option<Arc<str>>)>,
    data_accessor: Operator,

    // the result of statement
    unload_output: UnloadOutput,
    unload_output_blocks: Option<VecDeque<DataBlock>>,

    query_id: String,
    group_id: usize,
    batch_id: usize,

    targe_file_size: Option<usize>,
    current_partition: Option<Option<Arc<str>>>,
}

const MAX_BUFFER_SIZE: usize = 64 * 1024 * 1024;
// this is number of rows, not size
const MAX_ROW_GROUP_SIZE: usize = 1024 * 1024;

fn create_writer(
    arrow_schema: Arc<Schema>,
    targe_file_size: Option<usize>,
    compression: Compression,
    create_by: String,
) -> Result<ArrowWriter<Vec<u8>>> {
    let mut builder = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(compression)
        .set_created_by(create_by)
        .set_max_row_group_size(MAX_ROW_GROUP_SIZE)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_dictionary_enabled(true)
        .set_bloom_filter_enabled(false);

    // Set the encoding of the decimal column to `PLAIN` to avoid
    // compatibility issues caused by encoding as `Delta_Byte_Array`.
    for field in arrow_schema.fields() {
        if matches!(
            field.data_type(),
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
        ) {
            let column = ColumnPath::from(field.name().clone());
            builder = builder.set_column_encoding(column, Encoding::PLAIN);
        }
    }

    let props = builder.build();
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
        info: CopyIntoLocationInfo,
        schema: TableSchemaRef,
        data_accessor: Operator,
        query_id: String,
        group_id: usize,
        targe_file_size: Option<usize>,
        create_by: String,
    ) -> Result<ProcessorPtr> {
        let unload_output = UnloadOutput::create(info.options.detailed_output);

        let arrow_schema = Arc::new(Schema::from(schema.as_ref()));
        let compression = info.stage.file_format_params.compression();
        let compression = match &compression {
            StageFileCompression::Zstd => Compression::ZSTD(ZstdLevel::default()),
            StageFileCompression::Snappy => Compression::SNAPPY,
            StageFileCompression::None => Compression::UNCOMPRESSED,
            _ => {
                return Err(ErrorCode::Internal(format!(
                    "unexpected compression {compression}"
                )));
            }
        };
        let writer = create_writer(
            arrow_schema.clone(),
            targe_file_size,
            compression,
            create_by.clone(),
        )?;

        Ok(ProcessorPtr::create(Box::new(ParquetFileWriter {
            input,
            output,
            schema,
            info,
            arrow_schema,
            compression,
            create_by,
            unload_output,
            unload_output_blocks: None,
            writer,
            input_data: VecDeque::new(),
            input_bytes: 0,
            file_to_write: None,
            data_accessor,
            query_id,
            group_id,
            batch_id: 0,
            targe_file_size,
            row_counts: 0,
            current_partition: None,
        })))
    }
    pub fn reinit_writer(&mut self) -> Result<()> {
        self.writer = create_writer(
            self.arrow_schema.clone(),
            self.targe_file_size,
            self.compression,
            self.create_by.clone(),
        )?;
        self.row_counts = 0;
        self.input_bytes = 0;
        Ok(())
    }

    fn flush_stream_writer(&mut self) -> Result<()> {
        self.writer.finish().ok();
        let buf = mem::take(self.writer.inner_mut());
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
        self.row_counts = 0;
        self.input_bytes = 0;
        self.current_partition = None;
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
                    self.flush_stream_writer()?;
                    self.input_data.push_front(block);
                    return Ok(());
                }
                self.current_partition = Some(partition.clone());
            }

            self.input_bytes += block.memory_size();
            self.row_counts += block.num_rows();
            let batch = block.to_record_batch(&self.schema)?;
            self.writer.write(&batch)?;

            if let Some(target) = self.targe_file_size {
                if self.row_counts > 0 {
                    let file_size = self.writer.bytes_written();
                    let in_progress = self.writer.in_progress_size();
                    if file_size + in_progress >= target {
                        self.flush_stream_writer()?;
                        return Ok(());
                    }
                }
            }
        }

        if self.input.is_finished() && self.row_counts > 0 {
            self.flush_stream_writer()?;
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
