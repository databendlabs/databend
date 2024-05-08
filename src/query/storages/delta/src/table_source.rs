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

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_expression::Value;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_storages_parquet::ParquetFileReader;
use databend_common_storages_parquet::ParquetPart;
use databend_common_storages_parquet::ParquetRSFullReader;
use parquet::arrow::async_reader::ParquetRecordBatchStream;

use crate::partition::DeltaPartInfo;

pub type PartitionColumnIndex = usize;

pub struct DeltaTableSource {
    output: Arc<OutputPort>,
    generated_data: Option<DataBlock>,
    is_finished: bool,

    scan_progress: Arc<Progress>,
    // Used for get partition
    ctx: Arc<dyn TableContext>,

    // Used to read parquet file.
    parquet_reader: Arc<ParquetRSFullReader>,

    // Used to insert partition_block_entries to data block
    // FieldIndex is the index in the output_schema
    // PartitionColumnIndex is the index of in partition_fields and partition_block_entries
    // order by FieldIndex so we can insert in order
    output_partition_columns: Vec<(FieldIndex, PartitionColumnIndex)>,
    partition_fields: Vec<TableField>,
    // Used to check schema
    output_schema: DataSchemaRef,

    // Per partition
    stream: Option<ParquetRecordBatchStream<ParquetFileReader>>,
    partition_block_entries: Vec<BlockEntry>,
}

impl DeltaTableSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        output_schema: DataSchemaRef,
        parquet_reader: Arc<ParquetRSFullReader>,
        partition_fields: Vec<TableField>,
    ) -> Result<ProcessorPtr> {
        let output_partition_columns = output_schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(fi, f)| {
                partition_fields
                    .iter()
                    .position(|p| p.name() == f.name())
                    .map(|pi| (fi, pi))
            })
            .collect();
        let scan_progress = ctx.get_scan_progress();
        Ok(ProcessorPtr::create(Box::new(DeltaTableSource {
            output,
            scan_progress,
            ctx,
            parquet_reader,
            output_schema,
            partition_fields,
            output_partition_columns,
            stream: None,
            generated_data: None,
            is_finished: false,
            partition_block_entries: vec![],
        })))
    }
}

#[async_trait::async_trait]
impl Processor for DeltaTableSource {
    fn name(&self) -> String {
        "DeltaSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.is_finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        match self.generated_data.take() {
            None => Ok(Event::Async),
            Some(data_block) => {
                let progress_values = ProgressValues {
                    rows: data_block.num_rows(),
                    bytes: data_block.memory_size(),
                };
                self.scan_progress.incr(&progress_values);
                Profile::record_usize_profile(
                    ProfileStatisticsName::ScanBytes,
                    data_block.memory_size(),
                );
                self.output.push_data(Ok(data_block));
                Ok(Event::NeedConsume)
            }
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let Some(mut stream) = self.stream.take() {
            if let Some(block) = self
                .parquet_reader
                .read_block_from_stream(&mut stream)
                .await?
                .map(|b| {
                    let mut columns = b.columns().to_vec();
                    for (fi, pi) in self.output_partition_columns.iter() {
                        columns.insert(*fi, self.partition_block_entries[*pi].clone());
                    }
                    DataBlock::new(columns, b.num_rows())
                })
                .map(|b| check_block_schema(&self.output_schema, b))
                .transpose()?
            {
                self.generated_data = Some(block);
                self.stream = Some(stream);
            }
            // else:
            // If `read_block` returns `None`, it means the stream is finished.
            // And we should try to build another stream (in next event loop).
        } else if let Some(part) = self.ctx.get_partition() {
            let part = DeltaPartInfo::from_part(&part)?;
            match &part.data {
                ParquetPart::ParquetFiles(files) => {
                    assert_eq!(files.files.len(), 1);
                    let partition_fields = self
                        .partition_fields
                        .iter()
                        .cloned()
                        .zip(part.partition_values.iter().cloned())
                        .collect::<Vec<_>>();
                    self.partition_block_entries = partition_fields
                        .iter()
                        .map(|(f, v)| {
                            BlockEntry::new(f.data_type().into(), Value::Scalar(v.clone()))
                        })
                        .collect::<Vec<_>>();
                    let stream = self
                        .parquet_reader
                        .prepare_data_stream(&files.files[0].0, Some(&partition_fields))
                        .await?;
                    self.stream = Some(stream);
                }
                _ => unreachable!(),
            }
        } else {
            self.is_finished = true;
        }

        Ok(())
    }
}

fn check_block_schema(schema: &DataSchema, mut block: DataBlock) -> Result<DataBlock> {
    // Check if the schema of the data block is matched with the schema of the table.
    if block.num_columns() != schema.num_fields() {
        return Err(ErrorCode::TableSchemaMismatch(format!(
            "Data schema mismatched. Data columns length: {}, schema fields length: {}",
            block.num_columns(),
            schema.num_fields()
        )));
    }

    for (col, field) in block.columns_mut().iter_mut().zip(schema.fields().iter()) {
        // If the actual data is nullable, the field must be nullbale.
        if col.data_type.is_nullable_or_null() && !field.is_nullable() {
            return Err(ErrorCode::TableSchemaMismatch(format!(
                "Data schema mismatched (col name: {}). Data column is nullable, but schema field is not nullable",
                field.name()
            )));
        }
        // The inner type of the data and field should be the same.
        let data_type = col.data_type.remove_nullable();
        let schema_type = field.data_type().remove_nullable();
        if data_type != schema_type {
            return Err(ErrorCode::TableSchemaMismatch(format!(
                "Data schema mismatched (col name: {}). Data column type is {:?}, but schema field type is {:?}",
                field.name(),
                col.data_type,
                field.data_type()
            )));
        }
        // If the field is nullable but the actual data is not nullable,
        // we should wrap nullable for the data.
        if field.is_nullable() && !col.data_type.is_nullable_or_null() {
            col.data_type = col.data_type.wrap_nullable();
            col.value = col.value.clone().wrap_nullable(None);
        }
    }

    Ok(block)
}
