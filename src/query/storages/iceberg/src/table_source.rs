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

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_storages_parquet::ParquetPart;
use common_storages_parquet::ParquetRSReader;
use opendal::Reader;
use parquet::arrow::async_reader::ParquetRecordBatchStream;

use crate::partition::IcebergPartInfo;

pub struct IcebergTableSource {
    // Used for event transforming.
    ctx: Arc<dyn TableContext>,
    output: Arc<OutputPort>,
    generated_data: Option<DataBlock>,
    is_finished: bool,

    // Used to read parquet.
    output_schema: DataSchemaRef,
    parquet_reader: Arc<ParquetRSReader>,
    stream: Option<ParquetRecordBatchStream<Reader>>,
}

impl IcebergTableSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        output_schema: DataSchemaRef,
        parquet_reader: Arc<ParquetRSReader>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(IcebergTableSource {
            ctx,
            output,
            parquet_reader,
            output_schema,
            stream: None,
            generated_data: None,
            is_finished: false,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for IcebergTableSource {
    fn name(&self) -> String {
        "IcebergSource".to_string()
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
                self.output.push_data(Ok(data_block));
                Ok(Event::NeedConsume)
            }
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let Some(mut stream) = self.stream.take() {
            self.generated_data = self
                .parquet_reader
                .read_block(&mut stream)
                .await?
                .map(|b| check_block_schema(&self.output_schema, b))
                .transpose()?;
            self.stream = Some(stream);
        } else if let Some(part) = self.ctx.get_partition() {
            match IcebergPartInfo::from_part(&part)? {
                IcebergPartInfo::Parquet(ParquetPart::ParquetRSFile(file)) => {
                    let stream = self
                        .parquet_reader
                        .prepare_data_stream(self.ctx.clone(), &file.location)
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
