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

use common_base::base::Progress;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_storages_parquet::ParquetPart;
use common_storages_parquet::ParquetPartData;
use common_storages_parquet::ParquetRSReader;
use common_storages_parquet::ParquetReader;

use crate::partition::IcebergPartInfo;

pub struct IcebergTableSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    _scan_progress: Arc<Progress>,
    output: Arc<OutputPort>,

    /// The reader to read [`DataBlock`]s from parquet files.
    parquet_reader: Arc<ParquetRSReader>,
}

enum State {
    /// Read parquet file meta data
    InitReader(Option<PartInfoPtr>),

    /// Read data from parquet file.
    ReadParquetData(ParquetPartData, PartInfoPtr),

    /// Generate [`DataBlock`], which is the output of this processor.
    GenerateBlock(DataBlock),

    Finish,
}

impl IcebergTableSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        parquet_reader: Arc<ParquetRSReader>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        Ok(ProcessorPtr::create(Box::new(IcebergTableSource {
            ctx,
            output,
            _scan_progress: scan_progress,
            state: State::InitReader(None),
            parquet_reader,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for IcebergTableSource {
    fn name(&self) -> String {
        "IcebergEngineSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::InitReader(None)) {
            self.state = self.ctx.get_partition().map_or(State::Finish, |part_info| {
                State::InitReader(Some(part_info))
            });
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if matches!(self.state, State::GenerateBlock(_)) {
            if let State::GenerateBlock(block) = std::mem::replace(&mut self.state, State::Finish) {
                // Check if the schema of the data block is matched with the schema of the table.
                let block = check_block_schema(&self.parquet_reader.output_schema, block)?;
                self.output.push_data(Ok(block));
                self.state = self.ctx.get_partition().map_or(State::Finish, |part_info| {
                    State::InitReader(Some(part_info))
                });
                return Ok(Event::NeedConsume);
            }
            unreachable!()
        }

        match self.state {
            State::Finish => {
                self.output.finish();
                Ok(Event::Finished)
            }
            State::InitReader(_) => Ok(Event::Async),
            State::ReadParquetData(_, _) => Ok(Event::Sync),
            State::GenerateBlock(_) => unreachable!(),
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::InitReader(Some(part)) => {
                let iceberg_part = IcebergPartInfo::from_part(&part)?;
                match iceberg_part {
                    IcebergPartInfo::Parquet(parquet_part) => {
                        // Currently, we only support parquet file format.
                        let data = self
                            .parquet_reader
                            .readers_from_non_blocking_io(parquet_part)
                            .await?;

                        self.state = State::ReadParquetData(data, part.clone());
                    }
                }

                Ok(())
            }
            _ => Err(ErrorCode::Internal(
                "It's a bug for IcebergTableSource to async_process current state.",
            )),
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadParquetData(parquet_data, part) => match parquet_data {
                ParquetPartData::RowGroup(mut rg) => {
                    let iceberg_part = IcebergPartInfo::from_part(&part)?;
                    if let IcebergPartInfo::Parquet(ParquetPart::RowGroup(parquet_part)) =
                        &iceberg_part
                    {
                        let chunks = self.parquet_reader.read_from_readers(&mut rg)?;
                        // TODO: use `get_deseriliazer` to get `BlockIterator` to generate size-fixed blocks.
                        // Notice that `BlockIterator` will not hold the ownership of raw data,
                        // so if we use `BlockIterator`, we should hold the raw data until the iterator is drained.
                        let block = self
                            .parquet_reader
                            .deserialize(parquet_part, chunks, None)?;
                        self.state = State::GenerateBlock(block);
                    } else {
                        unreachable!()
                    }
                    Ok(())
                }
                ParquetPartData::SmallFiles(_) => Err(ErrorCode::ReadTableDataError(
                    "Do not support read small parqute files now",
                )),
            },
            _ => Err(ErrorCode::Internal(
                "It's a bug for IcebergTableSource to process current state.",
            )),
        }
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
