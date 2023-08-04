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

use arrow_array::RecordBatch;
use common_base::base::Progress;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use futures::StreamExt;
use opendal::Operator;
use parquet::arrow::arrow_reader::ArrowReaderBuilder;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::arrow::ProjectionMask;

use crate::partition::IcebergPartInfo;

type ParquetStream = ParquetRecordBatchStream<opendal::Reader>;

pub struct IcebergTableSource {
    state: State,
    ctx: Arc<dyn TableContext>,
    dal: Operator,
    _scan_progress: Arc<Progress>,
    output: Arc<OutputPort>,

    /// The projection to be applied on the full schema.
    /// It's used to construct a projected [`ParquetStream`].
    projection: ProjectionMask,
    /// The schema of the data blocks produced by the source [`ParquetStream`].
    /// It is projected by `projection` from the full schema.
    source_schema: DataSchemaRef,
}

enum State {
    /// Read parquet file meta data
    ReadMeta(Option<PartInfoPtr>),

    /// Read data from parquet file.
    ///
    /// `Option<RecordBatch>` means there are data blocks ready for push.
    ReadData(ParquetStream, Option<RecordBatch>),

    Finish,
}

impl IcebergTableSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        output: Arc<OutputPort>,
        source_schema: DataSchemaRef,
        projection: ProjectionMask,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        Ok(ProcessorPtr::create(Box::new(IcebergTableSource {
            ctx,
            dal,
            output,
            _scan_progress: scan_progress,
            state: State::ReadMeta(None),
            source_schema,
            projection,
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
        if matches!(self.state, State::ReadMeta(None)) {
            self.state = self
                .ctx
                .get_partition()
                .map_or(State::Finish, |part_info| State::ReadMeta(Some(part_info)));
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if matches!(self.state, State::ReadData(_, _)) {
            if let State::ReadData(ps, mut data) = std::mem::replace(&mut self.state, State::Finish)
            {
                if let Some(arrow_block) = data.take() {
                    let (mut data_block, _) =
                        DataBlock::from_record_batch(&arrow_block).map_err(|err| {
                            ErrorCode::ReadTableDataError(format!(
                                "Cannot convert arrow record batch to data block: {err:?}"
                            ))
                        })?;
                    // Check if the schema of the data block is matched with the schema of the table.
                    if data_block.num_columns() != self.source_schema.num_fields() {
                        return Err(ErrorCode::TableSchemaMismatch(format!(
                            "Data schema mismatched. Data columns length: {}, schema fields length: {}",
                            data_block.num_columns(),
                            self.source_schema.num_fields()
                        )));
                    }

                    for (col, field) in data_block
                        .columns_mut()
                        .iter_mut()
                        .zip(self.source_schema.fields().iter())
                    {
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

                    self.output.push_data(Ok(data_block));
                }

                // Let's fetch more data.
                self.state = State::ReadData(ps, None);

                return Ok(Event::Async);
            }
        }

        match self.state {
            State::Finish => {
                self.output.finish();
                Ok(Event::Finished)
            }
            State::ReadMeta(_) => Ok(Event::Async),
            State::ReadData(_, _) => Ok(Event::Async),
        }
    }

    fn process(&mut self) -> Result<()> {
        Err(ErrorCode::Internal(
            "It's a bug for IcebergTableSource to go into Event::Sync.",
        ))
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadMeta(Some(part)) => {
                let part = IcebergPartInfo::from_part(&part)?;
                let r = self.dal.reader(&part.path).await?;
                // icelake-io/icelake doesn't support customized parquert reader (projection, batch size, etc.) now,
                // so we use apache/arrow-rs API directly.
                let s = ArrowReaderBuilder::new(r)
                    .await
                    .map_err(parse_parquet_error)?
                    .with_projection(self.projection.clone())
                    .build()
                    .map_err(parse_parquet_error)?;
                self.state = State::ReadData(s, None);
                Ok(())
            }
            State::ReadData(mut stream, None) => match stream.next().await {
                None => {
                    self.state = State::ReadMeta(None);
                    Ok(())
                }
                Some(data) => {
                    let data = data.map_err(parse_parquet_error)?;
                    self.state = State::ReadData(stream, Some(data));
                    Ok(())
                }
            },
            _ => Err(ErrorCode::Internal(
                "It's a bug for IcebergTableSource to async_process current state.",
            )),
        }
    }
}

fn parse_parquet_error(err: parquet::errors::ParquetError) -> ErrorCode {
    ErrorCode::ReadTableDataError(format!("parquet operation failed: {:?}", err))
}
