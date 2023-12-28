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
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Evaluator;
use databend_common_expression::FunctionContext;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use opendal::Operator;

use crate::parquet_rs::copy_into_table::reader::RowGroupReaderForCopy;
use crate::parquet_rs::parquet_reader::policy::ReadPolicyImpl;
use crate::ParquetPart;
use crate::ReadSettings;

type SchemaIndex = usize;

enum State {
    Init,
    ReadRowGroup((SchemaIndex, ReadPolicyImpl)),
    // ReadFiles((SchemaIndex, Vec<(String, Vec<u8>)>)),
}

pub struct ParquetCopySource {
    // Source processor related fields.
    output: Arc<OutputPort>,
    scan_progress: Arc<Progress>,

    // Used for event transforming.
    ctx: Arc<dyn TableContext>,
    generated_data: Option<DataBlock>,
    is_finished: bool,

    // Used to read parquet.
    row_group_readers: Arc<HashMap<usize, RowGroupReaderForCopy>>,
    operator: Operator,
    schema: DataSchemaRef,
    func_ctx: FunctionContext,

    state: State,
    batch_size: usize,
}

impl ParquetCopySource {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        row_group_readers: Arc<HashMap<usize, RowGroupReaderForCopy>>,
        operator: Operator,
        schema: DataSchemaRef,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        let batch_size = ctx.get_settings().get_parquet_max_block_size()? as usize;
        let func_ctx = ctx.get_function_context()?;

        Ok(ProcessorPtr::create(Box::new(Self {
            output,
            scan_progress,
            ctx,
            operator,
            row_group_readers,
            func_ctx,
            batch_size,
            generated_data: None,
            is_finished: false,
            state: State::Init,
            schema,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for ParquetCopySource {
    fn name(&self) -> String {
        "ParquetCopySource".to_string()
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
            None => match &self.state {
                State::Init => Ok(Event::Async),
                State::ReadRowGroup(_) => Ok(Event::Sync),
            },
            Some(data_block) => {
                let progress_values = ProgressValues {
                    rows: data_block.num_rows(),
                    bytes: data_block.memory_size(),
                };
                self.scan_progress.incr(&progress_values);
                self.output.push_data(Ok(data_block));
                Ok(Event::NeedConsume)
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Init) {
            State::ReadRowGroup((schema_index, mut reader)) => {
                if let Some(block) = reader.as_mut().read_block()? {
                    let projection = self
                        .row_group_readers
                        .get(&schema_index)
                        .unwrap()
                        .output_projection();
                    let evaluator = Evaluator::new(&block, &self.func_ctx, &BUILTIN_FUNCTIONS);
                    let mut columns = Vec::with_capacity(projection.len());
                    for (field, expr) in self.schema.fields().iter().zip(projection.iter()) {
                        let value = evaluator.run(expr)?;
                        let column = BlockEntry::new(field.data_type().clone(), value);
                        columns.push(column);
                    }
                    let block = DataBlock::new(columns, block.num_rows());
                    self.generated_data = Some(block);
                    self.state = State::ReadRowGroup((schema_index, reader));
                }
                // Else: The reader is finished. We should try to build another reader.
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Init) {
            State::Init => {
                if let Some(part) = self.ctx.get_partition() {
                    match ParquetPart::from_part(&part)? {
                        ParquetPart::ParquetRSRowGroup(part) => {
                            let schema_index = part.schema_index;
                            let builder = self
                                .row_group_readers
                                .get(&schema_index)
                                .expect("schema index must exist");
                            let reader = builder
                                .build_reader(
                                    part,
                                    self.operator.clone(),
                                    &ReadSettings::from_ctx(&self.ctx)?,
                                    self.batch_size,
                                )
                                .await?
                                .expect("reader must exist");
                            {
                                self.state = State::ReadRowGroup((schema_index, reader));
                            }
                            // Else: keep in init state.
                        }
                        _ => unreachable!(),
                    }
                } else {
                    self.is_finished = true;
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}
