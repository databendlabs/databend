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
use std::time::Instant;

use arrow_array::RecordBatch;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_storage::CopyStatus;
use databend_common_storage::FileStatus;
use orc_rust::array_decoder::NaiveStripeDecoder;

use crate::copy_into_table::projection::ProjectionFactory;
use crate::hashable_schema::HashableSchema;
use crate::strip::StripeInMemory;
use crate::utils::map_orc_error;

pub struct StripeDecoderForCopy {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    input_data: Option<DataBlock>,
    output_data: VecDeque<DataBlock>,

    projections: Arc<ProjectionFactory>,
    copy_status: Option<Arc<CopyStatus>>,
    output_schema: DataSchemaRef,
    func_ctx: FunctionContext,

    stripe: Option<Stripe>,
}

struct Stripe {
    pub iter: Box<dyn Iterator<Item = Result<RecordBatch>> + Send>,
    pub path: String,
    pub schema: HashableSchema,
    pub projection: Vec<Expr>,
}

impl StripeDecoderForCopy {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table_ctx: Arc<dyn TableContext>,
        projections: Arc<ProjectionFactory>,
        output_schema: DataSchemaRef,
    ) -> Result<Self> {
        let copy_status = if matches!(table_ctx.get_query_kind(), QueryKind::CopyIntoTable) {
            Some(table_ctx.get_copy_status())
        } else {
            None
        };
        let func_ctx = table_ctx.get_function_context()?;
        Ok(StripeDecoderForCopy {
            input,
            output,
            input_data: None,
            output_schema,
            copy_status,
            projections,
            func_ctx,
            stripe: None,
            output_data: Default::default(),
        })
    }

    fn project(&self, block: DataBlock, projection: &[Expr]) -> Result<DataBlock> {
        let evaluator = Evaluator::new(&block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let mut columns = Vec::with_capacity(projection.len());
        for (field, expr) in self.output_schema.fields().iter().zip(projection.iter()) {
            let value = evaluator.run(expr)?;
            let column = BlockEntry::new(field.data_type().clone(), value);
            columns.push(column);
        }
        Ok(DataBlock::new(columns, block.num_rows()))
    }
}

impl Processor for StripeDecoderForCopy {
    fn name(&self) -> String {
        "StripeDecoderForCopy".to_string()
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

        if let Some(data_block) = self.output_data.pop_front() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.stripe.is_some() || self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if self.stripe.is_none() {
            let b = mem::take(&mut self.input_data);
            if let Some(block) = b {
                let stripe = block
                    .get_owned_meta()
                    .and_then(StripeInMemory::downcast_from)
                    .unwrap();
                let schema = stripe.schema.expect("schema not none");

                let projection = self.projections.get(&schema, &stripe.path)?;
                let decoder =
                    NaiveStripeDecoder::new(stripe.stripe, schema.arrow_schema.clone(), 100000)
                        .map_err(|e| map_orc_error(e, &stripe.path))?;
                let path = Arc::new(stripe.path.clone());
                self.stripe = Some(Stripe {
                    iter: Box::new(decoder.into_iter().map(move |r| {
                        let path = path.clone();
                        r.map_err(|e| map_orc_error(e, &path))
                    })),
                    path: stripe.path,
                    schema,
                    projection,
                });
            }
        }
        let stripe = mem::take(&mut self.stripe);
        if let Some(mut stripe) = stripe {
            if let Some(batch) = stripe.iter.next() {
                let start = Instant::now();
                let (block, _) =
                    DataBlock::from_record_batch(stripe.schema.data_schema.as_ref(), &batch?)?;
                let block = self.project(block, &stripe.projection)?;
                if let Some(copy_status) = &self.copy_status {
                    copy_status.add_chunk(&stripe.path, FileStatus {
                        num_rows_loaded: block.num_rows(),
                        error: None,
                    })
                }
                log::info!(
                    "decode orc block of {} rows use {} secs",
                    block.num_rows(),
                    start.elapsed().as_secs_f32()
                );
                self.output_data.push_back(block);
                self.stripe = Some(stripe)
            } else {
                self.stripe = None
            }
        }
        Ok(())
    }
}
