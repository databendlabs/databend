//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::any::Any;
use std::sync::Arc;

use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ConstantFolder;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Value;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;

use crate::fuse_part::FusePartInfo;
use crate::io::BlockReader;
use crate::metrics::metrics_inc_pruning_prewhere_nums;
use crate::operations::read::native_data_source::DataChunks;
use crate::operations::read::native_data_source::NativeDataSourceMeta;

pub struct NativeDeserializeDataTransform {
    func_ctx: FunctionContext,
    scan_progress: Arc<Progress>,
    block_reader: Arc<BlockReader>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    parts: Vec<PartInfoPtr>,
    chunks: Vec<DataChunks>,

    prewhere_columns: Vec<usize>,
    remain_columns: Vec<usize>,

    src_schema: DataSchema,
    output_schema: DataSchema,
    prewhere_filter: Arc<Option<Expr>>,

    prewhere_skipped: usize,
}

impl NativeDeserializeDataTransform {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        block_reader: Arc<BlockReader>,
        plan: &DataSourcePlan,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        let src_schema: DataSchema = (block_reader.schema().as_ref()).into();

        let prewhere_columns: Vec<usize> =
            match PushDownInfo::prewhere_of_push_downs(&plan.push_downs) {
                None => (0..src_schema.num_fields()).collect(),
                Some(v) => {
                    let projected_arrow_schema = v
                        .prewhere_columns
                        .project_schema(plan.source_info.schema().as_ref());
                    projected_arrow_schema
                        .fields()
                        .iter()
                        .map(|f| src_schema.index_of(f.name()).unwrap())
                        .collect()
                }
            };

        let remain_columns: Vec<usize> = (0..src_schema.num_fields())
            .filter(|i| !prewhere_columns.contains(i))
            .collect();

        let output_schema: DataSchema = match PushDownInfo::prewhere_of_push_downs(&plan.push_downs)
        {
            None => src_schema.clone(),
            Some(v) => {
                let projected = v
                    .output_columns
                    .project_schema(plan.source_info.schema().as_ref());
                (&projected).into()
            }
        };

        let func_ctx = ctx.try_get_function_context()?;
        let prewhere_schema = src_schema.project(&prewhere_columns);
        let prewhere_filter = Self::build_prewhere_filter_expr(plan, func_ctx, &prewhere_schema)?;

        Ok(ProcessorPtr::create(Box::new(
            NativeDeserializeDataTransform {
                func_ctx,
                scan_progress,
                block_reader,
                input,
                output,
                output_data: None,
                parts: vec![],
                chunks: vec![],

                prewhere_columns,
                remain_columns,
                src_schema,
                output_schema,
                prewhere_filter,
                prewhere_skipped: 0,
            },
        )))
    }

    fn build_prewhere_filter_expr(
        plan: &DataSourcePlan,
        ctx: FunctionContext,
        schema: &DataSchema,
    ) -> Result<Arc<Option<Expr>>> {
        Ok(
            match PushDownInfo::prewhere_of_push_downs(&plan.push_downs) {
                None => Arc::new(None),
                Some(v) => {
                    let expr = v.filter.as_expr(&BUILTIN_FUNCTIONS);
                    let expr =
                        expr.project_column_ref(|name| schema.column_with_name(name).unwrap().0);
                    let (expr, _) = ConstantFolder::fold(&expr, ctx, &BUILTIN_FUNCTIONS);
                    Arc::new(Some(expr))
                }
            },
        )
    }
}

impl Processor for NativeDeserializeDataTransform {
    fn name(&self) -> String {
        String::from("NativeDeserializeDataTransform")
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
            if let Some(mut source_meta) = data_block.take_meta() {
                if let Some(source_meta) = source_meta
                    .as_mut_any()
                    .downcast_mut::<NativeDataSourceMeta>()
                {
                    self.parts = source_meta.part.clone();
                    self.chunks = std::mem::take(&mut source_meta.chunks);
                    return Ok(Event::Sync);
                }
            }

            unreachable!();
        }

        if self.input.is_finished() {
            metrics_inc_pruning_prewhere_nums(self.prewhere_skipped as u64);
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(chunks) = self.chunks.last_mut() {
            // this means it's empty projection
            if chunks.is_empty() {
                let _ = self.chunks.pop();
                let part = self.parts.pop().unwrap();
                let part = FusePartInfo::from_part(&part)?;
                let data_block = DataBlock::new(vec![], part.nums_rows);
                let progress_values = ProgressValues {
                    rows: data_block.num_rows(),
                    bytes: data_block.memory_size(),
                };
                self.scan_progress.incr(&progress_values);
                self.output_data = Some(data_block);
                return Ok(());
            }

            let mut arrays = Vec::with_capacity(chunks.len());
            for index in self.prewhere_columns.iter() {
                let chunk = chunks.get_mut(*index).unwrap();
                if !chunk.1.has_next() {
                    // No data anymore
                    let _ = self.chunks.pop();
                    return Ok(());
                }
                arrays.push((chunk.0, chunk.1.next_array()?));
            }

            let data_block = match self.prewhere_filter.as_ref() {
                Some(filter) => {
                    let prewhere_block = self.block_reader.build_block(arrays.clone())?;
                    let evaluator =
                        Evaluator::new(&prewhere_block, self.func_ctx, &BUILTIN_FUNCTIONS);
                    let result = evaluator.run(filter).map_err(|(_, e)| {
                        ErrorCode::Internal(format!("eval prewhere filter failed: {}.", e))
                    })?;
                    let filter = DataBlock::cast_to_nonull_boolean(&result).unwrap();

                    let all_filtered = match &filter {
                        Value::Scalar(v) => !v,
                        Value::Column(bitmap) => bitmap.unset_bits() == bitmap.len(),
                    };

                    if all_filtered {
                        self.prewhere_skipped += 1;
                        for index in self.remain_columns.iter() {
                            let chunk = chunks.get_mut(*index).unwrap();
                            chunk.1.skip_page()?;
                        }
                        return Ok(());
                    }

                    for index in self.remain_columns.iter() {
                        let chunk = chunks.get_mut(*index).unwrap();
                        arrays.push((chunk.0, chunk.1.next_array()?));
                    }

                    let block = self.block_reader.build_block(arrays)?;
                    let block = block.resort(&self.src_schema, &self.output_schema)?;
                    block.filter(&result)
                }
                None => {
                    for index in self.remain_columns.iter() {
                        let chunk = chunks.get_mut(*index).unwrap();
                        arrays.push((chunk.0, chunk.1.next_array()?));
                    }
                    self.block_reader.build_block(arrays)
                }
            }?;

            let progress_values = ProgressValues {
                rows: data_block.num_rows(),
                bytes: data_block.memory_size(),
            };
            self.scan_progress.incr(&progress_values);
            self.output_data = Some(data_block);
        }

        Ok(())
    }
}
