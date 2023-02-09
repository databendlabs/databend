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
use std::collections::VecDeque;
use std::sync::Arc;

use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::PushDownInfo;
use common_catalog::plan::TopK;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::filter_helper::FilterHelpers;
use common_expression::Column;
use common_expression::ConstantFolder;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::TopKSorter;
use common_expression::Value;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use storages_common_index::Index;
use storages_common_index::RangeIndex;

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
    parts: VecDeque<PartInfoPtr>,
    chunks: VecDeque<DataChunks>,

    prewhere_columns: Vec<usize>,
    remain_columns: Vec<usize>,

    src_schema: DataSchema,
    output_schema: DataSchema,
    prewhere_filter: Arc<Option<Expr>>,

    skipped_page: usize,

    read_columns: Vec<usize>,
    top_k: Option<(TopK, TopKSorter, usize)>,
    topn_finish: bool,
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

        let table_schema = plan.source_info.schema();
        let src_schema: DataSchema = (block_reader.schema().as_ref()).into();

        let top_k = plan
            .push_downs
            .as_ref()
            .map(|p| p.top_k(table_schema.as_ref(), RangeIndex::supported_type))
            .unwrap_or_default();

        let mut prewhere_columns: Vec<usize> =
            match PushDownInfo::prewhere_of_push_downs(&plan.push_downs) {
                None => (0..src_schema.num_fields()).collect(),
                Some(v) => {
                    let projected_schema = v
                        .prewhere_columns
                        .project_schema(plan.source_info.schema().as_ref());

                    projected_schema
                        .fields()
                        .iter()
                        .map(|f| src_schema.index_of(f.name()).unwrap())
                        .collect()
                }
            };

        let top_k = top_k.map(|top_k| {
            let index = src_schema.index_of(top_k.order_by.name()).unwrap();
            let sorter = TopKSorter::new(top_k.limit, top_k.asc);

            if !prewhere_columns.contains(&index) {
                prewhere_columns.push(index);
                prewhere_columns.sort();
            }
            (top_k, sorter, index)
        });

        let remain_columns: Vec<usize> = (0..src_schema.num_fields())
            .filter(|i| !prewhere_columns.contains(i))
            .collect();

        let output_schema: DataSchema = plan.schema().into();

        let func_ctx = ctx.get_function_context()?;
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
                parts: VecDeque::new(),
                chunks: VecDeque::new(),

                prewhere_columns,
                remain_columns,
                src_schema,
                output_schema,
                prewhere_filter,
                skipped_page: 0,
                top_k,
                topn_finish: false,
                read_columns: vec![],
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

    fn add_block(&mut self, data_block: DataBlock) -> Result<()> {
        let rows = data_block.num_rows();
        if rows == 0 {
            return Ok(());
        }
        let progress_values = ProgressValues {
            rows,
            bytes: data_block.memory_size(),
        };
        self.scan_progress.incr(&progress_values);
        self.output_data = Some(data_block);
        Ok(())
    }

    /// check topk should return finished or not
    fn check_topn(&mut self) {
        if let Some((_, sorter, _)) = &mut self.top_k {
            if let Some(next_part) = self.parts.front() {
                let next_part = next_part.as_any().downcast_ref::<FusePartInfo>().unwrap();
                if next_part.sort_min_max.is_none() {
                    return;
                }
                if let Some(sort_min_max) = &next_part.sort_min_max {
                    self.topn_finish = sorter.never_match(sort_min_max);
                }
            }
        }
    }

    fn skip_chunks_page(read_columns: &[usize], chunks: &mut DataChunks) -> Result<()> {
        for (index, chunk) in chunks.iter_mut().enumerate() {
            if read_columns.contains(&index) {
                continue;
            }
            chunk.1.skip_page()?;
        }
        Ok(())
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

        if self.topn_finish {
            self.input.finish();
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;
            if let Some(mut source_meta) = data_block.take_meta() {
                if let Some(source_meta) = source_meta
                    .as_mut_any()
                    .downcast_mut::<NativeDataSourceMeta>()
                {
                    self.parts = VecDeque::from(std::mem::take(&mut source_meta.part));

                    self.check_topn();
                    if self.topn_finish {
                        self.input.finish();
                        self.output.finish();
                        return Ok(Event::Finished);
                    }
                    self.chunks = VecDeque::from(std::mem::take(&mut source_meta.chunks));
                    return Ok(Event::Sync);
                }
            }

            unreachable!();
        }

        if self.input.is_finished() {
            metrics_inc_pruning_prewhere_nums(self.skipped_page as u64);
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(chunks) = self.chunks.front_mut() {
            // this means it's empty projection
            if chunks.is_empty() {
                let _ = self.chunks.pop_front();
                let part = self.parts.pop_front().unwrap();
                let part = FusePartInfo::from_part(&part)?;
                let num_rows = part.nums_rows;
                let data_block = self.block_reader.build_default_values_block(num_rows)?;
                self.add_block(data_block)?;
                return Ok(());
            }

            let mut arrays = Vec::with_capacity(chunks.len());
            self.read_columns.clear();

            // Step 1: Check TOP_K, if prewhere_columns contains not only TOP_K, we can check if TOP_K column can satisfy the heap.
            if self.prewhere_columns.len() > 1 {
                if let Some((top_k, sorter, index)) = self.top_k.as_mut() {
                    let chunk = chunks.get_mut(*index).unwrap();
                    if !chunk.1.has_next() {
                        // No data anymore
                        let _ = self.chunks.pop_front();
                        let _ = self.parts.pop_front().unwrap();
                        self.check_topn();
                        // check finished
                        return Ok(());
                    }
                    let array = chunk.1.next_array()?;
                    self.read_columns.push(*index);

                    let data_type = top_k.order_by.data_type().into();
                    let col = Column::from_arrow(array.as_ref(), &data_type);

                    arrays.push((chunk.0, array));
                    if sorter.never_match_any(&col) {
                        self.skipped_page += 1;
                        return Self::skip_chunks_page(&self.read_columns, chunks);
                    }
                }
            }

            // Step 2: Read Prewhere columns and get the filter
            for index in self.prewhere_columns.iter() {
                if self.read_columns.contains(index) {
                    continue;
                }
                if let Some(chunk) = chunks.get_mut(*index) {
                    if !chunk.1.has_next() {
                        // No data anymore
                        let _ = self.chunks.pop_front();
                        let _ = self.parts.pop_front().unwrap();

                        self.check_topn();
                        return Ok(());
                    }
                    self.read_columns.push(*index);
                    arrays.push((chunk.0, chunk.1.next_array()?));
                }
            }

            let data_block = match self.prewhere_filter.as_ref() {
                Some(filter) => {
                    let prewhere_block = self.block_reader.build_block(arrays.clone())?;
                    let evaluator =
                        Evaluator::new(&prewhere_block, self.func_ctx, &BUILTIN_FUNCTIONS);
                    let result = evaluator
                        .run(filter)
                        .map_err(|e| e.add_message("eval prewhere filter failed:"))?;
                    let filter = FilterHelpers::cast_to_nonull_boolean(&result).unwrap();

                    // Step 3: Apply the filter, if it's all filtered, we can skip the remain columns.
                    if FilterHelpers::is_all_unset(&filter) {
                        self.skipped_page += 1;
                        return Self::skip_chunks_page(&self.read_columns, chunks);
                    }

                    // Step 4: Apply the filter to topk and update the bitmap, this will filter more results
                    let filter = if let Some((_, sorter, index)) = &mut self.top_k {
                        let index_prewhere = self
                            .prewhere_columns
                            .iter()
                            .position(|x| x == index)
                            .unwrap();
                        let top_k_column = prewhere_block
                            .get_by_offset(index_prewhere)
                            .value
                            .as_column()
                            .unwrap();

                        let mut bitmap =
                            FilterHelpers::filter_to_bitmap(filter, prewhere_block.num_rows());
                        sorter.push_column(top_k_column, &mut bitmap);
                        Value::Column(bitmap.into())
                    } else {
                        filter
                    };

                    if FilterHelpers::is_all_unset(&filter) {
                        self.skipped_page += 1;
                        return Self::skip_chunks_page(&self.read_columns, chunks);
                    }

                    for index in self.remain_columns.iter() {
                        let chunk = chunks.get_mut(*index).unwrap();
                        arrays.push((chunk.0, chunk.1.next_array()?));
                    }

                    let block = self.block_reader.build_block(arrays)?;
                    let block = block.resort(&self.src_schema, &self.output_schema)?;
                    block.filter_boolean_value(filter)
                }
                None => {
                    for index in self.remain_columns.iter() {
                        let chunk = chunks.get_mut(*index).unwrap();
                        arrays.push((chunk.0, chunk.1.next_array()?));
                    }
                    self.block_reader.build_block(arrays)
                }
            }?;

            // Step 5: fill missing field default value if need
            let data_block = self
                .block_reader
                .fill_missing_native_column_values(data_block, &self.parts)?;

            // Step 6: Add the block to output data
            self.add_block(data_block)?;
        }

        Ok(())
    }
}
