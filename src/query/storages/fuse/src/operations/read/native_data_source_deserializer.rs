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
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::native::read::column_iter_to_arrays;
use common_arrow::native::read::reader::NativeReader;
use common_arrow::native::read::ArrayIter;
use common_arrow::parquet::metadata::ColumnDescriptor;
use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::PushDownInfo;
use common_catalog::plan::TopK;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::filter_helper::FilterHelpers;
use common_expression::types::BooleanType;
use common_expression::BlockEntry;
use common_expression::BlockMetaInfoDowncast;
use common_expression::BlockMetaInfoPtr;
use common_expression::Column;
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
use common_storage::ColumnNode;

use crate::fuse_part::FusePartInfo;
use crate::io::BlockReader;
use crate::io::NativeReaderExt;
use crate::metrics::metrics_inc_pruning_prewhere_nums;
use crate::operations::read::native_data_source::DataChunks;
use crate::operations::read::native_data_source::NativeDataSourceMeta;

pub struct NativeDeserializeDataTransform {
    func_ctx: FunctionContext,
    scan_progress: Arc<Progress>,
    block_reader: Arc<BlockReader>,
    column_leaves: Vec<Vec<ColumnDescriptor>>,

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
    // Identifies whether the ArrayIter has been initialised.
    inited: bool,
    // The ArrayIter of each columns to read Pages in order.
    array_iters: BTreeMap<usize, ArrayIter<'static>>,
    // The Page numbers of each ArrayIter can skip.
    array_skip_pages: BTreeMap<usize, usize>,
}

impl NativeDeserializeDataTransform {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        block_reader: Arc<BlockReader>,
        plan: &DataSourcePlan,
        top_k: Option<TopK>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();

        let src_schema: DataSchema = (block_reader.schema().as_ref()).into();
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
        let prewhere_filter = Self::build_prewhere_filter_expr(plan, &prewhere_schema)?;

        let mut column_leaves = Vec::with_capacity(block_reader.project_column_nodes.len());
        for column_node in &block_reader.project_column_nodes {
            let leaves: Vec<ColumnDescriptor> = column_node
                .leaf_indices
                .iter()
                .map(|i| block_reader.parquet_schema_descriptor.columns()[*i].clone())
                .collect::<Vec<_>>();
            column_leaves.push(leaves);
        }

        Ok(ProcessorPtr::create(Box::new(
            NativeDeserializeDataTransform {
                func_ctx,
                scan_progress,
                block_reader,
                column_leaves,
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
                read_columns: vec![],
                inited: false,
                array_iters: BTreeMap::new(),
                array_skip_pages: BTreeMap::new(),
            },
        )))
    }

    fn build_prewhere_filter_expr(
        plan: &DataSourcePlan,
        schema: &DataSchema,
    ) -> Result<Arc<Option<Expr>>> {
        Ok(
            match PushDownInfo::prewhere_of_push_downs(&plan.push_downs) {
                None => Arc::new(None),
                Some(v) => {
                    let expr = v
                        .filter
                        .as_expr(&BUILTIN_FUNCTIONS)
                        .project_column_ref(|name| schema.column_with_name(name).unwrap().0);
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

    /// If the top-k or all prewhere columns are default values, check if the filter is met,
    /// and if not, ignore all pages, otherwise continue without repeating the check for subsequent processes.
    fn check_default_values(&mut self) -> Result<bool> {
        if self.prewhere_columns.len() > 1 {
            if let Some((_, sorter, index)) = self.top_k.as_mut() {
                if !self.array_iters.contains_key(index) {
                    let default_val = self.block_reader.default_vals[*index].clone();
                    if sorter.never_match_value(&default_val) {
                        return Ok(true);
                    }
                }
            }
        }
        if let Some(filter) = self.prewhere_filter.as_ref() {
            let all_defaults = &self
                .prewhere_columns
                .iter()
                .all(|index| !self.array_iters.contains_key(index));

            if *all_defaults {
                let columns = &self
                    .prewhere_columns
                    .iter()
                    .map(|index| {
                        let data_type = self.src_schema.field(*index).data_type().clone();
                        let default_val = &self.block_reader.default_vals[*index];
                        BlockEntry {
                            data_type,
                            value: Value::Scalar(default_val.to_owned()),
                        }
                    })
                    .collect::<Vec<_>>();
                let prewhere_block = DataBlock::new(columns.to_vec(), 1);
                let evaluator = Evaluator::new(&prewhere_block, self.func_ctx, &BUILTIN_FUNCTIONS);
                let filter = evaluator
                    .run(filter)
                    .map_err(|e| e.add_message("eval prewhere filter failed:"))?
                    .try_downcast::<BooleanType>()
                    .unwrap();

                if FilterHelpers::is_all_unset(&filter) {
                    return Ok(true);
                }

                // Default value satisfies the filter, update the value of top-k column.
                if let Some((_, sorter, index)) = self.top_k.as_mut() {
                    if !self.array_iters.contains_key(index) {
                        let part = FusePartInfo::from_part(&self.parts[0])?;
                        let num_rows = part.nums_rows;

                        let data_type = self.src_schema.field(*index).data_type().clone();
                        let default_val = self.block_reader.default_vals[*index].clone();
                        let value = Value::Scalar(default_val);
                        let col = value.convert_to_full_column(&data_type, num_rows);
                        let mut bitmap = MutableBitmap::from_len_set(num_rows);
                        sorter.push_column(&col, &mut bitmap);
                    }
                }
            }
        }
        Ok(false)
    }

    /// No more data need to read, finish process.
    fn finish_process(&mut self) -> Result<()> {
        let _ = self.chunks.pop_front();
        let _ = self.parts.pop_front().unwrap();

        self.inited = false;
        self.array_iters.clear();
        self.array_skip_pages.clear();
        Ok(())
    }

    fn fill_block_meta_index(data_block: DataBlock, fuse_part: &FusePartInfo) -> Result<DataBlock> {
        // Fill `BlockMetaInfoPtr` if query internal columns
        let meta: Option<BlockMetaInfoPtr> =
            Some(Box::new(fuse_part.block_meta_index().unwrap().to_owned()));
        data_block.add_meta(meta)
    }

    /// All columns are default values, not need to read.
    fn finish_process_with_default_values(&mut self) -> Result<()> {
        let _ = self.chunks.pop_front();
        let part = self.parts.pop_front().unwrap();
        let fuse_part = FusePartInfo::from_part(&part)?;

        let num_rows = fuse_part.nums_rows;
        let data_block = self.block_reader.build_default_values_block(num_rows)?;
        let data_block = data_block.resort(&self.src_schema, &self.output_schema)?;

        let data_block = if !self.block_reader.query_internal_columns() {
            data_block
        } else {
            Self::fill_block_meta_index(data_block, fuse_part)?
        };

        self.add_block(data_block)?;

        self.inited = false;
        self.array_iters.clear();
        self.array_skip_pages.clear();
        Ok(())
    }

    /// Empty projection use empty block.
    fn finish_process_with_empty_block(&mut self) -> Result<()> {
        let _ = self.chunks.pop_front();
        let part = self.parts.pop_front().unwrap();
        let fuse_part = FusePartInfo::from_part(&part)?;

        let num_rows = fuse_part.nums_rows;
        let data_block = DataBlock::new(vec![], num_rows);
        let data_block = if !self.block_reader.query_internal_columns() {
            data_block
        } else {
            Self::fill_block_meta_index(data_block, fuse_part)?
        };

        self.add_block(data_block)?;
        Ok(())
    }

    /// Update the number of pages that can be skipped per column.
    fn finish_process_skip_page(&mut self) -> Result<()> {
        self.skipped_page += 1;
        for (i, skip_num) in self.array_skip_pages.iter_mut() {
            if self.read_columns.contains(i) {
                continue;
            }
            *skip_num += 1;
        }
        Ok(())
    }

    fn build_array_iter(
        column_node: &ColumnNode,
        leaves: Vec<ColumnDescriptor>,
        readers: Vec<NativeReader<Box<dyn NativeReaderExt>>>,
    ) -> Result<ArrayIter<'static>> {
        let field = column_node.field.clone();
        let is_nested = column_node.is_nested;
        match column_iter_to_arrays(readers, leaves, field, is_nested) {
            Ok(array_iter) => Ok(array_iter),
            Err(err) => Err(err.into()),
        }
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
            if let Some(block_meta) = data_block.take_meta() {
                if let Some(source_meta) = NativeDataSourceMeta::downcast_from(block_meta) {
                    self.parts = VecDeque::from(source_meta.part);
                    self.chunks = VecDeque::from(source_meta.chunks);
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
            if chunks.is_empty() && !self.inited {
                return self.finish_process_with_empty_block();
            }

            // Init array_iters and array_skip_pages to read pages in subsequent processes.
            if !self.inited {
                if let Some((_top_k, sorter, _index)) = self.top_k.as_mut() {
                    let next_part = FusePartInfo::from_part(&self.parts[0])?;
                    if let Some(sort_min_max) = &next_part.sort_min_max {
                        if sorter.never_match(sort_min_max) {
                            return self.finish_process();
                        }
                    }
                }

                self.inited = true;
                for (index, column_node) in
                    self.block_reader.project_column_nodes.iter().enumerate()
                {
                    let readers = chunks.remove(&index).unwrap();
                    if !readers.is_empty() {
                        let leaves = self.column_leaves.get(index).unwrap().clone();
                        let array_iter = Self::build_array_iter(column_node, leaves, readers)?;
                        self.array_iters.insert(index, array_iter);
                        self.array_skip_pages.insert(index, 0);
                    }
                }
                if self.array_iters.len() < self.block_reader.project_column_nodes.len() {
                    // Check if the default value matches the top-k or filter,
                    // if not, return empty block.
                    if self.check_default_values()? {
                        return self.finish_process();
                    }
                }
                // No columns need to read, return default value directly.
                if self.array_iters.is_empty() {
                    return self.finish_process_with_default_values();
                }
            }

            let mut need_to_fill_data = false;
            self.read_columns.clear();
            let mut arrays = Vec::with_capacity(self.array_iters.len());

            // Step 1: Check TOP_K, if prewhere_columns contains not only TOP_K, we can check if TOP_K column can satisfy the heap.
            if self.prewhere_columns.len() > 1 {
                if let Some((top_k, sorter, index)) = self.top_k.as_mut() {
                    if let Some(mut array_iter) = self.array_iters.remove(index) {
                        match array_iter.next() {
                            Some(array) => {
                                let array = array?;
                                self.read_columns.push(*index);
                                let data_type = top_k.order_by.data_type().into();
                                let col = Column::from_arrow(array.as_ref(), &data_type);

                                arrays.push((*index, array));
                                self.array_iters.insert(*index, array_iter);
                                if sorter.never_match_any(&col) {
                                    return self.finish_process_skip_page();
                                }
                            }
                            None => {
                                return self.finish_process();
                            }
                        }
                    }
                }
            }

            // Step 2: Read Prewhere columns and get the filter
            let mut prewhere_default_val_indics = HashSet::new();
            for index in self.prewhere_columns.iter() {
                if self.read_columns.contains(index) {
                    continue;
                }
                if let Some(mut array_iter) = self.array_iters.remove(index) {
                    let skip_pages = self.array_skip_pages.get(index).unwrap();

                    match array_iter.nth(*skip_pages) {
                        Some(array) => {
                            self.read_columns.push(*index);
                            arrays.push((*index, array?));
                            self.array_iters.insert(*index, array_iter);
                            self.array_skip_pages.insert(*index, 0);
                        }
                        None => {
                            return self.finish_process();
                        }
                    }
                } else {
                    prewhere_default_val_indics.insert(*index);
                    need_to_fill_data = true;
                }
            }

            let filter = match self.prewhere_filter.as_ref() {
                Some(filter) => {
                    // Arrays are empty means all prewhere columns are default values,
                    // the filter have checked in the first process, don't need check again.
                    if arrays.is_empty() {
                        None
                    } else {
                        let prewhere_block = if arrays.len() < self.prewhere_columns.len() {
                            self.block_reader
                                .build_block(arrays.clone(), Some(prewhere_default_val_indics))?
                        } else {
                            self.block_reader.build_block(arrays.clone(), None)?
                        };
                        let evaluator =
                            Evaluator::new(&prewhere_block, self.func_ctx, &BUILTIN_FUNCTIONS);
                        let filter = evaluator
                            .run(filter)
                            .map_err(|e| e.add_message("eval prewhere filter failed:"))?
                            .try_downcast::<BooleanType>()
                            .unwrap();

                        // Step 3: Apply the filter, if it's all filtered, we can skip the remain columns.
                        if FilterHelpers::is_all_unset(&filter) {
                            return self.finish_process_skip_page();
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
                            return self.finish_process_skip_page();
                        }
                        Some(filter)
                    }
                }
                None => None,
            };

            // Step 5: read remain columns and filter block if needed.
            for index in self.remain_columns.iter() {
                if let Some(mut array_iter) = self.array_iters.remove(index) {
                    let skip_pages = self.array_skip_pages.get(index).unwrap();

                    match array_iter.nth(*skip_pages) {
                        Some(array) => {
                            self.read_columns.push(*index);
                            arrays.push((*index, array?));
                            self.array_iters.insert(*index, array_iter);
                            self.array_skip_pages.insert(*index, 0);
                        }
                        None => {
                            return self.finish_process();
                        }
                    }
                } else {
                    need_to_fill_data = true;
                }
            }

            let block = self.block_reader.build_block(arrays, None)?;
            let block = if let Some(filter) = filter {
                block.filter_boolean_value(&filter)?
            } else {
                block
            };

            // Step 6: fill missing field default value if need
            let block = if need_to_fill_data {
                self.block_reader
                    .fill_missing_native_column_values(block, &self.parts)?
            } else {
                block
            };

            // Step 7: Fill `BlockMetaIndex` as `DataBlock.meta` if query internal columns,
            // `FillInternalColumnProcessor` will generate internal columns using `BlockMetaIndex` in next pipeline.
            let block = if !self.block_reader.query_internal_columns() {
                block.resort(&self.src_schema, &self.output_schema)?
            } else {
                let fuse_part = FusePartInfo::from_part(&self.parts[0])?;
                let meta: Option<BlockMetaInfoPtr> =
                    Some(Box::new(fuse_part.block_meta_index().unwrap().to_owned()));
                block.add_meta(meta)?
            };

            // Step 8: Add the block to output data
            self.add_block(block)?;
        }

        Ok(())
    }
}
