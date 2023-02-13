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
use std::collections::VecDeque;
use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::native::read::deserialize::column_iter_to_arrays;
use common_arrow::native::read::reader::NativeReader;
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
use common_storage::ColumnNode;
use storages_common_index::Index;
use storages_common_index::RangeIndex;

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

        let mut column_leaves = Vec::with_capacity(block_reader.project_column_nodes.len());
        for column_node in &block_reader.project_column_nodes {
            let leaves: Vec<ColumnDescriptor> = column_node
                .leaf_ids
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

    // read all arrays from NativeReader, skip filtered pages by bitmap
    fn read_arrays(
        column_node: &ColumnNode,
        leaves: Vec<ColumnDescriptor>,
        readers: Vec<NativeReader<Box<dyn NativeReaderExt>>>,
        skip_bitmap: Option<Bitmap>,
    ) -> Result<BTreeMap<usize, Box<dyn Array>>> {
        let field = column_node.field.clone();
        let is_nested = column_node.is_nested;
        let skipped_readers = match skip_bitmap {
            Some(ref skip_bitmap) => readers
                .into_iter()
                .map(|mut reader| {
                    reader.set_skip_pages(skip_bitmap.clone());
                    reader
                })
                .collect::<Vec<_>>(),
            None => readers,
        };

        let mut array_iter = match column_iter_to_arrays(skipped_readers, leaves, field, is_nested)
        {
            Ok(array_iter) => array_iter,
            Err(err) => return Err(err.into()),
        };

        let mut arrays = BTreeMap::new();
        if let Some(skip_bitmap) = skip_bitmap {
            for (i, is_skip) in skip_bitmap.iter().enumerate() {
                if is_skip {
                    continue;
                } else {
                    let array = array_iter.next().unwrap();
                    arrays.insert(i, array?);
                }
            }
        } else {
            for (i, array) in array_iter.enumerate() {
                arrays.insert(i, array?);
            }
        }
        Ok(arrays)
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
        if let Some(mut chunks) = self.chunks.pop_front() {
            let part = self.parts.pop_front().unwrap();
            let part = FusePartInfo::from_part(&part)?;
            // this means it's empty projection
            if chunks.is_empty() {
                let num_rows = part.nums_rows;
                let data_block = self.block_reader.build_default_values_block(num_rows)?;
                self.add_block(data_block)?;
                return Ok(());
            }

            // init skip pages bitmap
            let meta = part.columns_meta.values().last().unwrap();
            let mut native_meta = meta.as_native().unwrap().clone();
            if let Some(range) = &part.range {
                native_meta = native_meta.slice(range.start, range.end);
            }
            let pages_num = native_meta.pages.len();
            let mut skip_pages = MutableBitmap::from_len_zeroed(pages_num);

            let mut arrays = Vec::with_capacity(chunks.len());
            self.read_columns.clear();
            // Step 1: Check TOP_K, if prewhere_columns contains not only TOP_K, we can check if TOP_K column can satisfy the heap.
            if self.prewhere_columns.len() > 1 {
                if let Some((top_k, sorter, index)) = self.top_k.as_mut() {
                    let column_node = &self.block_reader.project_column_nodes[*index];
                    let leaves = self.column_leaves.get(*index).unwrap().clone();
                    let readers = chunks.remove(index).unwrap();
                    let curr_arrays = Self::read_arrays(column_node, leaves, readers, None)?;
                    if curr_arrays.is_empty() {
                        // No data anymore
                        self.check_topn();
                        // check finished
                        return Ok(());
                    }
                    let data_type = top_k.order_by.data_type().into();
                    for (i, curr_array) in curr_arrays.iter() {
                        let col = Column::from_arrow(curr_array.as_ref(), &data_type);
                        if sorter.never_match_any(&col) {
                            self.skipped_page += 1;
                            skip_pages.set(*i, true);
                        }
                    }
                    if skip_pages.unset_bits() == 0 {
                        return Ok(());
                    }
                    self.read_columns.push(*index);
                    arrays.push((*index, curr_arrays));
                }
            }

            // Step 2: Read Prewhere columns and get the filter
            for index in self.prewhere_columns.iter() {
                if self.read_columns.contains(index) {
                    continue;
                }
                let column_node = &self.block_reader.project_column_nodes[*index];
                let leaves = self.column_leaves.get(*index).unwrap().clone();
                let readers = chunks.remove(index).unwrap();
                let skip_bitmap = if skip_pages.unset_bits() < skip_pages.len() {
                    Some(Bitmap::from(skip_pages.clone()))
                } else {
                    None
                };
                let curr_arrays = Self::read_arrays(column_node, leaves, readers, skip_bitmap)?;
                if curr_arrays.is_empty() {
                    // No data anymore
                    self.check_topn();
                    // check finished
                    return Ok(());
                }
                self.read_columns.push(*index);
                arrays.push((*index, curr_arrays));
            }

            let mut filters = BTreeMap::new();
            if let Some(filter) = self.prewhere_filter.as_ref() {
                for i in 0..skip_pages.len() {
                    let is_skip = skip_pages.get(i);
                    if is_skip {
                        continue;
                    }
                    let mut sub_arrays = Vec::with_capacity(arrays.len());
                    for (index, curr_arrays) in &arrays {
                        sub_arrays.push((*index, curr_arrays.get(&i).unwrap().clone()));
                    }
                    let prewhere_block = self.block_reader.build_block(sub_arrays)?;
                    let evaluator =
                        Evaluator::new(&prewhere_block, self.func_ctx, &BUILTIN_FUNCTIONS);
                    let result = evaluator
                        .run(filter)
                        .map_err(|e| e.add_message("eval prewhere filter failed:"))?;
                    let filter = FilterHelpers::cast_to_nonull_boolean(&result).unwrap();

                    // Step 3: Apply the filter, if it's all filtered, we can skip the remain columns.
                    if FilterHelpers::is_all_unset(&filter) {
                        self.skipped_page += 1;
                        skip_pages.set(i, true);
                    } else {
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
                            skip_pages.set(i, true);
                        } else {
                            filters.insert(i, filter);
                        }
                    }
                }
                if skip_pages.unset_bits() == 0 {
                    return Ok(());
                }
            }

            // Step 5: Read remain columns and build block
            for index in self.remain_columns.iter() {
                let column_node = &self.block_reader.project_column_nodes[*index];
                let leaves = self.column_leaves.get(*index).unwrap().clone();
                let readers = chunks.remove(index).unwrap();
                let skip_bitmap = if skip_pages.unset_bits() < skip_pages.len() {
                    Some(Bitmap::from(skip_pages.clone()))
                } else {
                    None
                };
                let curr_arrays = Self::read_arrays(column_node, leaves, readers, skip_bitmap)?;
                arrays.push((*index, curr_arrays));
            }
            let mut blocks = Vec::with_capacity(skip_pages.len() - skip_pages.unset_bits());
            for (i, is_skip) in skip_pages.iter().enumerate() {
                if is_skip {
                    continue;
                }
                let mut sub_arrays = Vec::with_capacity(arrays.len());
                for (index, curr_arrays) in &arrays {
                    sub_arrays.push((*index, curr_arrays.get(&i).unwrap().clone()));
                }
                let block = self.block_reader.build_block(sub_arrays)?;
                let mut block = block.resort(&self.src_schema, &self.output_schema)?;
                if let Some(filter) = filters.remove(&i) {
                    block = block.filter_boolean_value(filter)?;
                }
                blocks.push(block);
            }

            let data_block = DataBlock::concat(&blocks)?;

            // Step 6: fill missing field default value if need
            let data_block = self
                .block_reader
                .fill_missing_native_column_values(data_block, part)?;

            // Step 7: Add the block to output data
            self.add_block(data_block)?;
        }

        Ok(())
    }
}
