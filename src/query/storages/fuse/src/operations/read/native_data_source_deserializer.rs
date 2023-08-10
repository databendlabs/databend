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
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::native::read::ArrayIter;
use common_arrow::parquet::metadata::ColumnDescriptor;
use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::PushDownInfo;
use common_catalog::plan::TopK;
use common_catalog::plan::VirtualColumnInfo;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::eval_function;
use common_expression::filter_helper::FilterHelpers;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::BlockEntry;
use common_expression::BlockMetaInfoDowncast;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_expression::TopKSorter;
use common_expression::Value;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;

use super::fuse_source::fill_internal_column_meta;
use super::native_data_source::DataSource;
use crate::fuse_part::FusePartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::metrics::metrics_inc_pruning_prewhere_nums;
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
    chunks: VecDeque<DataSource>,

    prewhere_columns: Vec<usize>,
    prewhere_schema: DataSchema,
    remain_columns: Vec<usize>,

    src_schema: DataSchema,
    output_schema: DataSchema,
    virtual_columns: Option<Vec<VirtualColumnInfo>>,

    prewhere_filter: Arc<Option<Expr>>,
    prewhere_virtual_columns: Option<Vec<VirtualColumnInfo>>,

    skipped_page: usize,
    // The row offset of current part.
    // It's used to compute the row offset in one block (single data file in one segment).
    offset_in_part: usize,

    read_columns: Vec<usize>,
    top_k: Option<(TopK, TopKSorter, usize)>,
    // Identifies whether the ArrayIter has been initialised.
    inited: bool,
    // The ArrayIter of each columns to read Pages in order.
    array_iters: BTreeMap<usize, ArrayIter<'static>>,
    // The Page numbers of each ArrayIter can skip.
    array_skip_pages: BTreeMap<usize, usize>,

    index_reader: Arc<Option<AggIndexReader>>,
}

impl NativeDeserializeDataTransform {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        block_reader: Arc<BlockReader>,
        plan: &DataSourcePlan,
        top_k: Option<TopK>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        index_reader: Arc<Option<AggIndexReader>>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();

        let mut src_schema: DataSchema = (block_reader.schema().as_ref()).into();

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

        // add virtual columns to src_schema
        let (virtual_columns, prewhere_virtual_columns) = match &plan.push_downs {
            Some(push_downs) => {
                if let Some(virtual_columns) = &push_downs.virtual_columns {
                    let mut fields = src_schema.fields().clone();
                    for virtual_column in virtual_columns {
                        let field = DataField::new(
                            &virtual_column.name,
                            DataType::from(&*virtual_column.data_type),
                        );
                        fields.push(field);
                    }
                    src_schema = DataSchema::new(fields);
                }
                if let Some(prewhere) = &push_downs.prewhere {
                    if let Some(virtual_columns) = &prewhere.virtual_columns {
                        for virtual_column in virtual_columns {
                            prewhere_columns
                                .push(src_schema.index_of(&virtual_column.name).unwrap());
                        }
                        prewhere_columns.sort();
                    }
                    (
                        push_downs.virtual_columns.clone(),
                        prewhere.virtual_columns.clone(),
                    )
                } else {
                    (push_downs.virtual_columns.clone(), None)
                }
            }
            None => (None, None),
        };

        let remain_columns: Vec<usize> = (0..src_schema.num_fields())
            .filter(|i| !prewhere_columns.contains(i))
            .collect();

        let func_ctx = ctx.get_function_context()?;
        let prewhere_schema = src_schema.project(&prewhere_columns);
        let prewhere_filter = Self::build_prewhere_filter_expr(plan, &prewhere_schema)?;

        let mut output_schema = plan.schema().as_ref().clone();
        output_schema.remove_internal_fields();
        let output_schema: DataSchema = (&output_schema).into();

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
                prewhere_schema,
                remain_columns,
                src_schema,
                output_schema,
                virtual_columns,

                prewhere_filter,
                prewhere_virtual_columns,
                skipped_page: 0,
                top_k,
                read_columns: vec![],
                inited: false,
                array_iters: BTreeMap::new(),
                array_skip_pages: BTreeMap::new(),
                offset_in_part: 0,

                index_reader,
            },
        )))
    }

    fn build_prewhere_filter_expr(
        plan: &DataSourcePlan,
        schema: &DataSchema,
    ) -> Result<Arc<Option<Expr>>> {
        Ok(Arc::new(
            PushDownInfo::prewhere_of_push_downs(&plan.push_downs).map(|v| {
                v.filter
                    .as_expr(&BUILTIN_FUNCTIONS)
                    .project_column_ref(|name| schema.column_with_name(name).unwrap().0)
            }),
        ))
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

    /// If the virtual column has already generated, add it directly,
    /// otherwise generate it from the source column
    fn add_virtual_columns(
        &self,
        chunks: Vec<(usize, Box<dyn Array>)>,
        schema: &DataSchema,
        virtual_columns: &Option<Vec<VirtualColumnInfo>>,
        block: &mut DataBlock,
    ) -> Result<()> {
        if let Some(virtual_columns) = virtual_columns {
            for virtual_column in virtual_columns {
                let src_index = self.src_schema.index_of(&virtual_column.name).unwrap();
                if let Some(array) = chunks
                    .iter()
                    .find(|c| c.0 == src_index)
                    .map(|c| c.1.clone())
                {
                    let data_type: DataType =
                        (*self.src_schema.field(src_index).data_type()).clone();
                    let column = BlockEntry::new(
                        data_type.clone(),
                        Value::Column(Column::from_arrow(array.as_ref(), &data_type)),
                    );
                    block.add_column(column);
                    continue;
                }
                let index = schema.index_of(&virtual_column.source_name).unwrap();
                let source = block.get_by_offset(index);
                let mut src_arg = (source.value.clone(), source.data_type.clone());
                for path in virtual_column.paths.iter() {
                    let path_arg = match path {
                        Scalar::String(_) => (Value::Scalar(path.clone()), DataType::String),
                        Scalar::Number(NumberScalar::UInt64(_)) => (
                            Value::Scalar(path.clone()),
                            DataType::Number(NumberDataType::UInt64),
                        ),
                        _ => unreachable!(),
                    };
                    let (value, data_type) = eval_function(
                        None,
                        "get",
                        [src_arg, path_arg],
                        &self.func_ctx,
                        block.num_rows(),
                        &BUILTIN_FUNCTIONS,
                    )?;
                    src_arg = (value, data_type);
                }
                let column = BlockEntry::new(DataType::from(&*virtual_column.data_type), src_arg.0);
                block.add_column(column);
            }
        }

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

            let all_virtual_defaults = match &self.prewhere_virtual_columns {
                Some(ref prewhere_virtual_columns) => prewhere_virtual_columns.iter().all(|c| {
                    let src_index = self.src_schema.index_of(&c.source_name).unwrap();
                    !self.array_iters.contains_key(&src_index)
                }),
                None => true,
            };

            if *all_defaults && all_virtual_defaults {
                let columns = &mut self
                    .prewhere_columns
                    .iter()
                    .map(|index| {
                        let data_type = self.src_schema.field(*index).data_type().clone();
                        let default_val = &self.block_reader.default_vals[*index];
                        BlockEntry::new(data_type, Value::Scalar(default_val.to_owned()))
                    })
                    .collect::<Vec<_>>();

                if let Some(ref prewhere_virtual_columns) = &self.prewhere_virtual_columns {
                    for virtual_column in prewhere_virtual_columns {
                        // if the source column is default value, the virtual column is always Null.
                        let column = BlockEntry::new(
                            DataType::from(&*virtual_column.data_type),
                            Value::Scalar(Scalar::Null),
                        );
                        columns.push(column);
                    }
                }

                let prewhere_block = DataBlock::new(columns.to_vec(), 1);
                let evaluator = Evaluator::new(&prewhere_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
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
        self.offset_in_part = 0;
        Ok(())
    }

    /// All columns are default values, not need to read.
    fn finish_process_with_default_values(&mut self) -> Result<()> {
        let _ = self.chunks.pop_front();
        let part = self.parts.pop_front().unwrap();
        let fuse_part = FusePartInfo::from_part(&part)?;

        let num_rows = fuse_part.nums_rows;
        let mut data_block = self.block_reader.build_default_values_block(num_rows)?;
        if let Some(ref virtual_columns) = &self.virtual_columns {
            for virtual_column in virtual_columns {
                // if the source column is default value, the virtual column is always Null.
                let column = BlockEntry::new(
                    DataType::from(&*virtual_column.data_type),
                    Value::Scalar(Scalar::Null),
                );
                data_block.add_column(column);
            }
        }
        let data_block = if !self.block_reader.query_internal_columns() {
            data_block
        } else {
            fill_internal_column_meta(data_block, fuse_part, None)?
        };
        let data_block = data_block.resort(&self.src_schema, &self.output_schema)?;
        self.add_block(data_block)?;

        self.inited = false;
        self.array_iters.clear();
        self.array_skip_pages.clear();
        self.offset_in_part = 0;
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
            fill_internal_column_meta(data_block, fuse_part, None)?
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
                    self.chunks = VecDeque::from(source_meta.data);
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
            let chunks = match chunks {
                DataSource::AggIndex(data) => {
                    let agg_index_reader = self.index_reader.as_ref().as_ref().unwrap();
                    let block = agg_index_reader.deserialize_native_data(data)?;
                    self.output_data = Some(block);
                    return self.finish_process();
                }
                DataSource::Normal(data) => data,
            };

            // this means it's empty projection
            if chunks.is_empty() && !self.inited {
                return self.finish_process_with_empty_block();
            }

            // Init array_iters and array_skip_pages to read pages in subsequent processes.
            if !self.inited {
                let fuse_part = FusePartInfo::from_part(&self.parts[0])?;
                if let Some(range) = fuse_part.range() {
                    self.offset_in_part = fuse_part.page_size() * range.start;
                }

                if let Some((_top_k, sorter, _index)) = self.top_k.as_mut() {
                    if let Some(sort_min_max) = &fuse_part.sort_min_max {
                        if sorter.never_match(sort_min_max) {
                            return self.finish_process();
                        }
                    }
                }

                let mut has_default_value = false;
                self.inited = true;
                for (index, column_node) in
                    self.block_reader.project_column_nodes.iter().enumerate()
                {
                    let readers = chunks.remove(&index).unwrap();
                    if !readers.is_empty() {
                        let leaves = self.column_leaves.get(index).unwrap().clone();
                        let array_iter =
                            BlockReader::build_array_iter(column_node, leaves, readers)?;
                        self.array_iters.insert(index, array_iter);
                        self.array_skip_pages.insert(index, 0);
                    } else {
                        has_default_value = true;
                    }
                }
                if let Some(ref virtual_columns_meta) = fuse_part.virtual_columns_meta {
                    // Add optional virtual column array_iter
                    for (name, virtual_column_meta) in virtual_columns_meta.iter() {
                        let virtual_index = virtual_column_meta.index
                            + self.block_reader.project_column_nodes.len();
                        if let Some(readers) = chunks.remove(&virtual_index) {
                            let array_iter = BlockReader::build_virtual_array_iter(
                                name.clone(),
                                virtual_column_meta.desc.clone(),
                                readers,
                            )?;
                            let index = self.src_schema.index_of(name)?;
                            self.array_iters.insert(index, array_iter);
                            self.array_skip_pages.insert(index, 0);
                        }
                    }
                }

                if has_default_value {
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
                                    self.offset_in_part += col.len();
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
            let mut prewhere_default_val_indices = HashSet::new();
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
                    prewhere_default_val_indices.insert(*index);
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
                        let mut prewhere_block = if arrays.len() < self.prewhere_columns.len() {
                            self.block_reader
                                .build_block(arrays.clone(), Some(prewhere_default_val_indices))?
                        } else {
                            self.block_reader.build_block(arrays.clone(), None)?
                        };
                        // Add optional virtual columns for prewhere
                        self.add_virtual_columns(
                            arrays.clone(),
                            &self.prewhere_schema,
                            &self.prewhere_virtual_columns,
                            &mut prewhere_block,
                        )?;

                        let evaluator =
                            Evaluator::new(&prewhere_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
                        let filter = evaluator
                            .run(filter)
                            .map_err(|e| e.add_message("eval prewhere filter failed:"))?
                            .try_downcast::<BooleanType>()
                            .unwrap();

                        // Step 3: Apply the filter, if it's all filtered, we can skip the remain columns.
                        if FilterHelpers::is_all_unset(&filter) {
                            self.offset_in_part += prewhere_block.num_rows();
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
                            self.offset_in_part += prewhere_block.num_rows();
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

            let block = self.block_reader.build_block(arrays.clone(), None)?;
            let origin_num_rows = block.num_rows();
            let block = if let Some(filter) = &filter {
                block.filter_boolean_value(filter)?
            } else {
                block
            };

            // Step 6: fill missing field default value if need
            let mut block = if need_to_fill_data {
                self.block_reader
                    .fill_missing_native_column_values(block, &self.parts)?
            } else {
                block
            };

            // Step 7: Add optional virtual columns
            self.add_virtual_columns(arrays, &self.src_schema, &self.virtual_columns, &mut block)?;

            // Step 8: Fill `InternalColumnMeta` as `DataBlock.meta` if query internal columns,
            // `FillInternalColumnProcessor` will generate internal columns using `InternalColumnMeta` in next pipeline.
            let mut block = block.resort(&self.src_schema, &self.output_schema)?;
            if self.block_reader.query_internal_columns() {
                let offsets = if let Some(Value::Column(bitmap)) = filter.as_ref() {
                    (self.offset_in_part..self.offset_in_part + origin_num_rows)
                        .filter(|i| unsafe { bitmap.get_bit_unchecked(i - self.offset_in_part) })
                        .collect()
                } else {
                    (self.offset_in_part..self.offset_in_part + origin_num_rows).collect()
                };

                let fuse_part = FusePartInfo::from_part(&self.parts[0])?;
                block = fill_internal_column_meta(block, fuse_part, Some(offsets))?;
            };

            // Step 9: Add the block to output data
            self.offset_in_part += origin_num_rows;
            self.add_block(block)?;
        }

        Ok(())
    }
}
