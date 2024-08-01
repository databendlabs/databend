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
use std::ops::BitAnd;
use std::sync::Arc;

use databend_common_arrow::arrow::array::Array;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_arrow::native::read::ArrayIter;
use databend_common_arrow::parquet::metadata::ColumnDescriptor;
use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::TopK;
use databend_common_catalog::plan::VirtualColumnInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::eval_function;
use databend_common_expression::filter_helper::FilterHelpers;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FieldIndex;
use databend_common_expression::FilterExecutor;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::SelectExprBuilder;
use databend_common_expression::TopKSorter;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_metrics::storage::*;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_sql::IndexType;
use xorf::BinaryFuse16;

use super::native_data_source::NativeDataSource;
use super::util::add_data_block_meta;
use super::util::need_reserve_block_info;
use crate::fuse_part::FuseBlockPartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::VirtualColumnReader;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;
use crate::operations::read::runtime_filter_prunner::update_bitmap_with_bloom_filter;
use crate::DEFAULT_ROW_PER_PAGE;

/// A helper struct to store the intermediate state while reading a native partition.
#[derive(Default)]
struct ReadPartState {
    // Structures for reading a partition:
    /// The [`ArrayIter`] of each columns to read native pages in order.
    array_iters: BTreeMap<usize, ArrayIter<'static>>,
    /// The number of pages need to be skipped for each iter in `array_iters`.
    array_skip_pages: BTreeMap<usize, usize>,
    /// `read_column_ids` is the columns that are in the block to read.
    ///
    /// The not read columns may have two cases:
    /// 1. the columns added after `alter table`.
    /// 2. the source columns used to generate virtual columns,
    ///    and all the virtual columns have been generated,
    ///    then the source columns are not needed.
    ///
    /// These columns need to be filled with their default values.
    read_column_ids: HashSet<ColumnId>,
    /// If the block to read has default values, this flag is used for a short path.
    if_need_fill_defaults: bool,
    /// If current partition is finished.
    is_finished: bool,
    /// Row offset of next pages.
    offset: usize,

    // Structures for reading a set of pages (and produce a block):
    /// Indices of columns are already read into memory.
    /// It's used to mark the prefethed columns such as top-k and prewhere columns.
    read_columns: HashSet<usize>,
    /// Columns are already read into memory.
    arrays: Vec<(usize, Box<dyn Array>)>,
    /// The number of rows that are filtered while reading current set of pages.
    /// It's used for the filter executor.
    filtered_count: Option<usize>,
}

impl ReadPartState {
    fn new() -> Self {
        Self {
            array_iters: BTreeMap::new(),
            array_skip_pages: BTreeMap::new(),
            read_column_ids: HashSet::new(),
            if_need_fill_defaults: false,
            is_finished: true, // new state should be finished.
            offset: 0,
            read_columns: HashSet::new(),
            arrays: Vec::new(),
            filtered_count: None,
        }
    }

    /// Reset all the state. Mark the state as finished.
    fn finish(&mut self) {
        self.array_iters.clear();
        self.array_skip_pages.clear();
        self.read_column_ids.clear();
        self.if_need_fill_defaults = false;
        self.offset = 0;
        self.new_pages();

        self.is_finished = true;
    }

    /// Reset the state for reading a new set of pages (prepare to produce a new block).
    fn new_pages(&mut self) {
        self.read_columns.clear();
        self.arrays.clear();
        self.filtered_count = None;
    }

    /// Skip one page for each unread column.
    fn skip_pages(&mut self) {
        for (i, s) in self.array_skip_pages.iter_mut() {
            if self.read_columns.contains(i) {
                continue;
            }
            *s += 1;
        }
        if let Some((_, array)) = self.arrays.first() {
            // Advance the offset.
            self.offset += array.len();
        }
    }

    /// Read one page of one column.
    ///
    /// Return false if the column is finished.
    #[inline(always)]
    fn read_page(&mut self, index: usize) -> Result<bool> {
        if self.read_columns.contains(&index) {
            return Ok(true);
        }

        if let Some(array_iter) = self.array_iters.get_mut(&index) {
            let skipped_pages = self.array_skip_pages.get(&index).unwrap();
            match array_iter.nth(*skipped_pages) {
                Some(array) => {
                    self.read_columns.insert(index);
                    self.arrays.push((index, array?));
                    // reset the skipped pages for next reading.
                    self.array_skip_pages.insert(index, 0);
                }
                None => {
                    self.finish();
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// Check if current partition is finished.
    #[inline(always)]
    fn is_finished(&self) -> bool {
        self.is_finished
    }
}

pub struct NativeDeserializeDataTransform {
    // Structures for driving the pipeline:
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    parts: VecDeque<PartInfoPtr>,
    chunks: VecDeque<NativeDataSource>,
    scan_progress: Arc<Progress>,

    // Structures for table scan information:
    table_index: IndexType,
    block_reader: Arc<BlockReader>,
    column_leaves: Vec<Vec<ColumnDescriptor>>,
    src_schema: DataSchema,
    output_schema: DataSchema,

    // Structures for TopK:
    top_k: Option<(TopK, TopKSorter, usize)>,

    // Structures for prewhere and filter:
    func_ctx: FunctionContext,
    prewhere_schema: DataSchema,
    prewhere_columns: Vec<usize>,
    prewhere_filter: Arc<Option<Expr>>,
    filter_executor: Option<FilterExecutor>,

    // Structures for virtual columns:
    virtual_columns: Option<Vec<VirtualColumnInfo>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,
    prewhere_virtual_columns: Option<Vec<VirtualColumnInfo>>,

    // Structures for the bloom runtime filter:
    ctx: Arc<dyn TableContext>,
    bloom_runtime_filter: Option<Vec<(FieldIndex, BinaryFuse16)>>,

    // Structures for aggregating index:
    index_reader: Arc<Option<AggIndexReader>>,
    remain_columns: Vec<usize>,

    // Other structures:
    base_block_ids: Option<Scalar>,
    /// Record the state while reading a native partition.
    read_state: ReadPartState,
    /// Record how many sets of pages have been skipped.
    /// It's used for metrics.
    skipped_pages: usize,

    // for merge_into target build.
    need_reserve_block_info: bool,
}

impl NativeDeserializeDataTransform {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        ctx: Arc<dyn TableContext>,
        block_reader: Arc<BlockReader>,
        plan: &DataSourcePlan,
        top_k: Option<TopK>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        let (need_reserve_block_info, _) = need_reserve_block_info(ctx.clone(), plan.table_index);
        let mut src_schema: DataSchema = (block_reader.schema().as_ref()).into();

        let mut prewhere_columns: Vec<usize> =
            match PushDownInfo::prewhere_of_push_downs(plan.push_downs.as_ref()) {
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
            let index = src_schema.index_of(top_k.field.name()).unwrap();
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

        let filter_executor = if let Some(expr) = prewhere_filter.as_ref() {
            let (select_expr, has_or) = SelectExprBuilder::new().build(expr).into();
            Some(FilterExecutor::new(
                select_expr,
                func_ctx.clone(),
                has_or,
                DEFAULT_ROW_PER_PAGE,
                None,
                &BUILTIN_FUNCTIONS,
                false,
            ))
        } else if top_k.is_some() {
            Some(new_dummy_filter_executor(func_ctx.clone()))
        } else {
            None
        };

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
                ctx,
                table_index: plan.table_index,
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
                filter_executor,
                skipped_pages: 0,
                top_k,
                index_reader,
                virtual_reader,
                base_block_ids: plan.base_block_ids.clone(),
                bloom_runtime_filter: None,
                read_state: ReadPartState::new(),
                need_reserve_block_info,
            },
        )))
    }

    fn build_prewhere_filter_expr(
        plan: &DataSourcePlan,
        schema: &DataSchema,
    ) -> Result<Arc<Option<Expr>>> {
        Ok(Arc::new(
            PushDownInfo::prewhere_of_push_downs(plan.push_downs.as_ref()).map(|v| {
                v.filter
                    .as_expr(&BUILTIN_FUNCTIONS)
                    .project_column_ref(|name| schema.column_with_name(name).unwrap().0)
            }),
        ))
    }

    fn add_output_block(&mut self, data_block: DataBlock) {
        let rows = data_block.num_rows();
        if rows == 0 {
            return;
        }
        let progress_values = ProgressValues {
            rows,
            bytes: data_block.memory_size(),
        };
        self.scan_progress.incr(&progress_values);
        Profile::record_usize_profile(ProfileStatisticsName::ScanBytes, data_block.memory_size());
        self.output_data = Some(data_block);
    }

    /// If the virtual column has been already generated, add it directly,
    /// otherwise extract it from the source column
    fn add_virtual_columns(
        &self,
        chunks: &[(usize, Box<dyn Array>)],
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
                        Value::Column(Column::from_arrow(array.as_ref(), &data_type)?),
                    );
                    // If the source column is the default value, num_rows may be zero
                    if block.num_columns() > 0 && block.num_rows() == 0 {
                        let num_rows = array.len();
                        let mut columns = block.columns().to_vec();
                        columns.push(column);
                        *block = DataBlock::new(columns, num_rows);
                    } else {
                        block.add_column(column);
                    }
                    continue;
                }
                let index = schema.index_of(&virtual_column.source_name).unwrap();
                let source = block.get_by_offset(index);
                let src_arg = (source.value.clone(), source.data_type.clone());
                let path_arg = (
                    Value::Scalar(virtual_column.key_paths.clone()),
                    DataType::String,
                );

                let (value, data_type) = eval_function(
                    None,
                    "get_by_keypath",
                    [src_arg, path_arg],
                    &self.func_ctx,
                    block.num_rows(),
                    &BUILTIN_FUNCTIONS,
                )?;

                let column = BlockEntry::new(data_type, value);
                block.add_column(column);
            }
        }

        Ok(())
    }

    /// Check if can skip the whole block by default values.
    /// If the top-k or all prewhere columns are default values, check if the filter is met,
    /// and if not, ignore all pages, otherwise continue without repeating the check for subsequent processes.
    fn check_default_values(&mut self) -> Result<bool> {
        if self.prewhere_columns.len() > 1 {
            if let Some((_, sorter, index)) = self.top_k.as_mut() {
                if !self.read_state.array_iters.contains_key(index) {
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
                .all(|index| !self.read_state.array_iters.contains_key(index));

            let all_virtual_defaults = match &self.prewhere_virtual_columns {
                Some(ref prewhere_virtual_columns) => prewhere_virtual_columns.iter().all(|c| {
                    let src_index = self.src_schema.index_of(&c.source_name).unwrap();
                    !self.read_state.array_iters.contains_key(&src_index)
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
                    if !self.read_state.array_iters.contains_key(index) {
                        let part = FuseBlockPartInfo::from_part(&self.parts[0])?;
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

    /// Finish the processing of current partition.
    fn finish_partition(&mut self) {
        self.read_state.finish();
        self.chunks.pop_front();
        self.parts.pop_front();
    }

    /// Build a block whose columns are all default values.
    fn build_default_block(&self, fuse_part: &FuseBlockPartInfo) -> Result<DataBlock> {
        let mut data_block = self
            .block_reader
            .build_default_values_block(fuse_part.nums_rows)?;
        if let Some(virtual_columns) = &self.virtual_columns {
            for virtual_column in virtual_columns {
                // if the source column is default value, the virtual column is always Null.
                let column = BlockEntry::new(
                    DataType::from(&*virtual_column.data_type),
                    Value::Scalar(Scalar::Null),
                );
                data_block.add_column(column);
            }
        }

        data_block = add_data_block_meta(
            data_block,
            fuse_part,
            None,
            self.base_block_ids.clone(),
            self.block_reader.update_stream_columns(),
            self.block_reader.query_internal_columns(),
            self.need_reserve_block_info,
        )?;

        data_block.resort(&self.src_schema, &self.output_schema)
    }

    /// Initialize the read state for a new partition.
    fn new_read_state(&mut self) -> Result<()> {
        debug_assert!(self.read_state.is_finished());
        debug_assert!(!self.chunks.is_empty());
        debug_assert!(!self.parts.is_empty());

        if let NativeDataSource::Normal(chunks) = self.chunks.front_mut().unwrap() {
            let part = self.parts.front().unwrap();
            let part = FuseBlockPartInfo::from_part(part)?;

            if let Some(range) = part.range() {
                self.read_state.offset = part.page_size() * range.start;
            }

            for (index, column_node) in self.block_reader.project_column_nodes.iter().enumerate() {
                let readers = chunks.remove(&index).unwrap_or_default();
                if !readers.is_empty() {
                    let leaves = self.column_leaves.get(index).unwrap().clone();
                    let array_iter = BlockReader::build_array_iter(column_node, leaves, readers)?;
                    self.read_state.array_iters.insert(index, array_iter);
                    self.read_state.array_skip_pages.insert(index, 0);

                    for column_id in &column_node.leaf_column_ids {
                        self.read_state.read_column_ids.insert(*column_id);
                    }
                } else {
                    self.read_state.if_need_fill_defaults = true;
                }
            }

            // Add optional virtual columns' array_iters.
            if let Some(virtual_reader) = self.virtual_reader.as_ref() {
                for (index, virtual_column_info) in
                    virtual_reader.virtual_column_infos.iter().enumerate()
                {
                    let virtual_index = index + self.block_reader.project_column_nodes.len();
                    if let Some(readers) = chunks.remove(&virtual_index) {
                        let array_iter = BlockReader::build_virtual_array_iter(
                            virtual_column_info.name.clone(),
                            readers,
                        )?;
                        let index = self.src_schema.index_of(&virtual_column_info.name)?;
                        self.read_state.array_iters.insert(index, array_iter);
                        self.read_state.array_skip_pages.insert(index, 0);
                    }
                }
            }

            // Mark the state as active.
            self.read_state.is_finished = false;
        }

        Ok(())
    }

    /// Read and produce one [`DataBlock`].
    ///
    /// Columns in native format are stored in pages and each page has the same number of rows.
    /// Each `read_pages` produce a block by reading a page from each column.
    ///
    /// **NOTES**: filter and internal columns will be applied after calling this method.
    fn read_pages(&mut self) -> Result<Option<DataBlock>> {
        // Each loop tries to read one page of each column and combine them into a block.
        // If a page is skipped, ignore all the parallel pages of other columns and start a new loop.
        loop {
            if self.read_state.is_finished() {
                // The reader is already finished.
                return Ok(None);
            }

            // Prepare to read a new set of pages.
            self.read_state.new_pages();

            // 1. check the TopK heap.
            if !self.read_and_check_topk()? {
                // skip current pages.
                self.skipped_pages += 1;
                self.read_state.skip_pages();
                continue;
            }

            // 2. check prewhere columns and evaluator the filter.
            if !self.read_and_check_prewhere()? {
                // skip current pages.
                self.skipped_pages += 1;
                self.read_state.skip_pages();
                continue;
            }

            // 3. Update the topk heap and the filter.
            if !self.update_topk_heap()? {
                // skip current pages.
                self.skipped_pages += 1;
                self.read_state.skip_pages();
                continue;
            }

            // 4. check and evaluator the bloom runtime filter.
            if !self.read_and_check_bloom_runtime_filter()? {
                // skip current pages.
                self.skipped_pages += 1;
                self.read_state.skip_pages();
                continue;
            }

            // 5. read remain columns and generate a data block.
            if !self.read_remain_columns()? {
                debug_assert!(self.read_state.is_finished());
                return Ok(None);
            }
            let mut block = self
                .block_reader
                .build_block(&self.read_state.arrays, None)?;

            // 6. fill missing fields with default values.
            if self.read_state.if_need_fill_defaults {
                block = self
                    .block_reader
                    .fill_missing_native_column_values(block, &self.read_state.read_column_ids)?;
            }

            // 7. add optional virtual columns.
            self.add_virtual_columns(
                &self.read_state.arrays,
                &self.src_schema,
                &self.virtual_columns,
                &mut block,
            )?;

            return Ok(Some(block));
        }
    }

    /// Read and check the top-k column (only one column).
    ///
    /// It's always the first checking when read pages.
    /// So it will never skip any page.
    ///
    /// Returns false if skip the current page or the partition is finished.
    fn read_and_check_topk(&mut self) -> Result<bool> {
        if let Some((top_k, sorter, index)) = self.top_k.as_mut() {
            if !self.read_state.read_page(*index)? {
                debug_assert!(self.read_state.is_finished());
                return Ok(false);
            }
            // TopK should always be the first read column.
            debug_assert_eq!(self.read_state.arrays.len(), 1);
            let (i, array) = self.read_state.arrays.last().unwrap();
            debug_assert_eq!(i, index);
            let data_type = top_k.field.data_type().into();
            let col = Column::from_arrow(array.as_ref(), &data_type)?;
            if sorter.never_match_any(&col) {
                // skip current page.
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Read and check prewhere columns.
    ///
    /// Returns false if skip the current page or the partition is finished.
    fn read_and_check_prewhere(&mut self) -> Result<bool> {
        let mut prewhere_default_val_indices = HashSet::new();
        // Read the columns into memory.
        for index in self.prewhere_columns.iter() {
            if self.read_state.read_columns.contains(index) {
                continue;
            }

            let num_columns = self.read_state.arrays.len();
            if !self.read_state.read_page(*index)? {
                debug_assert!(self.read_state.is_finished());
                return Ok(false);
            }

            if num_columns == self.read_state.arrays.len() {
                // It means the column is not read and it's a default value.
                prewhere_default_val_indices.insert(*index);
            }
        }

        // Evaluate the filter.
        // If `self.read_state.arrays.is_empty()`,
        // it means there are only default columns in prewhere columns. (all prewhere columns are newly added by `alter table`)
        // In this case, we don't need to evaluate the filter, because the unsatisfied blocks are already filtered in `read_partitions`.
        if self.prewhere_filter.is_some() && !self.read_state.arrays.is_empty() {
            debug_assert!(self.filter_executor.is_some());
            let mut prewhere_block = if self.read_state.arrays.len() < self.prewhere_columns.len() {
                self.block_reader
                    .build_block(&self.read_state.arrays, Some(prewhere_default_val_indices))?
            } else {
                self.block_reader
                    .build_block(&self.read_state.arrays, None)?
            };
            // Add optional virtual columns for prewhere
            self.add_virtual_columns(
                &self.read_state.arrays,
                &self.prewhere_schema,
                &self.prewhere_virtual_columns,
                &mut prewhere_block,
            )?;

            let filter_executor = self.filter_executor.as_mut().unwrap();

            let count = filter_executor.select(&prewhere_block)?;

            // If it's all filtered, we can skip the current pages.
            if count == 0 {
                return Ok(false);
            }

            self.read_state.filtered_count = Some(count);
        }

        Ok(true)
    }

    // TODO(xudong): add selectivity prediction
    /// Read and check the column for the bloom runtime filter (only one column).
    ///
    /// Returns false if skip the current page or the partition is finished.
    fn read_and_check_bloom_runtime_filter(&mut self) -> Result<bool> {
        if let Some(bloom_runtime_filter) = self.bloom_runtime_filter.as_ref() {
            let mut bitmaps = Vec::with_capacity(bloom_runtime_filter.len());
            for (idx, filter) in bloom_runtime_filter.iter() {
                let array = if let Some((_, array)) =
                    self.read_state.arrays.iter().find(|(i, _)| i == idx)
                {
                    (*idx, array.clone())
                } else if !self.read_state.read_page(*idx)? {
                    debug_assert!(self.read_state.is_finished());
                    return Ok(false);
                } else {
                    // The runtime filter column must be the last column to read.
                    let (i, array) = self.read_state.arrays.last().unwrap();
                    debug_assert_eq!(i, idx);
                    (*idx, array.clone())
                };

                let probe_block = self.block_reader.build_block(&[array], None)?;
                let mut bitmap = MutableBitmap::from_len_zeroed(probe_block.num_rows());
                let probe_column = probe_block.get_last_column().clone();
                update_bitmap_with_bloom_filter(probe_column, filter, &mut bitmap)?;
                let unset_bits = bitmap.unset_bits();
                if unset_bits == bitmap.len() {
                    // skip current page.
                    return Ok(false);
                }
                if unset_bits != 0 {
                    bitmaps.push(bitmap);
                }
            }
            if !bitmaps.is_empty() {
                let rf_bitmap = bitmaps
                    .into_iter()
                    .reduce(|acc, rf_filter| acc.bitand(&rf_filter.into()))
                    .unwrap();

                let filter_executor = self.filter_executor.as_mut().unwrap();
                let filter_count = if let Some(count) = self.read_state.filtered_count {
                    filter_executor.select_bitmap(count, rf_bitmap)
                } else {
                    filter_executor.from_bitmap(rf_bitmap)
                };
                self.read_state.filtered_count = Some(filter_count);
            }
        }

        Ok(true)
    }

    /// Update the top-k heap with by the topk column.
    ///
    /// Returns false if skip the current page.
    fn update_topk_heap(&mut self) -> Result<bool> {
        if let Some((top_k, sorter, index)) = &mut self.top_k {
            // Topk column should always be the first column read.
            let (i, array) = self.read_state.arrays.first().unwrap();
            debug_assert_eq!(i, index);
            let data_type = top_k.field.data_type().into();
            let col = Column::from_arrow(array.as_ref(), &data_type)?;

            let filter_executor = self.filter_executor.as_mut().unwrap();
            let count = if let Some(count) = self.read_state.filtered_count {
                sorter.push_column_with_selection::<false>(
                    &col,
                    filter_executor.mutable_true_selection(),
                    count,
                )
            } else {
                // If there is no prewhere filter, initialize the true selection.
                sorter.push_column_with_selection::<true>(
                    &col,
                    filter_executor.mutable_true_selection(),
                    col.len(),
                )
            };

            if count == 0 {
                return Ok(false);
            }
            self.read_state.filtered_count = Some(count);
        };

        Ok(true)
    }

    /// Read remain columns.
    ///
    /// Returns false if the partition is finished.
    fn read_remain_columns(&mut self) -> Result<bool> {
        for index in self.remain_columns.iter() {
            if !self.read_state.read_page(*index)? {
                debug_assert!(self.read_state.is_finished());
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Try to get bloom runtime filter from context.
    fn try_init_bloom_runtime_filter(&mut self) {
        if self.bloom_runtime_filter.is_none() {
            let bloom_filters = self.ctx.get_bloom_runtime_filter_with_id(self.table_index);
            let bloom_filters = bloom_filters
                .into_iter()
                .filter_map(|filter| {
                    let name = filter.0.as_str();
                    // Some probe keys are not in the schema, they are derived from expressions.
                    self.src_schema
                        .index_of(name)
                        .ok()
                        .map(|idx| (idx, filter.1.clone()))
                })
                .collect::<Vec<_>>();
            if !bloom_filters.is_empty() {
                self.bloom_runtime_filter = Some(bloom_filters);
                if self.filter_executor.is_none() {
                    self.filter_executor = Some(new_dummy_filter_executor(self.func_ctx.clone()));
                }
            }
        }
    }

    /// Pre-process the partition before reading it.
    fn pre_process_partition(&mut self) -> Result<()> {
        debug_assert!(!self.chunks.is_empty());
        debug_assert!(!self.parts.is_empty());

        // Create a new read state.
        self.new_read_state()?;

        if self.read_state.if_need_fill_defaults && self.check_default_values()? {
            // Check if the default value matches the top-k or filter,
            // if not, finish current partition.
            self.finish_partition();
            return Ok(());
        }

        if self.read_state.array_iters.is_empty() {
            // All columns are default values, not need to read.
            let part = self.parts.front().unwrap();
            let fuse_part = FuseBlockPartInfo::from_part(part)?;
            let block = self.build_default_block(fuse_part)?;
            self.add_output_block(block);

            self.finish_partition();
            return Ok(());
        }

        Ok(())
    }

    /// Post preprocess after reading a block.
    fn post_process_block(&mut self, block: DataBlock) -> Result<DataBlock> {
        let origin_num_rows = block.num_rows();
        let block = if let Some(count) = &self.read_state.filtered_count {
            let filter_executor = self.filter_executor.as_mut().unwrap();
            filter_executor.take(block, origin_num_rows, *count)?
        } else {
            block
        };

        // Fill `InternalColumnMeta` as `DataBlock.meta` if query internal columns,
        // `TransformAddInternalColumns` will generate internal columns using `InternalColumnMeta` in next pipeline.
        let mut block = block.resort(&self.src_schema, &self.output_schema)?;
        let fuse_part = FuseBlockPartInfo::from_part(&self.parts[0])?;
        let offsets = if self.block_reader.query_internal_columns() {
            let offset = self.read_state.offset;
            let offsets = if let Some(count) = self.read_state.filtered_count {
                let filter_executor = self.filter_executor.as_mut().unwrap();
                filter_executor.mutable_true_selection()[0..count]
                    .iter()
                    .map(|idx| *idx as usize + offset)
                    .collect::<Vec<_>>()
            } else {
                (offset..offset + origin_num_rows).collect()
            };
            Some(offsets)
        } else {
            None
        };
        block = add_data_block_meta(
            block,
            fuse_part,
            offsets,
            self.base_block_ids.clone(),
            self.block_reader.update_stream_columns(),
            self.block_reader.query_internal_columns(),
            self.need_reserve_block_info,
        )?;

        self.read_state.offset += origin_num_rows;

        Ok(block)
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
                if let Some(source_meta) = DataSourceWithMeta::downcast_from(block_meta) {
                    self.parts = VecDeque::from(source_meta.meta);
                    self.chunks = VecDeque::from(source_meta.data);
                    return Ok(Event::Sync);
                }
            }

            unreachable!();
        }

        if self.input.is_finished() {
            metrics_inc_pruning_prewhere_nums(self.skipped_pages as u64);
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        // Try to get the bloom runtime filter from the context if existed.
        self.try_init_bloom_runtime_filter();

        // Only if current read state is finished can we start to read a new partition.
        if self.read_state.is_finished() {
            if let Some(chunks) = self.chunks.front_mut() {
                let chunks = match chunks {
                    NativeDataSource::AggIndex(data) => {
                        let agg_index_reader = self.index_reader.as_ref().as_ref().unwrap();
                        let block = agg_index_reader.deserialize_native_data(data)?;
                        self.output_data = Some(block);
                        self.finish_partition();
                        return Ok(());
                    }
                    NativeDataSource::Normal(data) => data,
                };

                if chunks.is_empty() {
                    // This means it's an empty projection
                    let part = self.parts.front().unwrap();
                    let fuse_part = FuseBlockPartInfo::from_part(part)?;
                    let mut data_block = DataBlock::new(vec![], fuse_part.nums_rows);
                    data_block = add_data_block_meta(
                        data_block,
                        fuse_part,
                        None,
                        self.base_block_ids.clone(),
                        self.block_reader.update_stream_columns(),
                        self.block_reader.query_internal_columns(),
                        self.need_reserve_block_info,
                    )?;

                    self.finish_partition();
                    self.add_output_block(data_block);
                    return Ok(());
                }

                // Prepare to read a new partition.
                self.pre_process_partition()?;
            }
        }

        if self.read_state.is_finished() {
            // There is no more partitions to read.
            return Ok(());
        }

        // Each `process` try to produce one `DataBlock`.
        if let Some(block) = self.read_pages()? {
            let block = self.post_process_block(block)?;
            self.add_output_block(block);
        } else {
            // No more data can be read from current partition.
            self.finish_partition();
        }

        Ok(())
    }
}

/// Build a dummy filter executor to retain a selection.
///
/// This method may be used by `update_topk_heap` and `read_and_check_bloom_runtime_filter`.
fn new_dummy_filter_executor(func_ctx: FunctionContext) -> FilterExecutor {
    let dummy_expr = Expr::Constant {
        span: None,
        scalar: Scalar::Boolean(true),
        data_type: DataType::Boolean,
    };
    let (select_expr, has_or) = SelectExprBuilder::new().build(&dummy_expr).into();
    // TODO: specify the capacity (max_block_size) of the selection.
    FilterExecutor::new(
        select_expr,
        func_ctx,
        has_or,
        DEFAULT_ROW_PER_PAGE,
        None,
        &BUILTIN_FUNCTIONS,
        false,
    )
}
