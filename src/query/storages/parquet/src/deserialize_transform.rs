//  Copyright 2023 Datafuse Labs.
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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::parquet::indexes::Interval;
use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::filter_helper::FilterHelpers;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_expression::TopKSorter;
use common_expression::Value;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;

use crate::parquet_part::ParquetRowGroupPart;
use crate::parquet_reader::IndexedReaders;
use crate::parquet_reader::ParquetReader;
use crate::parquet_source::ParquetSourceMeta;

#[derive(Clone)]
pub struct ParquetPrewhereInfo {
    pub func_ctx: FunctionContext,
    pub reader: Arc<ParquetReader>,
    pub filter: Expr,
    pub top_k: Option<(usize, TopKSorter)>,
    // the usize is the index of the column in ParquetReader.schema
}

pub struct ParquetDeserializeTransform {
    // Used for pipeline operations
    scan_progress: Arc<Progress>,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,

    // data from input
    parts: VecDeque<PartInfoPtr>,
    data_readers: VecDeque<IndexedReaders>,

    src_schema: DataSchemaRef,
    output_schema: DataSchemaRef,

    // Used for prewhere reading and filtering
    prewhere_info: Option<ParquetPrewhereInfo>,

    // Used for remain reading
    remain_reader: Arc<ParquetReader>,
    // Used for top k optimization
    top_k_finished: bool,
}

impl ParquetDeserializeTransform {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        src_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        prewhere_info: Option<ParquetPrewhereInfo>,
        remain_reader: Arc<ParquetReader>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();

        Ok(ProcessorPtr::create(Box::new(
            ParquetDeserializeTransform {
                scan_progress,
                input,
                output,
                output_data: None,

                parts: VecDeque::new(),
                data_readers: VecDeque::new(),

                src_schema,
                output_schema,

                prewhere_info,
                remain_reader,

                top_k_finished: false,
            },
        )))
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
        if let Some(ParquetPrewhereInfo {
            top_k: Some((_, sorter)),
            ..
        }) = &mut self.prewhere_info.as_mut()
        {
            if let Some(next_part) = self.parts.front() {
                let next_part = next_part
                    .as_any()
                    .downcast_ref::<ParquetRowGroupPart>()
                    .unwrap();

                if let Some(sort_min_max) = &next_part.sort_min_max {
                    self.top_k_finished = sorter.never_match(sort_min_max);
                }
            }
        }
    }
}

impl Processor for ParquetDeserializeTransform {
    fn name(&self) -> String {
        String::from("ParquetDeserializeTransform")
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

        if !self.data_readers.is_empty() {
            if !self.input.has_data() {
                self.input.set_need_data();
            }
            return Ok(Event::Sync);
        }

        if self.top_k_finished {
            self.input.finish();
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;
            let source_meta = data_block.take_meta().unwrap();
            let source_meta = ParquetSourceMeta::downcast_from(source_meta).unwrap();

            self.parts = VecDeque::from(source_meta.parts);

            self.check_topn();
            if self.top_k_finished {
                self.input.finish();
                self.output.finish();
                return Ok(Event::Finished);
            }

            self.data_readers = VecDeque::from(source_meta.readers);
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
        if let Some(mut readers) = self.data_readers.pop_front() {
            let part = self.parts.pop_front().unwrap();
            let part = ParquetRowGroupPart::from_part(&part)?;
            let row_selection = part
                .row_selection
                .as_ref()
                .map(|sel| intervals_to_bitmap(sel, part.num_rows));

            // this means it's empty projection
            if readers.is_empty() {
                let data_block = DataBlock::new(vec![], part.num_rows);
                self.add_block(data_block)?;
                return Ok(());
            }

            let data_block = match self.prewhere_info.as_mut() {
                Some(ParquetPrewhereInfo {
                    func_ctx,
                    reader,
                    filter,
                    top_k,
                }) => {
                    let chunks = reader.read_from_readers(&mut readers)?;

                    // only if there is not dictionary page, we can push down the row selection
                    let can_push_down = chunks
                        .iter()
                        .all(|(id, _)| !part.column_metas[id].has_dictionary);
                    let push_down = if can_push_down {
                        row_selection.clone()
                    } else {
                        None
                    };

                    let mut prewhere_block = reader.deserialize(part, chunks, push_down)?;
                    // Step 1: Check TOP_K, if prewhere_columns contains not only TOP_K, we can check if TOP_K column can satisfy the heap.
                    if let Some((index, sorter)) = top_k {
                        let col = prewhere_block
                            .get_by_offset(*index)
                            .value
                            .as_column()
                            .unwrap();
                        if sorter.never_match_any(col) {
                            return Ok(());
                        }
                    }

                    // Step 2: Read Prewhere columns and get the filter
                    let evaluator = Evaluator::new(&prewhere_block, *func_ctx, &BUILTIN_FUNCTIONS);
                    let filter = evaluator
                        .run(filter)
                        .map_err(|e| e.add_message("eval prewhere filter failed:"))?
                        .try_downcast::<BooleanType>()
                        .unwrap();

                    // Step 3: Apply the filter, if it's all filtered, we can skip the remain columns.
                    if FilterHelpers::is_all_unset(&filter) {
                        return Ok(());
                    }

                    // Step 4: Apply the filter to topk and update the bitmap, this will filter more results
                    let filter = if let Some((index, sorter)) = top_k {
                        let top_k_column = prewhere_block
                            .get_by_offset(*index)
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
                        return Ok(());
                    }

                    // Step 5 Remove columns that are not needed for output. Use dummy column to replce them.
                    let mut columns = prewhere_block.columns().to_vec();
                    for (col, f) in columns.iter_mut().zip(reader.output_schema.fields()) {
                        if !self.output_schema.has_field(f.name()) {
                            *col = BlockEntry {
                                data_type: DataType::Null,
                                value: Value::Scalar(Scalar::Null),
                            };
                        }
                    }

                    // Step 6: Read remain columns.
                    let chunks = self.remain_reader.read_from_readers(&mut readers)?;
                    let can_push_down = chunks
                        .iter()
                        .all(|(id, _)| !part.column_metas[id].has_dictionary);
                    let push_down = if can_push_down { row_selection } else { None };
                    if push_down.is_some() || !can_push_down {
                        let remain_block =
                            self.remain_reader.deserialize(part, chunks, push_down)?;

                        // Combine two blocks.
                        for col in remain_block.columns() {
                            prewhere_block.add_column(col.clone());
                        }

                        let block = prewhere_block.resort(&self.src_schema, &self.output_schema)?;
                        block.filter_boolean_value(&filter)
                    } else {
                        // filter prewhere columns first.
                        let mut prewhere_block = prewhere_block.filter_boolean_value(&filter)?;
                        // If row_selection is None, we can push down the prewhere filter to remain data deserialization.
                        let remain_block = match filter {
                            Value::Column(bitmap) => {
                                self.remain_reader.deserialize(part, chunks, Some(bitmap))?
                            }
                            _ => self.remain_reader.deserialize(part, chunks, None)?, // all true
                        };
                        for col in remain_block.columns() {
                            prewhere_block.add_column(col.clone());
                        }

                        prewhere_block.resort(&self.src_schema, &self.output_schema)
                    }
                }
                None => {
                    let chunks = self.remain_reader.read_from_readers(&mut readers)?;
                    self.remain_reader.deserialize(part, chunks, row_selection)
                }
            }?;

            self.add_block(data_block)?;
        }

        Ok(())
    }
}

/// Convert intervals to a bitmap. The `intervals` represents the row selection across `num_rows`.
fn intervals_to_bitmap(interval: &[Interval], num_rows: usize) -> Bitmap {
    debug_assert!(
        interval.is_empty()
            || interval.last().unwrap().start + interval.last().unwrap().length < num_rows
    );

    let mut bitmap = MutableBitmap::with_capacity(num_rows);
    let mut offset = 0;

    for intv in interval {
        bitmap.extend_constant(intv.start - offset, false);
        bitmap.extend_constant(intv.length, true);
        offset = intv.start + intv.length;
    }
    bitmap.extend_constant(num_rows - offset, false);

    bitmap.into()
}
