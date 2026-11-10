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

//! Order-preserving read of row-based files for `COPY ... FILE_SORTED_BY`.
//!
//! The batches of one file are decoded in parallel and stitched back in file order, so that
//! every output block holds a contiguous range of one file:
//!
//! ```text
//! Separator(+FileEnd) -> resize(1) -> SeqAssigner -> resize(N) -> OrderedBlockBuilder
//!   -> OrderedBatchStitcher(N -> 1) -> resize(N) -> TransformCompactBlock
//! ```
//!
//! Every batch takes the next global sequence number on a single lane, so each builder
//! receives, and emits, increasing sequence numbers. The stitcher holds at most one block per
//! input and consumes them strictly by sequence number. Whenever the next sequence number is
//! not held, its input slot is free, because anything that input already emitted has a
//! smaller sequence number and was consumed; so the stitcher keeps pulling from it and the
//! next block always arrives. Port back pressure alone bounds the memory to about one decoded
//! batch per builder.
//!
//! `FILE_SORTED_BY` is checked at batch granularity: the rows of a block are sorted again on
//! write, so what keeps the blocks of a file from overlapping is that each batch starts at or
//! after the end of the batch before it in the same file. Only the first and last rows of a
//! batch are checked.

use std::any::Any;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::local_block_meta_serde;
use databend_common_expression::utils::sort_key_order::SortKeyEvaluator;
use databend_common_expression::utils::sort_key_order::SortKeyRow;
use databend_common_expression::utils::sort_key_order::compare_sort_key_rows;
use databend_common_expression::utils::sort_key_order::display_sort_key_row;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::processors::BlockCompactMeta;
use databend_common_pipeline_transforms::processors::Transform;

use crate::read::load_context::LoadContext;
use crate::read::row_based::batch::RowBatchWithPosition;
use crate::read::row_based::format::RowBasedFileFormat;
use crate::read::row_based::processors::BlockBuilder;

/// Marks the end of one input file.
#[derive(Debug)]
pub struct FileEnd {
    pub path: String,
}

local_block_meta_serde!(FileEnd);

#[typetag::serde(name = "ordered_file_end")]
impl BlockMetaInfo for FileEnd {}

/// A batch, or the end of a file, with its global sequence number.
#[derive(Debug)]
struct SequencedBatch {
    seq: u64,
    path: String,
    batch: Option<RowBatchWithPosition>,
}

local_block_meta_serde!(SequencedBatch);

#[typetag::serde(name = "ordered_sequenced_batch")]
impl BlockMetaInfo for SequencedBatch {}

/// The decoded block of a [`SequencedBatch`].
#[derive(Debug)]
struct SequencedBlock {
    seq: u64,
    path: String,
    is_file_end: bool,
    /// The row of the file the batch starts at, for error messages.
    start_row: usize,
    /// The sort keys of the first and the last rows of the block, if it has rows.
    bounds: Option<(SortKeyRow, SortKeyRow)>,
}

local_block_meta_serde!(SequencedBlock);

#[typetag::serde(name = "ordered_sequenced_block")]
impl BlockMetaInfo for SequencedBlock {}

fn file_sorted_by_violation(path: &str, row: usize, detail: String) -> ErrorCode {
    ErrorCode::BadBytes(format!(
        "File {path} is not ordered by FILE_SORTED_BY: near row {row}, {detail}"
    ))
}

/// Gives every batch, on a single lane, the next global sequence number.
pub struct SeqAssigner {
    next_seq: u64,
}

impl SeqAssigner {
    pub fn create() -> Self {
        Self { next_seq: 0 }
    }
}

impl Transform for SeqAssigner {
    const NAME: &'static str = "SeqAssigner";

    fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        let meta = data
            .take_meta()
            .ok_or_else(|| ErrorCode::Internal("ordered read expects a batch meta"))?;
        let sequenced = match RowBatchWithPosition::downcast_from_err(meta) {
            Ok(batch) => SequencedBatch {
                seq: self.next_seq,
                path: batch.start_pos.path.clone(),
                batch: Some(batch),
            },
            Err(meta) => {
                let file_end = FileEnd::downcast_from(meta)
                    .ok_or_else(|| ErrorCode::Internal("ordered read got an unknown meta"))?;
                SequencedBatch {
                    seq: self.next_seq,
                    path: file_end.path,
                    batch: None,
                }
            }
        };
        self.next_seq += 1;
        Ok(DataBlock::empty_with_meta(Box::new(sequenced)))
    }
}

/// Decodes each batch into exactly one block, and finds its range of sort keys.
pub struct OrderedBlockBuilder {
    builder: BlockBuilder,
    sort_keys: SortKeyEvaluator,
}

impl OrderedBlockBuilder {
    pub fn try_create(
        ctx: Arc<LoadContext>,
        format: &Arc<dyn RowBasedFileFormat>,
        sort_keys: SortKeyEvaluator,
    ) -> Result<Self> {
        Ok(Self {
            builder: BlockBuilder::create(ctx, format)?,
            sort_keys,
        })
    }

    pub fn create_sort_key_evaluator(ctx: &LoadContext) -> Result<Option<SortKeyEvaluator>> {
        let Some(keys) = ctx.table_context.copy_state().get_file_sort_keys() else {
            return Ok(None);
        };
        let schema = DataSchema::from(ctx.schema.as_ref());
        Ok(Some(SortKeyEvaluator::try_create(
            &keys,
            &schema,
            ctx.table_context.get_function_context()?,
            &BUILTIN_FUNCTIONS,
        )?))
    }
}

impl Transform for OrderedBlockBuilder {
    const NAME: &'static str = "OrderedBlockBuilder";

    fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        let sequenced = data
            .take_meta()
            .and_then(SequencedBatch::downcast_from)
            .ok_or_else(|| ErrorCode::Internal("ordered read expects a sequenced batch"))?;
        let Some(batch) = sequenced.batch else {
            return Ok(DataBlock::empty_with_meta(Box::new(SequencedBlock {
                seq: sequenced.seq,
                path: sequenced.path,
                is_file_end: true,
                start_row: 0,
                bounds: None,
            })));
        };

        let start_row = batch.start_pos.rows;
        let Some(block) = self.builder.decode_batch(batch)? else {
            return Ok(DataBlock::empty_with_meta(Box::new(SequencedBlock {
                seq: sequenced.seq,
                path: sequenced.path,
                is_file_end: false,
                start_row,
                bounds: None,
            })));
        };

        // The rows of a block are sorted again on write, so only its range matters: blocks of one
        // file must not overlap, which the stitcher checks between consecutive batches.
        let bounds = self.sort_keys.boundary_keys(&block, &BUILTIN_FUNCTIONS)?;
        if let Some((first, last)) = &bounds
            && compare_sort_key_rows(first, last) == Ordering::Greater
        {
            return Err(file_sorted_by_violation(
                &sequenced.path,
                start_row + 1,
                format!(
                    "the rows from here start at {}, above {} where they end",
                    display_sort_key_row(first),
                    display_sort_key_row(last),
                ),
            ));
        }
        block.add_meta(Some(Box::new(SequencedBlock {
            seq: sequenced.seq,
            path: sequenced.path,
            is_file_end: false,
            start_row,
            bounds,
        })))
    }
}

/// The blocks of one file waiting to form an output block.
#[derive(Default)]
struct FileRun {
    blocks: Vec<DataBlock>,
    rows: usize,
    bytes: usize,
    /// The sort key of the last row so far; the batches of one file must not start below it.
    last_key: Option<SortKeyRow>,
}

/// Consumes decoded blocks in sequence order and groups each file's consecutive blocks into
/// output blocks, as [`BlockCompactMeta`] for a parallel [`TransformCompactBlock`].
///
/// [`TransformCompactBlock`]: databend_common_pipeline_transforms::processors::TransformCompactBlock
pub struct OrderedBatchStitcher {
    inputs: Vec<Arc<InputPort>>,
    slots: Vec<Option<(SequencedBlock, DataBlock)>>,
    output: Arc<OutputPort>,
    next_seq: u64,
    files: HashMap<String, FileRun>,
    ready: VecDeque<DataBlock>,
    thresholds: BlockThresholds,
}

impl OrderedBatchStitcher {
    pub fn create(
        inputs: Vec<Arc<InputPort>>,
        output: Arc<OutputPort>,
        thresholds: BlockThresholds,
    ) -> ProcessorPtr {
        let slots = inputs.iter().map(|_| None).collect();
        ProcessorPtr::create(Box::new(Self {
            inputs,
            slots,
            output,
            next_seq: 0,
            files: HashMap::new(),
            ready: VecDeque::new(),
            thresholds,
        }))
    }

    fn take_next(&mut self) -> Option<(SequencedBlock, DataBlock)> {
        let next_seq = self.next_seq;
        let slot = self
            .slots
            .iter_mut()
            .find(|slot| matches!(slot, Some((meta, _)) if meta.seq == next_seq))?;
        self.next_seq += 1;
        slot.take()
    }

    fn consume(&mut self, meta: SequencedBlock, block: DataBlock) -> Result<()> {
        if meta.is_file_end {
            if let Some(run) = self.files.remove(&meta.path) {
                self.emit(run.blocks, run.rows, run.bytes);
            }
            return Ok(());
        }
        let Some((first_key, last_key)) = meta.bounds else {
            return Ok(());
        };

        let run = self.files.entry(meta.path.clone()).or_default();
        if let Some(previous) = &run.last_key
            && compare_sort_key_rows(&first_key, previous) == Ordering::Less
        {
            return Err(file_sorted_by_violation(
                &meta.path,
                meta.start_row + 1,
                format!(
                    "the rows from here start at {}, below {} of the rows before",
                    display_sort_key_row(&first_key),
                    display_sort_key_row(previous),
                ),
            ));
        }
        run.last_key = Some(last_key);
        run.rows += block.num_rows();
        run.bytes += block.estimate_block_size(block.num_columns());
        run.blocks.push(block);

        if self.thresholds.check_large_enough(run.rows, run.bytes) {
            let blocks = std::mem::take(&mut run.blocks);
            let (rows, bytes) = (run.rows, run.bytes);
            run.rows = 0;
            run.bytes = 0;
            self.emit(blocks, rows, bytes);
        }
        Ok(())
    }

    fn emit(&mut self, mut blocks: Vec<DataBlock>, rows: usize, bytes: usize) {
        if blocks.is_empty() {
            return;
        }
        let meta = if !self.thresholds.check_for_compact(rows, bytes) {
            BlockCompactMeta::Split {
                block_num: self.thresholds.calc_compact_block_num(rows, bytes),
                blocks,
            }
        } else if blocks.len() == 1 {
            BlockCompactMeta::NoChange(vec![blocks.pop().unwrap()])
        } else {
            BlockCompactMeta::Concat(blocks)
        };
        self.ready
            .push_back(DataBlock::empty_with_meta(Box::new(meta)));
    }
}

impl Processor for OrderedBatchStitcher {
    fn name(&self) -> String {
        "OrderedBatchStitcher".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            for input in &self.inputs {
                input.finish();
            }
            return Ok(Event::Finished);
        }
        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }
        if let Some(block) = self.ready.pop_front() {
            self.output.push_data(Ok(block));
            return Ok(Event::NeedConsume);
        }

        let mut all_finished = true;
        for (input, slot) in self.inputs.iter().zip(self.slots.iter_mut()) {
            if slot.is_none() && input.has_data() {
                let mut block = input.pull_data().unwrap()?;
                let meta = block
                    .take_meta()
                    .and_then(SequencedBlock::downcast_from)
                    .ok_or_else(|| ErrorCode::Internal("ordered read expects a sequenced block"))?;
                *slot = Some((meta, block));
            }
            if slot.is_some() {
                all_finished = false;
            } else if !input.is_finished() {
                // Only a free slot asks for more: a held block is the back pressure.
                input.set_need_data();
                all_finished = false;
            }
        }

        let next_seq = self.next_seq;
        if self
            .slots
            .iter()
            .any(|slot| matches!(slot, Some((meta, _)) if meta.seq == next_seq))
        {
            return Ok(Event::Sync);
        }
        if all_finished {
            if !self.files.is_empty() {
                return Ok(Event::Sync);
            }
            self.output.finish();
            return Ok(Event::Finished);
        }
        if self.inputs.iter().all(|input| input.is_finished()) {
            return Err(ErrorCode::Internal(format!(
                "ordered read lost the batch with sequence number {next_seq}"
            )));
        }
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        while let Some((meta, block)) = self.take_next() {
            self.consume(meta, block)?;
        }
        if self.inputs.iter().all(|input| input.is_finished())
            && self.slots.iter().all(Option::is_none)
        {
            // Files whose end marker never arrived, e.g. after an upstream that stops early.
            for (_, run) in std::mem::take(&mut self.files) {
                self.emit(run.blocks, run.rows, run.bytes);
            }
        }
        Ok(())
    }
}
