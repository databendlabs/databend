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
use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::split_row_id;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::local_block_meta_serde;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::Buffer;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_pipeline::basic::create_resize_item;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::AccumulatingTransform;
use databend_common_pipeline_transforms::AccumulatingTransformer;
use databend_common_pipeline_transforms::create_dummy_item;
use databend_storages_common_cache::CachePolicy;
use databend_storages_common_io::ReadSettings;

use super::parquet_rows_fetcher::ParquetRowsFetcher;
use super::parquet_rows_fetcher::RowsFetchMetadata;
use super::parquet_rows_fetcher::TransformRowFetch;
use crate::FuseStorageFormat;
use crate::FuseTable;

/// Adds the fetch of `projection` to the lanes of `pipeline` selected by
/// `fetch_lane`, leaving the other lanes as they are. Each selected lane becomes
///
/// ```text
/// Plan -> Resize(1, N) -> Fetch x N -> Resize(N, 1) -> Assemble
/// ```
///
/// Each `Fetch` handles one block at a time: it reads asynchronously, so the workers
/// of the IO runtime only do IO, then deserializes synchronously on an executor
/// thread. So N bounds both the concurrent reads and the block data held in memory.
///
/// A lane keeps its order. The rows of a batch are spread over the blocks they
/// come from, whose fetches overtake each other in the N lanes, and `Assemble`
/// restores the batches and their rows: a sort upstream must not be lost.
#[allow(clippy::too_many_arguments)]
pub fn build_row_fetch_pipeline(
    ctx: Arc<dyn TableContext>,
    pipeline: &mut Pipeline,
    row_id_col_offset: usize,
    source: &DataSourcePlan,
    projection: Projection,
    need_wrap_nullable: bool,
    cache_policy: CachePolicy,
    fetch_lane: impl Fn(usize) -> bool,
) -> Result<()> {
    let table = ctx.build_table_from_source_plan(source)?;
    let fuse_table = table
        .as_any()
        .downcast_ref::<FuseTable>()
        .ok_or_else(|| ErrorCode::Internal("Row fetcher is only supported by Fuse engine"))?
        .to_owned();
    let fuse_table = Arc::new(fuse_table);
    if let FuseStorageFormat::Unsupported = &fuse_table.storage_format {
        return Err(crate::unsupported_storage_format_error());
    }

    let table_schema = source.source_info.schema();
    let projected_schema = projection.project_schema(table_schema.as_ref());
    let fetched_data_types = projected_schema
        .fields()
        .iter()
        .map(|field| DataType::from(field.data_type()))
        .collect::<Vec<_>>();
    let block_reader =
        fuse_table.create_block_reader(ctx.clone(), projection.clone(), cache_policy)?;
    let read_settings = ReadSettings::from_ctx(&ctx)?;
    let settings = ctx.get_settings();
    let max_threads = settings.get_max_threads()? as usize;
    let block_threshold = BlockThreshold {
        max_rows: settings.get_max_block_size()? as usize,
        max_bytes: settings.get_max_block_bytes()? as usize,
        cur_rows: 0,
        cur_bytes: 0,
    };

    let lanes = (0..pipeline.output_len())
        .map(fetch_lane)
        .collect::<Vec<_>>();
    let fetched_lanes = lanes.iter().filter(|fetched| **fetched).count();
    if fetched_lanes == 0 {
        return Ok(());
    }
    // Lanes are not merged: downstream may depend on them, e.g. a block id
    // repartition upstream or the matched and unmatched lanes of a MERGE INTO.
    let width = (max_threads / fetched_lanes).max(1);

    add_pipe_by_lane(pipeline, &lanes, || {
        Ok(vec![transform_item(|input, output| {
            TransformRowFetchPlan::create(
                input,
                output,
                row_id_col_offset,
                ParquetRowsFetcher::create(fuse_table.clone(), projection.clone(), &block_reader),
                block_threshold,
            )
        })])
    })?;
    if width > 1 {
        add_pipe_by_lane(pipeline, &lanes, || Ok(vec![create_resize_item(1, width)]))?;
    }
    add_pipe_by_lane(pipeline, &lanes, || {
        Ok((0..width)
            .map(|_| {
                transform_item(|input, output| {
                    TransformRowFetch::create(input, output, block_reader.clone(), read_settings)
                })
            })
            .collect())
    })?;
    if width > 1 {
        add_pipe_by_lane(pipeline, &lanes, || Ok(vec![create_resize_item(width, 1)]))?;
    }
    add_pipe_by_lane(pipeline, &lanes, || {
        Ok(vec![transform_item(|input, output| {
            AccumulatingTransformer::create(
                input,
                output,
                TransformRowFetchAssemble::create(
                    row_id_col_offset,
                    need_wrap_nullable,
                    fetched_data_types.clone(),
                ),
            )
        })])
    })
}

/// Adds a pipe with the items of `fetched` for each fetched lane, and a
/// pass-through for each other lane.
fn add_pipe_by_lane(
    pipeline: &mut Pipeline,
    lanes: &[bool],
    fetched: impl Fn() -> Result<Vec<PipeItem>>,
) -> Result<()> {
    let mut items = Vec::with_capacity(lanes.len());
    for is_fetched in lanes {
        match is_fetched {
            true => items.extend(fetched()?),
            false => items.push(create_dummy_item()),
        }
    }
    let inputs = items.iter().map(|item| item.inputs_port.len()).sum();
    let outputs = items.iter().map(|item| item.outputs_port.len()).sum();
    pipeline.add_pipe(Pipe::create(inputs, outputs, items));
    Ok(())
}

fn transform_item(
    create: impl FnOnce(Arc<InputPort>, Arc<OutputPort>) -> Box<dyn Processor>,
) -> PipeItem {
    let input = InputPort::create();
    let output = OutputPort::create();
    let processor = ProcessorPtr::create(create(input.clone(), output.clone()));
    PipeItem::create(processor, vec![input], vec![output])
}

/// The input rows of a fetch, to which `Assemble` adds the fetched columns.
pub(super) struct RowFetchBatch {
    id: u64,
    inputs: Vec<DataBlock>,
    /// For each row with a row id, the part it is in and its row in that part.
    indices: Vec<(u32, u32)>,
    parts: usize,
    start: Instant,
}

/// What `Plan` sends to `Fetch`, as the meta of an empty block.
pub(super) struct RowFetchTask {
    pub(super) batch: Arc<RowFetchBatch>,
    /// `None` for a batch without row ids, so that it also reaches `Assemble`.
    pub(super) block: Option<RowFetchBlockTask>,
}

/// The rows to fetch from one block of a batch.
pub(super) struct RowFetchBlockTask {
    pub(super) part: u32,
    pub(super) metadata: Arc<RowsFetchMetadata>,
    pub(super) take_indices: Vec<u32>,
}

/// What `Fetch` sends to `Assemble`, as the meta of an empty block.
pub(super) struct RowFetched {
    pub(super) batch: Arc<RowFetchBatch>,
    /// The part and its rows, `None` for a batch without row ids.
    pub(super) block: Option<(u32, DataBlock)>,
}

impl Debug for RowFetchTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowFetchTask").finish()
    }
}

impl Debug for RowFetched {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowFetched").finish()
    }
}

local_block_meta_serde!(RowFetchTask);
local_block_meta_serde!(RowFetched);

#[typetag::serde(name = "row_fetch_task")]
impl BlockMetaInfo for RowFetchTask {}

#[typetag::serde(name = "row_fetched")]
impl BlockMetaInfo for RowFetched {}

/// The message a block of a fetch carries.
pub(super) fn take_row_fetch_meta<T: BlockMetaInfoDowncast>(mut block: DataBlock) -> Result<T> {
    block
        .take_meta()
        .and_then(T::downcast_from)
        .ok_or_else(|| ErrorCode::Internal("Row fetch got a block without its message"))
}

#[derive(Clone, Copy)]
pub struct BlockThreshold {
    max_rows: usize,
    max_bytes: usize,
    cur_rows: usize,
    cur_bytes: usize,
}

impl BlockThreshold {
    pub fn acc(&mut self, rows: usize, bytes: usize) -> bool {
        self.cur_rows += rows;
        self.cur_bytes += bytes;
        self.check_threshold()
    }

    fn check_threshold(&self) -> bool {
        self.cur_rows >= self.max_rows || self.cur_bytes >= self.max_bytes
    }

    fn reset(&mut self) {
        self.cur_rows = 0;
        self.cur_bytes = 0;
    }
}

/// Batches the input rows by their size with the fetched columns, and splits each
/// batch into a task per block its row ids are in. Async for the block metadata.
pub struct TransformRowFetchPlan {
    fetcher: ParquetRowsFetcher,

    initialize: bool,
    finished: bool,
    row_id_col_offset: usize,
    fetch_row_ids: Vec<u64>,
    blocks: Vec<DataBlock>,
    next_batch_id: u64,

    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,

    input_data: Option<DataBlock>,
    messages: VecDeque<DataBlock>,

    block_threshold: BlockThreshold,
    metadata: HashMap<u64, Arc<RowsFetchMetadata>>,
}

#[async_trait::async_trait]
impl Processor for TransformRowFetchPlan {
    fn name(&self) -> String {
        String::from("TransformRowFetchPlan")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output_port.is_finished() {
            self.input_port.finish();
            return Ok(Event::Finished);
        }

        if !self.output_port.can_push() {
            self.input_port.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(message) = self.messages.pop_front() {
            self.output_port.push_data(Ok(message));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Async);
        }

        if self.input_port.has_data() {
            self.input_data = Some(self.input_port.pull_data().unwrap()?);
            return Ok(Event::Async);
        }

        if self.input_port.is_finished() {
            if !self.finished {
                self.finished = true;
                return Ok(Event::Async);
            }

            self.output_port.finish();
            return Ok(Event::Finished);
        }

        self.input_port.set_need_data();
        Ok(Event::NeedData)
    }

    async fn async_process(&mut self) -> Result<()> {
        if !self.initialize {
            self.initialize = true;
            self.fetcher.initialize().await?;
        }

        if let Some(data) = self.input_data.take() {
            let num_rows = data.num_rows();
            let fetched_columns_bytes = data.memory_size() / data.num_rows();
            let entry = &data.columns()[self.row_id_col_offset];
            let mut consumed_len = num_rows;
            match entry.data_type() {
                DataType::Number(NumberDataType::UInt64) => {
                    let row_id_column = self.get_row_id_column(&data);
                    // Process the row id column in block batch
                    // Ensure that the same block would be processed in the same batch and threads
                    for (idx, row_id) in row_id_column.iter().enumerate() {
                        let (prefix, _) = split_row_id(*row_id);

                        if !self.metadata.contains_key(&prefix) {
                            let metadata = self.fetcher.fetch_metadata(prefix).await?;
                            self.metadata.insert(prefix, metadata);
                        }

                        let fetch_columns_bytes = self.metadata[&prefix].row_bytes;
                        let bytes = fetched_columns_bytes + fetch_columns_bytes;
                        let reach_threshold = self.block_threshold.acc(1, bytes);
                        self.fetch_row_ids.push(*row_id);
                        if reach_threshold {
                            consumed_len = idx + 1;
                            break;
                        }
                    }

                    self.blocks.push(data.slice(0..consumed_len));
                }
                DataType::Nullable(inner)
                    if matches!(inner.as_ref(), DataType::Number(NumberDataType::UInt64)) =>
                {
                    let column = entry.to_column();
                    let value = column.into_nullable().unwrap();
                    let row_id_column = value.column.into_number().unwrap().into_u_int64().unwrap();

                    for (idx, (row_id, is_valid)) in
                        row_id_column.iter().zip(value.validity.iter()).enumerate()
                    {
                        if !is_valid {
                            if self.block_threshold.acc(1, fetched_columns_bytes) {
                                consumed_len = idx + 1;
                                break;
                            }
                            continue;
                        }

                        let (prefix, _) = split_row_id(*row_id);

                        if !self.metadata.contains_key(&prefix) {
                            let metadata = self.fetcher.fetch_metadata(prefix).await?;
                            self.metadata.insert(prefix, metadata);
                        }

                        let fetch_columns_bytes = self.metadata[&prefix].row_bytes;
                        let bytes = fetched_columns_bytes + fetch_columns_bytes;
                        let reach_threshold = self.block_threshold.acc(1, bytes);
                        self.fetch_row_ids.push(*row_id);
                        if reach_threshold {
                            consumed_len = idx + 1;
                            break;
                        }
                    }

                    self.blocks.push(data.slice(0..consumed_len));
                }
                _ => unreachable!("Row id column should be UInt64 or Nullable(UInt64)"),
            };

            let remain_rows = data.slice(consumed_len..num_rows);

            if !remain_rows.is_empty() {
                self.input_data = Some(remain_rows);
            }
        }

        if !self.finished && !self.block_threshold.check_threshold() {
            return Ok(());
        }

        self.block_threshold.reset();
        self.plan_batch();

        assert!(self.blocks.is_empty());
        assert!(self.metadata.is_empty());

        Ok(())
    }
}

impl TransformRowFetchPlan {
    fn get_row_id_column(&self, data: &DataBlock) -> Buffer<u64> {
        let entry = &data.columns()[self.row_id_col_offset];
        let column = entry.to_column();
        if matches!(entry.data_type(), DataType::Number(NumberDataType::UInt64)) {
            column.into_number().unwrap().into_u_int64().unwrap()
        } else {
            // From merge into matched data, the row id column is nullable but has no null value.
            let value = *column.into_nullable().unwrap();
            debug_assert!(value.validity.null_count() == 0);
            value.column.into_number().unwrap().into_u_int64().unwrap()
        }
    }

    fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        row_id_col_offset: usize,
        fetcher: ParquetRowsFetcher,
        block_threshold: BlockThreshold,
    ) -> Box<dyn Processor> {
        Box::new(TransformRowFetchPlan {
            fetcher,
            block_threshold,
            row_id_col_offset,

            input_port: input,
            output_port: output,
            blocks: vec![],
            input_data: None,
            messages: VecDeque::new(),
            fetch_row_ids: vec![],
            metadata: HashMap::new(),
            next_batch_id: 0,
            finished: false,
            initialize: false,
        })
    }

    /// Queues the batch accumulated so far, and a task per block of its row ids.
    fn plan_batch(&mut self) {
        let inputs = std::mem::take(&mut self.blocks);
        let row_ids = std::mem::take(&mut self.fetch_row_ids);
        let metadata = std::mem::take(&mut self.metadata);

        if inputs.iter().all(|block| block.num_rows() == 0) {
            return;
        }

        let parts = metadata
            .keys()
            .enumerate()
            .map(|(part, block_id)| (*block_id, part as u32))
            .collect::<HashMap<_, _>>();

        // The rows to take from each block, and where each row of the batch is.
        let mut take_indices = HashMap::with_capacity(metadata.len());
        let mut indices = Vec::with_capacity(row_ids.len());
        for row_id in row_ids {
            let (block_id, row) = split_row_id(row_id);
            match take_indices.entry(block_id) {
                Entry::Occupied(mut rows) => {
                    let rows: &mut Vec<_> = rows.get_mut();
                    indices.push((parts[&block_id], rows.len() as u32));
                    rows.push(row as u32);
                }
                Entry::Vacant(rows) => {
                    indices.push((parts[&block_id], 0));
                    rows.insert(vec![row as u32]);
                }
            }
        }

        let batch = Arc::new(RowFetchBatch {
            id: self.next_batch_id,
            inputs,
            indices,
            parts: take_indices.len(),
            start: Instant::now(),
        });
        self.next_batch_id += 1;

        if take_indices.is_empty() {
            self.messages
                .push_back(DataBlock::empty_with_meta(Box::new(RowFetchTask {
                    batch,
                    block: None,
                })));
            return;
        }
        for (block_id, take_indices) in take_indices {
            self.messages
                .push_back(DataBlock::empty_with_meta(Box::new(RowFetchTask {
                    batch: batch.clone(),
                    block: Some(RowFetchBlockTask {
                        part: parts[&block_id],
                        metadata: metadata[&block_id].clone(),
                        take_indices,
                    }),
                })));
        }
    }
}

/// A batch of `Assemble` and the fetched parts received so far, in any order.
struct PendingBatch {
    batch: Arc<RowFetchBatch>,
    fetched: Vec<(u32, DataBlock)>,
}

impl PendingBatch {
    fn is_complete(&self) -> bool {
        self.batch.parts == self.fetched.len()
    }
}

/// Adds the fetched columns to the input rows, batch after batch.
pub struct TransformRowFetchAssemble {
    row_id_col_offset: usize,
    need_wrap_nullable: bool,
    fetched_data_types: Vec<DataType>,
    next_batch_id: u64,
    pending: HashMap<u64, PendingBatch>,
}

impl AccumulatingTransform for TransformRowFetchAssemble {
    const NAME: &'static str = "TransformRowFetchAssemble";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let RowFetched { batch, block } = take_row_fetch_meta(data)?;
        let pending = self.pending.entry(batch.id).or_insert(PendingBatch {
            batch,
            fetched: Vec::new(),
        });
        pending.fetched.extend(block);

        let mut output = Vec::new();
        while self
            .pending
            .get(&self.next_batch_id)
            .is_some_and(PendingBatch::is_complete)
        {
            let pending = self.pending.remove(&self.next_batch_id).unwrap();
            self.next_batch_id += 1;
            output.push(self.assemble(pending)?);
        }
        Ok(output)
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if output && !self.pending.is_empty() {
            return Err(ErrorCode::Internal(format!(
                "Row fetch finished with {} incomplete batches",
                self.pending.len()
            )));
        }
        Ok(vec![])
    }
}

impl TransformRowFetchAssemble {
    fn create(
        row_id_col_offset: usize,
        need_wrap_nullable: bool,
        fetched_data_types: Vec<DataType>,
    ) -> Self {
        TransformRowFetchAssemble {
            row_id_col_offset,
            need_wrap_nullable,
            fetched_data_types,
            next_batch_id: 0,
            pending: HashMap::new(),
        }
    }

    fn assemble(&self, pending: PendingBatch) -> Result<DataBlock> {
        let batch = pending.batch;
        let num_blocks = batch.inputs.len();
        let mut data = DataBlock::concat(&batch.inputs)?;
        let num_rows = data.num_rows();

        let entry = &data.columns()[self.row_id_col_offset];
        let row_id_validity = match entry.data_type() {
            DataType::Nullable(inner)
                if matches!(inner.as_ref(), DataType::Number(NumberDataType::UInt64)) =>
            {
                let column = entry.to_column();
                let value = column.into_nullable().unwrap();
                Some(value.validity)
            }
            _ => None,
        };
        let non_null_count = batch.indices.len();

        if non_null_count == num_rows {
            let fetched_block = take_fetched_rows(pending.fetched, &batch.indices)?;

            for mut entry in fetched_block.take_columns() {
                if self.need_wrap_nullable {
                    entry = wrap_true_validity(&entry, num_rows);
                }

                data.add_entry(entry);
            }
        } else {
            let validity =
                row_id_validity.expect("Row id validity should be available for null handling");

            if non_null_count == 0 {
                for data_type in &self.fetched_data_types {
                    let nullable_type = data_type.wrap_nullable();
                    let mut builder = ColumnBuilder::with_capacity(&nullable_type, num_rows);
                    for _ in 0..num_rows {
                        builder.push_default();
                    }
                    data.add_entry(builder.build().into());
                }
            } else {
                let fetched_block = take_fetched_rows(pending.fetched, &batch.indices)?;
                for entry in fetched_block.take_columns() {
                    let column = entry.to_column();
                    let mut fetched_iter = column.iter();
                    let nullable_type = entry.data_type().wrap_nullable();
                    let mut builder = ColumnBuilder::with_capacity(&nullable_type, num_rows);
                    for is_valid in validity.iter() {
                        if is_valid {
                            let scalar = fetched_iter.next().unwrap();
                            builder.push(scalar);
                        } else {
                            builder.push_default();
                        }
                    }
                    data.add_entry(builder.build().into());
                }
            }
        }

        log::info!(
            "Row fetch flush: num_rows: {}, num_bytes: {}, input blocks: {} in {} milliseconds.",
            num_rows,
            data.memory_size(),
            num_blocks,
            batch.start.elapsed().as_millis()
        );

        Ok(data)
    }
}

/// The rows of a batch, from the parts they were fetched into.
fn take_fetched_rows(
    mut fetched: Vec<(u32, DataBlock)>,
    indices: &[(u32, u32)],
) -> Result<DataBlock> {
    fetched.sort_unstable_by_key(|(part, _)| *part);
    let blocks = fetched
        .into_iter()
        .map(|(_, block)| block)
        .collect::<Vec<_>>();

    // check if row index is in valid bounds cause we don't ensure rowid is valid
    for (block_idx, row_idx) in indices {
        if *block_idx as usize >= blocks.len()
            || *row_idx as usize >= blocks[*block_idx as usize].num_rows()
        {
            return Err(ErrorCode::Internal(format!(
                "RowID is invalid, block idx {block_idx}, row idx {row_idx}, blocks len {}, block idx len {:?}",
                blocks.len(),
                blocks.get(*block_idx as usize).map(|b| b.num_rows()),
            )));
        }
    }

    Ok(DataBlock::take_blocks(&blocks, indices))
}

fn wrap_true_validity(column: &BlockEntry, num_rows: usize) -> BlockEntry {
    let (value, data_type) = (&column.value(), &column.data_type());
    let col = value.convert_to_full_column(data_type, num_rows);
    if matches!(col, Column::Null { .. }) || col.as_nullable().is_some() {
        column.clone()
    } else {
        NullableColumn::new_column(col, Bitmap::new_trued(num_rows)).into()
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::UInt64Type;

    use super::*;

    fn int32s(block: &DataBlock, column: usize) -> Vec<i32> {
        block.columns()[column]
            .to_column()
            .into_number()
            .unwrap()
            .into_int32()
            .unwrap()
            .to_vec()
    }

    fn batch(
        id: u64,
        row_ids: Vec<u64>,
        indices: Vec<(u32, u32)>,
        parts: usize,
    ) -> Arc<RowFetchBatch> {
        let inputs = vec![DataBlock::new_from_columns(vec![UInt64Type::from_data(
            row_ids,
        )])];
        Arc::new(RowFetchBatch {
            id,
            inputs,
            indices,
            parts,
            start: Instant::now(),
        })
    }

    fn fetched(batch: &Arc<RowFetchBatch>, part: u32, values: Vec<i32>) -> DataBlock {
        DataBlock::empty_with_meta(Box::new(RowFetched {
            batch: batch.clone(),
            block: Some((
                part,
                DataBlock::new_from_columns(vec![Int32Type::from_data(values)]),
            )),
        }))
    }

    fn assemble() -> TransformRowFetchAssemble {
        TransformRowFetchAssemble::create(0, false, vec![DataType::Number(NumberDataType::Int32)])
    }

    #[test]
    fn test_assemble_restores_batches_and_rows() -> Result<()> {
        let mut assemble = assemble();
        let first = batch(0, vec![1, 2, 3, 4], vec![(1, 1), (0, 0), (1, 1), (1, 0)], 2);
        let second = batch(1, vec![10], vec![(0, 0)], 1);

        // The second batch completes first, it waits for the first one.
        assert!(assemble.transform(fetched(&second, 0, vec![7]))?.is_empty());

        // The parts of the first batch arrive out of order. Row 0 and row 2 of the
        // batch are the same fetched row.
        assert!(
            assemble
                .transform(fetched(&first, 1, vec![20, 30]))?
                .is_empty()
        );
        let output = assemble.transform(fetched(&first, 0, vec![40]))?;

        assert_eq!(output.len(), 2);
        assert_eq!(int32s(&output[0], 1), vec![30, 40, 30, 20]);
        assert_eq!(int32s(&output[1], 1), vec![7]);
        assert!(assemble.on_finish(true)?.is_empty());
        Ok(())
    }

    #[test]
    fn test_assemble_batch_without_row_ids() -> Result<()> {
        let mut assemble = TransformRowFetchAssemble::create(0, false, vec![DataType::Number(
            NumberDataType::Int32,
        )]);
        let null_row_ids =
            NullableColumn::new_column(UInt64Type::from_data(vec![0u64, 0]), Bitmap::new_zeroed(2));
        let batch = Arc::new(RowFetchBatch {
            id: 0,
            inputs: vec![DataBlock::new_from_columns(vec![null_row_ids])],
            indices: vec![],
            parts: 0,
            start: Instant::now(),
        });

        let output = assemble.transform(DataBlock::empty_with_meta(Box::new(RowFetched {
            batch,
            block: None,
        })))?;

        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 2);
        assert_eq!(output[0].num_columns(), 2);
        Ok(())
    }

    #[test]
    fn test_assemble_rejects_incomplete_batch_on_finish() -> Result<()> {
        let mut assemble = assemble();
        let batch = batch(0, vec![1, 2], vec![(0, 0), (1, 0)], 2);
        assert!(assemble.transform(fetched(&batch, 0, vec![1]))?.is_empty());

        assert!(assemble.on_finish(true).is_err());
        // Downstream stopped reading: nothing is lost.
        assert!(assemble.on_finish(false)?.is_empty());
        Ok(())
    }

    #[test]
    fn test_assemble_rejects_row_out_of_fetched_block() {
        let mut assemble = assemble();
        let batch = batch(0, vec![1], vec![(0, 1)], 1);

        let result = assemble.transform(fetched(&batch, 0, vec![1]));
        assert!(result.err().unwrap().message().contains("RowID is invalid"));
    }
}
