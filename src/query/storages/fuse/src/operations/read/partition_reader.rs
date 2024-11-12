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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use async_channel::Receiver;
use databend_common_arrow::arrow::datatypes::Field;
use databend_common_arrow::arrow::datatypes::Schema;
use databend_common_arrow::native::read::NativeColumnsReader;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::StealablePartitions;
use databend_common_catalog::runtime_filter_info::HashJoinProbeStatistics;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::FieldIndex;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_sql::field_default_value;
use databend_common_sql::IndexType;
use databend_common_storage::ColumnNode;
use databend_common_storage::ColumnNodes;
use databend_storages_common_io::ReadSettings;
use opendal::Operator;
use xorf::BinaryFuse16;

use crate::fuse_part::FuseBlockPartInfo;
use crate::io::read::inner_project_field_default_values;
use crate::io::BlockReader;
use crate::operations::read::runtime_filter_prunner::runtime_filter_pruner;
use crate::operations::read::PartitionScanMeta;
use crate::operations::read::SourceBlockReader;
use crate::operations::read::SourceReader;

#[derive(Copy, Clone, Debug)]
enum Step {
    Sync(SyncStep),
    Async(AsyncStep),
    Finish,
}

#[derive(Copy, Clone, Debug)]
enum SyncStep {
    SyncRead,
}

#[derive(Copy, Clone, Debug)]
enum AsyncStep {
    WaitTask,
    AsyncRead,
}

pub struct PartitionReader {
    // Processor state.
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    is_finished: bool,
    step: Step,

    // Read source related variables.
    id: usize,
    ctx: Arc<dyn TableContext>,
    func_ctx: FunctionContext,
    table_index: IndexType,
    schema: Arc<TableSchema>,
    batch_size: usize,
    is_blocking_io: bool,
    read_settings: ReadSettings,
    read_task: Option<DataBlock>,
    stealable_partitions: StealablePartitions,

    // Block reader related variables.
    operator: Operator,
    bloom_filter_columns: Vec<(usize, String)>,
    bloom_filter_statistics: Vec<(String, Arc<HashJoinProbeStatistics>)>,
    is_bloom_filter_statistics_fetched: bool,
    column_indices: Vec<usize>,
    query_internal_columns: bool,
    update_stream_columns: bool,
    put_cache: bool,

    // Sync with deserializer processors.
    partition_scan_state: Arc<PartitionScanState>,
    read_tasks: Receiver<Arc<DataBlock>>,
}

impl PartitionReader {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        output: Arc<OutputPort>,
        id: usize,
        ctx: Arc<dyn TableContext>,
        table_index: IndexType,
        schema: Arc<TableSchema>,
        read_settings: ReadSettings,
        stealable_partitions: StealablePartitions,
        bloom_filter_columns: Vec<(usize, String)>,
        column_indices: Vec<usize>,
        partition_scan_state: Arc<PartitionScanState>,
        read_tasks: Receiver<Arc<DataBlock>>,
        operator: Operator,
        query_internal_columns: bool,
        update_stream_columns: bool,
        put_cache: bool,
    ) -> Result<ProcessorPtr> {
        let is_blocking_io = operator.info().native_capability().blocking;
        let (batch_size, step) = if is_blocking_io {
            (1, Step::Sync(SyncStep::SyncRead))
        } else {
            (
                ctx.get_settings().get_storage_fetch_part_num()? as usize,
                Step::Async(AsyncStep::AsyncRead),
            )
        };
        let func_ctx = ctx.get_function_context()?;
        Ok(ProcessorPtr::create(Box::new(PartitionReader {
            output,
            output_data: None,
            is_finished: false,
            step,
            id,
            ctx,
            func_ctx,
            table_index,
            schema,
            batch_size,
            is_blocking_io,
            read_settings,
            read_task: None,
            stealable_partitions,
            bloom_filter_columns,
            bloom_filter_statistics: vec![],
            is_bloom_filter_statistics_fetched: false,
            column_indices,
            partition_scan_state,
            read_tasks,
            operator,
            query_internal_columns,
            update_stream_columns,
            put_cache,
        })))
    }

    fn next_step(&mut self, step: Step) -> Result<Event> {
        let event = match step {
            Step::Sync(_) => Event::Sync,
            Step::Async(_) => Event::Async,
            Step::Finish => {
                self.read_tasks.close();
                self.output.finish();
                Event::Finished
            }
        };
        self.step = step;
        Ok(event)
    }

    fn output_data_block(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(read_result) = self.output_data.take() {
            self.output.push_data(Ok(read_result));
        }

        if self.is_blocking_io {
            self.next_step(Step::Async(AsyncStep::WaitTask))
        } else {
            self.next_step(Step::Async(AsyncStep::AsyncRead))
        }
    }

    async fn read_task(&mut self) -> Result<Option<DataBlock>> {
        if let Ok(task) = self.read_tasks.try_recv() {
            return Ok(Some(task.as_ref().clone()));
        }

        let partitions = self.steal_partitions()?;
        if !partitions.is_empty() {
            let source_reader = self.create_source_reader()?;
            return Ok(Some(DataBlock::empty_with_meta(PartitionScanMeta::create(
                partitions,
                source_reader,
            ))));
        } else if self.partition_scan_state.num_readded_partitions() == 0 {
            self.is_finished = true;
            return Ok(None);
        }

        if self.partition_scan_state.num_readded_partitions()
            == self.partition_scan_state.num_deserialized_partitions()
        {
            self.is_finished = true;
            return Ok(None);
        }
        match self.read_tasks.recv().await {
            Ok(task) => Ok(Some(task.as_ref().clone())),
            Err(_) => {
                self.is_finished = true;
                Ok(None)
            }
        }
    }

    fn steal_partitions(&mut self) -> Result<Vec<PartInfoPtr>> {
        let _guard = self.partition_scan_state.mutex.lock().unwrap();
        let mut partitions = Vec::with_capacity(self.batch_size);
        while partitions.len() < self.batch_size {
            let parts = self
                .stealable_partitions
                .steal(self.id, self.batch_size - partitions.len());

            let Some(parts) = parts else {
                self.partition_scan_state.set_stealable_partitions_empty();
                break;
            };

            for part in parts.into_iter() {
                let mut filters = self
                    .stealable_partitions
                    .ctx
                    .get_inlist_runtime_filter_with_id(self.table_index);
                filters.extend(
                    self.stealable_partitions
                        .ctx
                        .get_min_max_runtime_filter_with_id(self.table_index),
                );

                if !runtime_filter_pruner(self.schema.clone(), &part, &filters, &self.func_ctx)? {
                    partitions.push(part);
                };
            }
        }
        self.partition_scan_state
            .inc_num_readded_partitions(partitions.len());
        Ok(partitions)
    }

    fn sync_read(
        &self,
        mut meta: PartitionScanMeta,
        source_block_reader: SourceBlockReader,
    ) -> Result<DataBlock> {
        let data_block = match source_block_reader {
            SourceBlockReader::Parquet { block_reader, .. } => {
                for partition in meta.partitions.iter() {
                    let merge_io_read_result = block_reader.sync_read_columns_data_by_merge_io(
                        &self.read_settings,
                        partition,
                        &None,
                    )?;
                    meta.io_results.push_back(merge_io_read_result);
                }
                DataBlock::empty_with_meta(Box::new(meta))
            }
        };
        Ok(data_block)
    }

    async fn async_read(
        &self,
        mut meta: PartitionScanMeta,
        source_block_reader: SourceBlockReader,
    ) -> Result<DataBlock> {
        let data_block = match source_block_reader {
            SourceBlockReader::Parquet { block_reader, .. } => {
                let mut block_read_results = Vec::with_capacity(meta.partitions.len());
                for partition in meta.partitions.iter() {
                    let reader = block_reader.clone();
                    let settings = ReadSettings::from_ctx(&self.stealable_partitions.ctx)?;
                    let partition = partition.clone();
                    block_read_results.push(async move {
                        databend_common_base::runtime::spawn(async move {
                            let part = FuseBlockPartInfo::from_part(&partition)?;
                            reader
                                .read_columns_data_by_merge_io(
                                    &settings,
                                    &part.location,
                                    &part.columns_meta,
                                    &None,
                                )
                                .await
                        })
                        .await
                        .unwrap()
                    });
                }

                meta.io_results
                    .extend(futures::future::try_join_all(block_read_results).await?);
                DataBlock::empty_with_meta(Box::new(meta))
            }
        };
        Ok(data_block)
    }

    fn fetch_bloom_filters(&self) -> Vec<Option<Arc<BinaryFuse16>>> {
        let mut bloom_filters = self.ctx.get_bloom_runtime_filter_with_id(self.table_index);
        bloom_filters.sort_by(|a, b| {
            let a_pos = self
                .bloom_filter_statistics
                .iter()
                .position(|(name, _)| name == &a.0)
                .unwrap_or(usize::MAX);
            let b_pos = self
                .bloom_filter_statistics
                .iter()
                .position(|(name, _)| name == &b.0)
                .unwrap_or(usize::MAX);
            a_pos.cmp(&b_pos)
        });

        self.bloom_filter_statistics
            .iter()
            .map(|(name, _)| {
                bloom_filters
                    .iter()
                    .find(|(filter_name, _)| filter_name == name)
                    .map(|(_, filter)| filter.clone())
            })
            .collect()
    }

    fn create_source_reader(&self) -> Result<SourceReader> {
        let bloom_filters = self.fetch_bloom_filters();
        let mut column_indices = self.column_indices.clone();
        let mut column_positions = Vec::with_capacity(column_indices.len());
        let mut filter_readers = Vec::new();
        let block_reader = self.create_block_reader(Projection::Columns(column_indices.clone()))?;

        for ((column_name, hash_join_probe_statistics), bloom_filter) in self
            .bloom_filter_statistics
            .iter()
            .zip(bloom_filters.into_iter())
        {
            if !hash_join_probe_statistics.prefer_runtime_filter() {
                continue;
            }

            let Some(bloom_filter) = bloom_filter else {
                continue;
            };

            let position = self.schema.index_of(column_name)?;
            let block_reader = self.create_block_reader(Projection::Columns(vec![position]))?;
            column_positions.push(position);
            filter_readers.push((block_reader, bloom_filter));
            column_indices = column_indices
                .into_iter()
                .filter(|&x| x != position)
                .collect::<Vec<_>>();
        }

        let remaining_reader = if !filter_readers.is_empty() && !column_indices.is_empty() {
            for column_index in column_indices.iter() {
                column_positions.push(*column_index);
            }
            let remain_column_projection = Projection::Columns(column_indices);
            Some(self.create_block_reader(remain_column_projection)?)
        } else {
            None
        };

        Ok(SourceReader::Parquet {
            block_reader,
            filter_readers,
            remaining_reader,
            column_positions,
        })
    }

    pub fn create_block_reader(&self, projection: Projection) -> Result<Arc<BlockReader>> {
        // init projected_schema and default_vals of schema.fields
        let (projected_schema, default_vals) = match projection {
            Projection::Columns(ref indices) => {
                let projected_schema = TableSchemaRef::new(self.schema.project(indices));
                // If projection by Columns, just calc default values by projected fields.
                let mut default_vals = Vec::with_capacity(projected_schema.fields().len());
                for field in projected_schema.fields() {
                    let default_val = field_default_value(self.ctx.clone(), field)?;
                    default_vals.push(default_val);
                }

                (projected_schema, default_vals)
            }
            Projection::InnerColumns(ref path_indices) => {
                let projected_schema = TableSchemaRef::new(self.schema.inner_project(path_indices));
                let mut field_default_vals = Vec::with_capacity(self.schema.fields().len());

                // If projection by InnerColumns, first calc default value of all schema fields.
                for field in self.schema.fields() {
                    field_default_vals.push(field_default_value(self.ctx.clone(), field)?);
                }

                // Then calc project scalars by path_indices
                let mut default_vals = Vec::with_capacity(self.schema.fields().len());
                path_indices.values().for_each(|path| {
                    default_vals.push(
                        inner_project_field_default_values(&field_default_vals, path).unwrap(),
                    );
                });

                (projected_schema, default_vals)
            }
        };

        let arrow_schema: Schema = self.schema.as_ref().into();
        let native_columns_reader = NativeColumnsReader::new(arrow_schema.clone())?;
        let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&self.schema));

        let project_column_nodes: Vec<ColumnNode> = projection
            .project_column_nodes(&column_nodes)?
            .iter()
            .map(|c| (*c).clone())
            .collect();
        let project_indices = Self::build_projection_indices(&project_column_nodes);

        Ok(Arc::new(BlockReader {
            ctx: self.ctx.clone(),
            operator: self.operator.clone(),
            projection,
            projected_schema,
            project_indices,
            project_column_nodes,
            default_vals,
            query_internal_columns: self.query_internal_columns,
            update_stream_columns: self.update_stream_columns,
            put_cache: self.put_cache,
            original_schema: self.schema.clone(),
            native_columns_reader,
        }))
    }

    // Build non duplicate leaf_indices to avoid repeated read column from parquet
    pub(crate) fn build_projection_indices(
        columns: &[ColumnNode],
    ) -> BTreeMap<FieldIndex, (ColumnId, Field, DataType)> {
        let mut indices = BTreeMap::new();
        for column in columns {
            for (i, index) in column.leaf_indices.iter().enumerate() {
                let f = DataField::try_from(&column.field).unwrap();
                indices.insert(
                    *index,
                    (
                        column.leaf_column_ids[i],
                        column.field.clone(),
                        f.data_type().clone(),
                    ),
                );
            }
        }
        indices
    }

    fn fetche_bloom_filter_statistics(&mut self) {
        for (hash_join_id, column_name) in self.bloom_filter_columns.iter() {
            let statistics = self.ctx.get_hash_join_probe_statistics(*hash_join_id);
            self.bloom_filter_statistics
                .push((column_name.clone(), statistics))
        }
        self.is_bloom_filter_statistics_fetched = true;
    }
}

#[async_trait::async_trait]
impl Processor for PartitionReader {
    fn name(&self) -> String {
        String::from("PartitionReader")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.is_finished {
            return self.next_step(Step::Finish);
        }
        if !self.is_bloom_filter_statistics_fetched {
            self.fetche_bloom_filter_statistics();
        }
        match self.step {
            Step::Sync(step) => match step {
                SyncStep::SyncRead => self.output_data_block(),
            },
            Step::Async(step) => match step {
                AsyncStep::AsyncRead => self.output_data_block(),
                AsyncStep::WaitTask => self.next_step(Step::Sync(SyncStep::SyncRead)),
            },
            Step::Finish => self.next_step(Step::Finish),
        }
    }

    fn process(&mut self) -> Result<()> {
        let Some(mut read_task) = self.read_task.take() else {
            return Ok(());
        };

        if let Some(meta) = read_task.take_meta() {
            if let Some(mut meta) = PartitionScanMeta::downcast_from(meta) {
                meta.reader_state = meta.reader_state.next_reader_state(&meta.source_reader);
                let source_block_reader =
                    meta.source_reader.source_block_reader(&meta.reader_state);
                self.output_data = Some(self.sync_read(meta, source_block_reader)?);
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match self.step {
            Step::Async(AsyncStep::WaitTask) => {
                self.read_task = self.read_task().await?;
            }
            Step::Async(AsyncStep::AsyncRead) => {
                let Some(mut read_task) = self.read_task().await?.take() else {
                    return Ok(());
                };

                if let Some(meta) = read_task.take_meta() {
                    if let Some(mut meta) = PartitionScanMeta::downcast_from(meta) {
                        meta.reader_state =
                            meta.reader_state.next_reader_state(&meta.source_reader);
                        let source_block_reader =
                            meta.source_reader.source_block_reader(&meta.reader_state);
                        self.output_data = Some(self.async_read(meta, source_block_reader).await?);
                    }
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct PartitionScanState {
    mutex: Mutex<()>,
    is_stealable_partitions_empty: AtomicBool,
    num_readded_partitions: AtomicUsize,
    num_deserialized_partitions: AtomicUsize,
}

impl PartitionScanState {
    pub fn new() -> Self {
        Self {
            mutex: Mutex::new(()),
            is_stealable_partitions_empty: AtomicBool::new(false),
            num_readded_partitions: AtomicUsize::new(0),
            num_deserialized_partitions: AtomicUsize::new(0),
        }
    }

    pub fn is_stealable_partitions_empty(&self) -> bool {
        self.is_stealable_partitions_empty.load(Ordering::Acquire)
    }

    pub fn num_readded_partitions(&self) -> usize {
        self.num_readded_partitions.load(Ordering::Acquire)
    }

    pub fn num_deserialized_partitions(&self) -> usize {
        self.num_deserialized_partitions.load(Ordering::Acquire)
    }

    pub fn set_stealable_partitions_empty(&self) {
        self.is_stealable_partitions_empty
            .store(true, Ordering::Release);
    }

    pub fn inc_num_readded_partitions(&self, num: usize) {
        self.num_readded_partitions.fetch_add(num, Ordering::AcqRel);
    }

    pub fn inc_num_deserialized_partitions(&self, num: usize) {
        self.num_deserialized_partitions
            .fetch_add(num, Ordering::AcqRel);
    }

    pub fn finished(&self, num: usize) -> bool {
        let num_deserialized_partitions = self
            .num_deserialized_partitions
            .fetch_add(num, Ordering::AcqRel);
        self.is_stealable_partitions_empty()
            && self.num_readded_partitions() == num_deserialized_partitions + num
    }
}
