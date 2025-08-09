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
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_channel::Receiver;
use async_channel::Sender;
use async_trait::async_trait;
use databend_common_base::base::tokio::sync::Semaphore;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_io::prelude::borsh_deserialize_from_slice;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sinks::AsyncSink;
use databend_common_pipeline_sinks::AsyncSinker;
use databend_common_storage::Datum;
use databend_common_storage::Histogram;
use databend_common_storage::HistogramBucket;
use databend_common_storage::MetaHLL;
use databend_common_storage::MetaHLL12;
use databend_storages_common_cache::BlockMeta;
use databend_storages_common_cache::CompactSegmentInfo;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_cache::SegmentStatistics;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::decode_column_hll;
use databend_storages_common_table_meta::meta::encode_column_hll;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use databend_storages_common_table_meta::meta::Versioned;
use opendal::Operator;

use crate::io::build_column_hlls;
use crate::io::read::meta::SegmentStatsReader;
use crate::io::BlockReader;
use crate::io::CachedMetaWriter;
use crate::io::CompactSegmentInfoReader;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::operations::acquire_task_permit;
use crate::statistics::reduce_block_statistics;
use crate::statistics::reduce_cluster_statistics;
use crate::FuseLazyPartInfo;
use crate::FuseStorageFormat;
use crate::FuseTable;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
enum AnalyzeStep {
    CollectNDV,
    CollectHistogram,
    CommitStatistics,
}

impl FuseTable {
    #[allow(clippy::too_many_arguments)]
    pub fn do_analyze(
        ctx: Arc<dyn TableContext>,
        output_schema: Arc<DataSchema>,
        catalog: &str,
        database: &str,
        table: &str,
        snapshot_id: SnapshotId,
        pipeline: &mut Pipeline,
        histogram_info_receivers: HashMap<u32, Receiver<DataBlock>>,
    ) -> Result<()> {
        pipeline.add_sink(|input| {
            SinkAnalyzeState::create(
                ctx.clone(),
                output_schema.clone(),
                catalog,
                database,
                table,
                snapshot_id,
                input,
                histogram_info_receivers.clone(),
            )
        })?;
        Ok(())
    }
}

struct SinkAnalyzeState {
    ctx: Arc<dyn TableContext>,
    output_schema: Arc<DataSchema>,
    input_port: Arc<InputPort>,

    catalog: String,
    database: String,
    table: String,
    snapshot_id: SnapshotId,
    histogram_info_receivers: HashMap<u32, Receiver<DataBlock>>,
    input_data: Option<DataBlock>,
    committed: bool,
    ndv_states: HashMap<ColumnId, MetaHLL12>,
    histograms: HashMap<ColumnId, Histogram>,
    step: AnalyzeStep,
}

impl SinkAnalyzeState {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output_schema: Arc<DataSchema>,
        catalog: &str,
        database: &str,
        table: &str,
        snapshot_id: SnapshotId,
        input: Arc<InputPort>,
        histogram_info_receivers: HashMap<u32, Receiver<DataBlock>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(SinkAnalyzeState {
            ctx,
            output_schema,
            input_port: input,
            catalog: catalog.to_string(),
            database: database.to_string(),
            table: table.to_string(),
            snapshot_id,
            histogram_info_receivers,
            input_data: None,
            committed: false,
            ndv_states: Default::default(),
            histograms: Default::default(),
            step: AnalyzeStep::CollectNDV,
        })))
    }

    async fn get_table(&self) -> Result<Arc<dyn Table>> {
        // always use the latest table
        let tenant = self.ctx.get_tenant();
        let catalog = CatalogManager::instance()
            .get_catalog(
                tenant.tenant_name(),
                &self.catalog,
                self.ctx.session_state(),
            )
            .await?;
        let table = catalog
            .get_table(&tenant, &self.database, &self.table)
            .await?;
        Ok(table)
    }

    #[async_backtrace::framed]
    pub async fn merge_analyze_states(&mut self, data_block: DataBlock) -> Result<()> {
        if data_block.num_rows() == 0 {
            return Ok(());
        }
        let table = self.get_table().await?;
        let table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot = table.read_table_snapshot().await?;

        if snapshot.is_none() {
            return Ok(());
        }
        let table_statistics = table
            .read_table_snapshot_statistics(snapshot.as_ref())
            .await?;

        let is_full = data_block.columns()[self.output_schema.num_fields() - 1]
            .index(0)
            .unwrap();

        let is_full = is_full.as_boolean().unwrap();

        let mut ndv_states = table_statistics.map(|s| s.hll.clone()).unwrap_or_default();

        let index_num = self.output_schema.num_fields() - 1;

        for (f, col) in self
            .output_schema
            .fields()
            .iter()
            .take(index_num)
            .zip(data_block.columns())
        {
            let name = f.name();
            let index: u32 = name.strip_prefix("ndv_").unwrap().parse().unwrap();

            let col = col.index(0).unwrap();
            let data = col.as_tuple().unwrap()[0].as_binary().unwrap();
            let hll: MetaHLL12 = borsh_deserialize_from_slice(data)?;

            if !is_full {
                ndv_states
                    .entry(index)
                    .and_modify(|c| c.merge(&hll))
                    .or_insert(hll);
            } else {
                ndv_states.insert(index, hll);
            }
        }

        self.ndv_states = ndv_states;
        Ok(())
    }

    #[async_backtrace::framed]
    async fn create_histogram(&mut self, col_id: u32, data_block: DataBlock) -> Result<()> {
        if data_block.num_rows() == 0 {
            return Ok(());
        }
        for row in 0..data_block.num_rows() {
            let column = &data_block.columns()[1];
            let value = column.index(row).clone().unwrap();
            let number = value.as_number().unwrap();
            let ndv = number.as_u_int64().unwrap();
            let upper_bound =
                Datum::from_scalar(data_block.get_by_offset(2).index(row).unwrap().to_owned())
                    .ok_or_else(|| {
                        ErrorCode::Internal("Don't support the type to generate histogram")
                    })?;
            let lower_bound =
                Datum::from_scalar(data_block.get_by_offset(3).index(row).unwrap().to_owned())
                    .ok_or_else(|| {
                        ErrorCode::Internal("Don't support the type to generate histogram")
                    })?;

            let count: Option<_> = try {
                *data_block
                    .get_by_offset(4)
                    .index(row)?
                    .as_number()?
                    .as_u_int64()?
            };

            let bucket =
                HistogramBucket::new(lower_bound, upper_bound, count.unwrap() as f64, *ndv as f64);
            self.histograms
                .entry(col_id)
                .and_modify(|histogram| histogram.add_bucket(bucket.clone()))
                .or_insert(Histogram::new(vec![bucket], true));
        }
        Ok(())
    }

    async fn commit_statistics(&self) -> Result<()> {
        let table = self.get_table().await?;
        let table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot = table.read_table_snapshot().await?;
        if snapshot.is_none() {
            return Ok(());
        }
        let snapshot = snapshot.unwrap();
        // 3. Generate new table statistics
        let table_statistics = TableSnapshotStatistics::new(
            self.ndv_states.clone(),
            self.histograms.clone(),
            self.snapshot_id,
            snapshot.summary.row_count,
        );
        let table_statistics_location = table
            .meta_location_generator
            .snapshot_statistics_location_from_uuid(
                &table_statistics.snapshot_id,
                table_statistics.format_version(),
            )?;

        let (col_stats, cluster_stats) =
            regenerate_statistics(table, snapshot.as_ref(), &self.ctx).await?;
        // 4. Save table statistics
        let mut new_snapshot = TableSnapshot::try_from_previous(
            snapshot.clone(),
            Some(table.get_table_info().ident.seq),
            self.ctx
                .get_table_meta_timestamps(table, Some(snapshot.clone()))?,
        )?;
        new_snapshot.summary.col_stats = col_stats;
        new_snapshot.summary.cluster_stats = cluster_stats;
        new_snapshot.table_statistics_location = Some(table_statistics_location);
        table
            .commit_to_meta_server(
                self.ctx.as_ref(),
                &table.table_info,
                &table.meta_location_generator,
                new_snapshot,
                Some(table_statistics),
                &None,
                &table.operator,
            )
            .await
    }
}

#[async_trait::async_trait]
impl Processor for SinkAnalyzeState {
    fn name(&self) -> String {
        "SinkAnalyzeState".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.input_data.is_some() {
            if !self.input_port.has_data() {
                self.input_port.set_need_data();
            }
            return Ok(Event::Async);
        }

        if self.input_port.is_finished() {
            match self.step {
                AnalyzeStep::CollectNDV => {
                    self.step = AnalyzeStep::CollectHistogram;
                    return Ok(Event::Async);
                }
                AnalyzeStep::CollectHistogram => {
                    return Ok(Event::Async);
                }
                AnalyzeStep::CommitStatistics => {
                    if self.committed {
                        return Ok(Event::Finished);
                    } else {
                        return Ok(Event::Async);
                    }
                }
            }
        }

        if self.input_port.has_data() {
            self.input_data = Some(self.input_port.pull_data().unwrap()?);
            return Ok(Event::Async);
        }

        self.input_port.set_need_data();
        Ok(Event::NeedData)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match self.step {
            AnalyzeStep::CollectNDV => {
                if let Some(data_block) = self.input_data.take() {
                    self.merge_analyze_states(data_block.clone()).await?;
                }
            }
            AnalyzeStep::CollectHistogram => {
                let mut finished_count = 0;
                let receivers = self.histogram_info_receivers.clone();
                for (id, receiver) in receivers.iter() {
                    if let Ok(res) = receiver.recv().await {
                        self.create_histogram(*id, res).await?;
                    } else {
                        finished_count += 1;
                    }
                }
                if finished_count == self.histogram_info_receivers.len() {
                    self.step = AnalyzeStep::CommitStatistics;
                }
            }
            AnalyzeStep::CommitStatistics => {
                let mismatch_code = ErrorCode::TABLE_VERSION_MISMATCHED;
                loop {
                    if let Err(e) = self.commit_statistics().await {
                        if e.code() == mismatch_code {
                            log::warn!("Retry after got TableVersionMismatched");
                            continue;
                        } else {
                            return Err(e);
                        }
                    }
                    break;
                }
                self.committed = true;
            }
        }
        return Ok(());
    }
}

pub async fn regenerate_statistics(
    table: &FuseTable,
    snapshot: &TableSnapshot,
    ctx: &Arc<dyn TableContext>,
) -> Result<(StatisticsOfColumns, Option<ClusterStatistics>)> {
    // 1. Read table snapshot.
    let default_cluster_key_id = table.cluster_key_id();

    // 2. Iterator segments and blocks to estimate statistics.
    let mut read_segment_count = 0;
    let mut col_stats = HashMap::new();
    let mut cluster_stats = None;

    let start = Instant::now();
    let segments_io = SegmentsIO::create(ctx.clone(), table.operator.clone(), table.schema());
    let chunk_size = ctx.get_settings().get_max_threads()? as usize * 4;
    let number_segments = snapshot.segments.len();
    for chunk in snapshot.segments.chunks(chunk_size) {
        let mut stats_of_columns = Vec::new();
        let mut blocks_cluster_stats = Vec::new();
        if !col_stats.is_empty() {
            stats_of_columns.push(col_stats.clone());
            blocks_cluster_stats.push(cluster_stats.clone());
        }

        let segments = segments_io
            .read_segments::<SegmentInfo>(chunk, true)
            .await?;
        for segment in segments {
            let segment = segment?;
            stats_of_columns.push(segment.summary.col_stats.clone());
            blocks_cluster_stats.push(segment.summary.cluster_stats.clone());
        }

        // Generate new column statistics for snapshot
        col_stats = reduce_block_statistics(&stats_of_columns);
        cluster_stats = reduce_cluster_statistics(&blocks_cluster_stats, default_cluster_key_id);

        // Status.
        {
            read_segment_count += chunk.len();
            let status = format!(
                "analyze: read segment files:{}/{}, cost:{:?}",
                read_segment_count,
                number_segments,
                start.elapsed()
            );
            ctx.set_status_info(&status);
        }
    }

    Ok((col_stats, cluster_stats))
}

pub struct HistogramInfoSink {
    sender: Option<Sender<DataBlock>>,
}

impl HistogramInfoSink {
    pub fn create(tx: Option<Sender<DataBlock>>, input: Arc<InputPort>) -> Box<dyn Processor> {
        AsyncSinker::create(input, HistogramInfoSink { sender: tx })
    }
}

#[async_trait]
impl AsyncSink for HistogramInfoSink {
    const NAME: &'static str = "HistogramInfoSink";

    #[async_backtrace::framed]
    async fn on_finish(&mut self) -> Result<()> {
        drop(self.sender.take());
        Ok(())
    }

    #[async_backtrace::framed]
    async fn consume(&mut self, data_block: DataBlock) -> Result<bool> {
        if let Some(sender) = self.sender.as_ref() {
            if sender.send(data_block).await.is_err() {
                return Err(ErrorCode::Internal("HistogramInfoSink sender failed"));
            };
        }
        Ok(false)
    }
}

struct SegmentWithHLL {
    location: Location,
    blocks: Vec<Arc<BlockMeta>>,
    summary: Statistics,
    origin_hlls: Vec<RawBlockHLL>,
    indexes: Vec<usize>,
    new_hlls: Vec<Option<BlockHLL>>,
}
enum State {
    ReadData(Option<PartInfoPtr>),
    CollectNDV {
        location: Location,
        info: Arc<CompactSegmentInfo>,
        hlls: Vec<RawBlockHLL>,
    },
    GenerateHLL,
    SyncGen,
    Write,
    Finish,
}

pub struct AnalyzeCollectNDVSource {
    state: State,
    output: Arc<OutputPort>,
    column_hlls: HashMap<ColumnId, MetaHLL>,
    row_count: u64,

    segment_with_hll: Option<SegmentWithHLL>,

    ctx: Arc<dyn TableContext>,
    block_reader: Arc<BlockReader>,
    dal: Operator,
    settings: ReadSettings,
    storage_format: FuseStorageFormat,
    segment_reader: CompactSegmentInfoReader,
    stats_reader: SegmentStatsReader,
    ndv_columns_map: BTreeMap<FieldIndex, TableField>,
}

#[async_trait::async_trait]
impl Processor for AnalyzeCollectNDVSource {
    fn name(&self) -> String {
        "AnalyzeCollectNDVSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::ReadData(None)) {
            self.state = self
                .ctx
                .get_partition()
                .map_or(State::Finish, |part| State::ReadData(Some(part)));
        }

        if matches!(self.state, State::Finish) {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }
        todo!()
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::CollectNDV {
                location,
                info,
                hlls,
            } => {
                let mut indexes = vec![];
                for (idx, data) in hlls.iter().enumerate() {
                    let block_hll = decode_column_hll(&data)?;
                    if let Some(column_hlls) = &block_hll {
                        for (column_id, column_hll) in self.column_hlls.iter_mut() {
                            if let Some(hll) = column_hlls.get(column_id) {
                                column_hll.merge(hll);
                            }
                        }
                    } else {
                        indexes.push(idx);
                    }
                }

                if !indexes.is_empty() {
                    assert!(self.segment_with_hll.is_none());
                    let new_hlls = Vec::with_capacity(indexes.len());
                    self.segment_with_hll = Some(SegmentWithHLL {
                        location,
                        blocks: info.block_metas()?,
                        summary: info.summary.clone(),
                        origin_hlls: hlls,
                        indexes,
                        new_hlls,
                    });
                    self.state = State::GenerateHLL;
                } else {
                    self.state = State::ReadData(None);
                }
            }
            State::SyncGen => {
                let segment_with_hll = self.segment_with_hll.as_mut().unwrap();
                let new_hlls = std::mem::take(&mut segment_with_hll.new_hlls);
                for (idx, new) in new_hlls.into_iter().enumerate() {
                    if let Some(column_hlls) = new {
                        for (column_id, column_hll) in self.column_hlls.iter_mut() {
                            if let Some(hll) = column_hlls.get(column_id) {
                                column_hll.merge(hll);
                            }
                        }
                        segment_with_hll.origin_hlls[idx] = encode_column_hll(&column_hlls)?;
                    }
                }
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadData(Some(part)) => {
                let part = FuseLazyPartInfo::from_part(&part)?;
                let (path, ver) = &part.segment_location;
                if *ver < 2 {
                    self.state = State::ReadData(None);
                    return Ok(());
                }
                let load_param = LoadParams {
                    location: path.clone(),
                    len_hint: None,
                    ver: *ver,
                    put_cache: true,
                };
                let compact_segment_info = self.segment_reader.read(&load_param).await?;

                let block_count = compact_segment_info.summary.block_count as usize;
                let block_hlls =
                    if let Some(meta) = &compact_segment_info.summary.additional_stats_meta {
                        let (path, ver) = &meta.location;
                        let load_param = LoadParams {
                            location: path.clone(),
                            len_hint: None,
                            ver: *ver,
                            put_cache: true,
                        };
                        let stats = self.stats_reader.read(&load_param).await?;
                        stats.block_hlls.clone()
                    } else {
                        vec![vec![]; block_count]
                    };
                self.state = State::CollectNDV {
                    location: part.segment_location.clone(),
                    info: compact_segment_info,
                    hlls: block_hlls,
                };
            }
            State::GenerateHLL => {
                let segment_with_hll = self.segment_with_hll.as_mut().unwrap();
                let max_threads = self.ctx.get_settings().get_max_threads()? as usize;
                let max_concurrency = std::cmp::max(max_threads * 2, 10);
                let semaphore = Arc::new(Semaphore::new(max_concurrency));
                let runtime = GlobalIORuntime::instance();
                let mut handlers = Vec::with_capacity(segment_with_hll.indexes.len());
                for &idx in &segment_with_hll.indexes {
                    let permit = acquire_task_permit(semaphore.clone()).await?;
                    let block_reader = self.block_reader.clone();
                    let settings = self.settings.clone();
                    let storage_format = self.storage_format.clone();
                    let block_meta = segment_with_hll.blocks[idx].clone();
                    let ndv_columns_map = self.ndv_columns_map.clone();
                    let handler = runtime.spawn(async move {
                        let block = block_reader
                            .read_by_meta(&settings, &block_meta, &storage_format)
                            .await?;
                        let column_hlls = build_column_hlls(&block, &ndv_columns_map)?;
                        drop(permit);
                        Ok::<_, ErrorCode>(column_hlls)
                    });
                    handlers.push(handler);
                }

                let joint = futures::future::try_join_all(handlers).await.map_err(|e| {
                    ErrorCode::StorageOther(format!(
                        "[BLOCK-COMPACT] Failed to deserialize segment blocks: {}",
                        e
                    ))
                })?;
                let new_hlls = joint.into_iter().collect::<Result<Vec<_>>>()?;
                if new_hlls.iter().all(|v| v.is_none()) {
                    self.segment_with_hll = None;
                    self.state = State::ReadData(None);
                } else {
                    segment_with_hll.new_hlls = new_hlls;
                    self.state = State::Write;
                }
            }
            State::Write => {
                let SegmentWithHLL {
                    location,
                    blocks,
                    mut summary,
                    origin_hlls,
                    ..
                } = std::mem::take(&mut self.segment_with_hll).unwrap();

                let segment_loc = location.0.as_str();
                let data = SegmentStatistics::new(origin_hlls).to_bytes()?;
                let segment_stats_location =
                    TableMetaLocationGenerator::gen_segment_stats_location_from_segment_location(
                        segment_loc,
                    );
                let additional_stats_meta = AdditionalStatsMeta {
                    size: data.len() as u64,
                    location: (segment_stats_location.clone(), SegmentStatistics::VERSION),
                };
                self.dal.write(&segment_stats_location, data).await?;
                summary.additional_stats_meta = Some(additional_stats_meta);
                let new_segment = SegmentInfo::new(blocks, summary);
                new_segment
                    .write_meta_through_cache(&self.dal, segment_loc)
                    .await?;
                self.state = State::ReadData(None);
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
