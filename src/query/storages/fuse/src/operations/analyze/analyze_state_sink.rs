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
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_channel::Receiver;
use backoff::backoff::Backoff;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_statistics::DEFAULT_HISTOGRAM_BUCKETS;
use databend_common_statistics::Datum;
use databend_common_statistics::Histogram;
use databend_common_statistics::HistogramBucket;
use databend_common_statistics::KllBucketBounds;
use databend_common_statistics::KllSketch;
use databend_common_storage::MetaHLL;
use databend_storages_common_cache::Partitions;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::BlockCountMinSketch;
use databend_storages_common_table_meta::meta::BlockTopN;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use databend_storages_common_table_meta::meta::column_oriented_segment::AbstractSegment;
use databend_storages_common_table_meta::meta::encode_column_hll;
use databend_storages_common_table_meta::meta::merge_column_count_min_sketch_mut;
use databend_storages_common_table_meta::meta::merge_column_top_n_mut;
use tokio::sync::Semaphore;

use crate::FuseLazyPartInfo;
use crate::FuseTable;
use crate::io::SegmentsIO;
use crate::operations::analyze::AnalyzeCollectHistogramInfo;
use crate::operations::analyze::AnalyzeCollectNDVSource;
use crate::operations::analyze::AnalyzeNDVMeta;
use crate::operations::util::set_backoff;
use crate::statistics::reduce_block_statistics;
use crate::statistics::reduce_cluster_statistics;
use crate::statistics::reducers::reduce_virtual_column_statistics;

const ANALYZE_COMMIT_MAX_RETRY_ELAPSED: Duration = Duration::from_secs(15 * 60);

#[derive(Clone)]
pub enum AnalyzeHistogramInfo {
    None,
    Window(HashMap<u32, Receiver<DataBlock>>),
    KllFast { relative_error: f64 },
    KllFull { relative_error: f64 },
}

impl AnalyzeHistogramInfo {
    fn collect_info(&self) -> AnalyzeCollectHistogramInfo {
        match self {
            AnalyzeHistogramInfo::KllFast { relative_error }
            | AnalyzeHistogramInfo::KllFull { relative_error } => {
                AnalyzeCollectHistogramInfo::Kll {
                    relative_error: *relative_error,
                }
            }
            AnalyzeHistogramInfo::None | AnalyzeHistogramInfo::Window(_) => {
                AnalyzeCollectHistogramInfo::None
            }
        }
    }
}

#[derive(Clone)]
enum SinkAnalyzeHistogramInfo {
    None,
    Window(HashMap<u32, Receiver<DataBlock>>),
    KllFast,
    KllFull,
}

impl From<AnalyzeHistogramInfo> for SinkAnalyzeHistogramInfo {
    fn from(value: AnalyzeHistogramInfo) -> Self {
        match value {
            AnalyzeHistogramInfo::None => SinkAnalyzeHistogramInfo::None,
            AnalyzeHistogramInfo::Window(receivers) => SinkAnalyzeHistogramInfo::Window(receivers),
            AnalyzeHistogramInfo::KllFast { .. } => SinkAnalyzeHistogramInfo::KllFast,
            AnalyzeHistogramInfo::KllFull { .. } => SinkAnalyzeHistogramInfo::KllFull,
        }
    }
}

impl SinkAnalyzeHistogramInfo {
    fn accuracy(&self) -> bool {
        matches!(self, SinkAnalyzeHistogramInfo::Window(_))
    }

    fn collect_statistics(&self) -> bool {
        !matches!(self, SinkAnalyzeHistogramInfo::None)
    }
}

impl FuseTable {
    pub fn do_analyze(
        &self,
        ctx: Arc<dyn TableContext>,
        snapshot: Arc<TableSnapshot>,
        pipeline: &mut Pipeline,
        histogram_info: AnalyzeHistogramInfo,
        top_n_size: Option<usize>,
        frequency_columns: Option<String>,
        count_min_sketch_error_rate: Option<f64>,
        no_scan: bool,
        retry_commit: bool,
    ) -> Result<()> {
        let mut parts = Vec::with_capacity(snapshot.segments.len());
        for (idx, segment_location) in snapshot.segments.iter().enumerate() {
            parts.push(FuseLazyPartInfo::create(idx, segment_location.clone()));
        }
        ctx.set_partitions(Partitions::create(PartitionsShuffleKind::Mod, parts))?;

        let (top_n_size, frequency_columns, count_min_sketch_error_rate) = if no_scan {
            (None, None, None)
        } else {
            (top_n_size, frequency_columns, count_min_sketch_error_rate)
        };
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_concurrency = std::cmp::max(max_threads * 2, 10);
        let io_request_semaphore = Arc::new(Semaphore::new(max_concurrency));
        let collect_histogram_info = histogram_info.collect_info();
        let sink_histogram_info = SinkAnalyzeHistogramInfo::from(histogram_info);
        let snapshot_id = snapshot.snapshot_id;
        pipeline.add_source(
            |output| {
                AnalyzeCollectNDVSource::try_create(
                    output,
                    self,
                    ctx.clone(),
                    io_request_semaphore.clone(),
                    no_scan,
                    collect_histogram_info,
                    top_n_size,
                    frequency_columns.clone(),
                    count_min_sketch_error_rate,
                )
            },
            ctx.get_settings().get_max_threads()? as usize,
        )?;
        pipeline.try_resize(1)?;
        pipeline.add_sink(|input| {
            SinkAnalyzeState::create(
                ctx.clone(),
                self,
                snapshot.clone(),
                snapshot_id,
                input,
                sink_histogram_info.clone(),
                top_n_size,
                count_min_sketch_error_rate,
                retry_commit,
            )
        })?;
        Ok(())
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
enum AnalyzeStep {
    CollectNDV,
    CollectHistogram,
    CommitStatistics,
    Finished,
}

struct SinkAnalyzeState {
    ctx: Arc<dyn TableContext>,
    input_port: Arc<InputPort>,

    table: Arc<dyn Table>,
    snapshot: Arc<TableSnapshot>,
    snapshot_id: SnapshotId,
    histogram_info: SinkAnalyzeHistogramInfo,
    input_data: Option<DataBlock>,
    row_count: u64,
    unstats_rows: u64,
    ndv_states: HashMap<ColumnId, MetaHLL>,
    top_n: Option<BlockTopN>,
    count_min_sketch: Option<BlockCountMinSketch>,
    dropped_top_n_columns: BTreeSet<ColumnId>,
    histograms: HashMap<ColumnId, Vec<HistogramBucket>>,
    kll_histograms: HashMap<ColumnId, KllSketch>,
    top_n_size: Option<usize>,
    step: AnalyzeStep,
    retry_commit: bool,
}

impl SinkAnalyzeState {
    fn create(
        ctx: Arc<dyn TableContext>,
        table: &FuseTable,
        snapshot: Arc<TableSnapshot>,
        snapshot_id: SnapshotId,
        input_port: Arc<InputPort>,
        histogram_info: SinkAnalyzeHistogramInfo,
        top_n_size: Option<usize>,
        count_min_sketch_error_rate: Option<f64>,
        retry_commit: bool,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(SinkAnalyzeState {
            ctx,
            input_port,
            table: Arc::new(table.clone()),
            snapshot,
            snapshot_id,
            histogram_info,
            input_data: None,
            row_count: 0,
            unstats_rows: 0,
            ndv_states: Default::default(),
            top_n: top_n_size.map(|_| Default::default()),
            count_min_sketch: count_min_sketch_error_rate.map(|_| Default::default()),
            dropped_top_n_columns: BTreeSet::new(),
            histograms: Default::default(),
            kll_histograms: Default::default(),
            top_n_size,
            step: AnalyzeStep::CollectNDV,
            retry_commit,
        })))
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
            let upper_bound = data_block
                .get_by_offset(2)
                .index(row)
                .unwrap()
                .to_owned()
                .to_datum()
                .ok_or_else(|| {
                    ErrorCode::Internal("Don't support the type to generate histogram")
                })?;
            let lower_bound = data_block
                .get_by_offset(3)
                .index(row)
                .unwrap()
                .to_owned()
                .to_datum()
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

            let count =
                count.ok_or_else(|| ErrorCode::Internal("Missing histogram bucket count"))?;
            let bucket = HistogramBucket::try_from_bounds(
                lower_bound,
                upper_bound,
                count as f64,
                *ndv as f64,
            )
            .map_err(ErrorCode::Internal)?;
            self.histograms.entry(col_id).or_default().push(bucket);
        }
        Ok(())
    }

    fn collect_kll_fast_histograms(&mut self) -> Result<()> {
        for (column_id, sketch) in std::mem::take(&mut self.kll_histograms) {
            let column_ndv = self
                .ndv_states
                .get(&column_id)
                .map(|hll| hll.count() as f64);
            let buckets = sketch.into_equal_depth_buckets(DEFAULT_HISTOGRAM_BUCKETS, column_ndv)?;
            if !buckets.is_empty() {
                self.histograms.insert(column_id, buckets);
            }
        }
        Ok(())
    }

    async fn collect_kll_full_histograms(&mut self) -> Result<()> {
        let Some(mut kll_histograms) = self.create_kll_histogram_collectors()? else {
            return Ok(());
        };
        self.scan_kll_histogram_buckets(&mut kll_histograms).await?;

        for histogram in kll_histograms {
            let column_id = histogram.column_id;
            let buckets = histogram.into_histogram_buckets()?;
            if !buckets.is_empty() {
                self.histograms.insert(column_id, buckets);
            }
        }
        Ok(())
    }

    fn create_kll_histogram_collectors(&mut self) -> Result<Option<Vec<KllHistogramCollector>>> {
        if self.kll_histograms.is_empty() {
            return Ok(None);
        }

        let table = FuseTable::try_from_table(self.table.as_ref())?;
        let mut kll_histograms = std::mem::take(&mut self.kll_histograms);
        let mut collectors = Vec::with_capacity(kll_histograms.len());
        for field in table.schema().fields() {
            let column_id = field.column_id();
            let Some(sketch) = kll_histograms.remove(&column_id) else {
                continue;
            };
            let bounds = sketch.into_equal_depth_bounds(DEFAULT_HISTOGRAM_BUCKETS)?;
            let collector = KllHistogramCollector::new(column_id, bounds)?;
            if collector.is_empty() {
                continue;
            }
            collectors.push(collector);
        }

        if collectors.is_empty() {
            Ok(None)
        } else {
            Ok(Some(collectors))
        }
    }

    async fn scan_kll_histogram_buckets(
        &self,
        collectors: &mut [KllHistogramCollector],
    ) -> Result<()> {
        let table = FuseTable::try_from_table(self.table.as_ref())?;
        let mut field_indices = Vec::with_capacity(collectors.len());
        let mut collector_offsets = HashMap::with_capacity(collectors.len());
        for (field_index, field) in table.schema().fields().iter().enumerate() {
            if let Some(collector_index) = collectors
                .iter()
                .position(|collector| collector.column_id == field.column_id())
            {
                collector_offsets.insert(field_indices.len(), collector_index);
                field_indices.push(field_index as FieldIndex);
            }
        }
        if field_indices.is_empty() {
            return Ok(());
        }

        let projection = Projection::Columns(field_indices);
        let block_reader = table.create_block_reader(self.ctx.clone(), projection, false)?;
        let settings = ReadSettings::from_ctx(&self.ctx)?;
        let storage_format = table.get_storage_format();
        let segments_io =
            SegmentsIO::create(self.ctx.clone(), table.operator.clone(), table.schema());
        let chunk_size = self.ctx.get_settings().get_max_threads()? as usize * 4;

        for chunk in self.snapshot.segments.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;
            for segment in segments {
                let segment = segment?;
                for block_meta in segment.block_metas()? {
                    let block = block_reader
                        .read_by_meta(&settings, &block_meta, &storage_format)
                        .await?;
                    update_kll_histogram_collectors(block, &collector_offsets, collectors)?;
                }
            }
        }

        Ok(())
    }

    async fn commit_statistics(&mut self) -> Result<()> {
        let table = self.table.refresh(self.ctx.as_ref()).await?;
        let table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot = table.read_table_snapshot().await?;
        if snapshot.is_none() {
            return Ok(());
        }
        let snapshot = snapshot.unwrap();
        let column_ids = snapshot.schema.to_leaf_column_id_set();
        self.ndv_states.retain(|k, _| column_ids.contains(k));
        if let Some(top_n) = &mut self.top_n {
            top_n.retain(|k, _| column_ids.contains(k) && !self.dropped_top_n_columns.contains(k));
        }
        if let Some(count_min_sketch) = &mut self.count_min_sketch {
            count_min_sketch.retain(|k, _| column_ids.contains(k));
        }

        let mut new_snapshot = TableSnapshot::try_from_previous(
            snapshot.clone(),
            table.cluster_key_meta(),
            Some(table.get_table_info().ident.seq),
            self.ctx
                .get_table_meta_timestamps(table, Some(snapshot.clone()))?,
        )?;

        // 3. Generate new table statistics
        new_snapshot.summary.additional_stats_meta = Some(AdditionalStatsMeta {
            hll: Some(encode_column_hll(&self.ndv_states)?),
            row_count: self.row_count,
            unstats_rows: self.unstats_rows,
            ..Default::default()
        });

        let has_top_n = self.top_n.as_ref().is_some_and(|top_n| !top_n.is_empty());
        let has_count_min_sketch = self
            .count_min_sketch
            .as_ref()
            .is_some_and(|count_min_sketch| !count_min_sketch.is_empty());
        let table_statistics = if self.ctx.get_settings().get_enable_table_snapshot_stats()?
            || self.histogram_info.collect_statistics()
            || has_top_n
            || has_count_min_sketch
        {
            let histograms = self
                .histograms
                .iter()
                .map(|(column_id, buckets)| {
                    Ok((
                        *column_id,
                        Histogram::try_from_buckets(
                            self.histogram_info.accuracy(),
                            buckets.clone(),
                            None,
                        )?,
                    ))
                })
                .collect::<Result<_>>()?;
            let stats = TableSnapshotStatistics::new(
                self.ndv_states.clone(),
                self.top_n.clone().unwrap_or_default(),
                self.count_min_sketch.clone().unwrap_or_default(),
                histograms,
                self.snapshot_id,
                self.row_count,
            );
            new_snapshot.table_statistics_location = Some(
                table
                    .meta_location_generator
                    .snapshot_statistics_location_from_uuid(
                        &stats.snapshot_id,
                        stats.format_version(),
                    )?,
            );
            Some(stats)
        } else {
            None
        };

        let (col_stats, virtual_col_stats, cluster_stats) =
            self.regenerate_statistics(table, snapshot.as_ref()).await?;
        // 4. Save table statistics
        new_snapshot.summary.col_stats = col_stats;
        new_snapshot.summary.virtual_col_stats = virtual_col_stats;
        new_snapshot.summary.cluster_stats = cluster_stats;
        table
            .commit_to_meta_server(
                self.ctx.as_ref(),
                &table.table_info,
                &table.meta_location_generator,
                new_snapshot,
                table_statistics,
                &None,
                &table.operator,
            )
            .await
    }

    async fn commit_statistics_with_retry(&mut self) -> Result<()> {
        if !self.retry_commit {
            return self.commit_statistics().await;
        }

        let mut retries = 0;
        let mut backoff = set_backoff(None, None, Some(ANALYZE_COMMIT_MAX_RETRY_ELAPSED));
        loop {
            match self.commit_statistics().await {
                Ok(_) => return Ok(()),
                Err(e) if e.code() == ErrorCode::TABLE_VERSION_MISMATCHED => {
                    let Some(duration) = backoff.next_backoff() else {
                        return Err(ErrorCode::StorageOther(format!(
                            "commit analyze statistics failed after {retries} retries"
                        )));
                    };
                    retries += 1;
                    log::warn!(
                        "Retry analyze statistics commit after TableVersionMismatched, sleep {} ms, retrying {} times",
                        duration.as_millis(),
                        retries
                    );
                    tokio::time::sleep(duration).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn regenerate_statistics(
        &self,
        table: &FuseTable,
        snapshot: &TableSnapshot,
    ) -> Result<(
        StatisticsOfColumns,
        Option<StatisticsOfColumns>,
        Option<ClusterStatistics>,
    )> {
        // 1. Read table snapshot.
        let default_cluster_key_id = table.cluster_key_id();

        // 2. Iterator segments and blocks to estimate statistics.
        let mut read_segment_count = 0;
        let mut col_stats = HashMap::new();
        let mut virtual_col_stats = None;
        let mut cluster_stats = None;

        let start = Instant::now();
        let segments_io =
            SegmentsIO::create(self.ctx.clone(), table.operator.clone(), table.schema());
        let chunk_size = self.ctx.get_settings().get_max_threads()? as usize * 4;
        let number_segments = snapshot.segments.len();
        for chunk in snapshot.segments.chunks(chunk_size) {
            let mut stats_of_columns = Vec::new();
            let mut stats_of_virtual_columns = Vec::new();
            let mut blocks_cluster_stats = Vec::new();
            if !col_stats.is_empty() {
                stats_of_columns.push(col_stats.clone());
                blocks_cluster_stats.push(cluster_stats.clone());
            }
            if virtual_col_stats.is_some() {
                stats_of_virtual_columns.push(virtual_col_stats);
            }

            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;
            for segment in segments {
                let segment = segment?;
                stats_of_columns.push(segment.summary.col_stats.clone());
                stats_of_virtual_columns.push(segment.summary.virtual_col_stats.clone());
                blocks_cluster_stats.push(segment.summary.cluster_stats.clone());
            }

            // Generate new column statistics for snapshot
            col_stats = reduce_block_statistics(&stats_of_columns);
            virtual_col_stats = reduce_virtual_column_statistics(&stats_of_virtual_columns);
            cluster_stats =
                reduce_cluster_statistics(&blocks_cluster_stats, default_cluster_key_id);

            // Status.
            {
                read_segment_count += chunk.len();
                let status = format!(
                    "analyze: read segment files:{}/{}, cost:{:?}",
                    read_segment_count,
                    number_segments,
                    start.elapsed()
                );
                self.ctx.set_status_info(&status);
            }
        }

        for (id, hll) in &self.ndv_states {
            if let Some(stats) = col_stats.get_mut(id) {
                let ndv = hll.count() as u64;
                stats.distinct_of_values = Some(ndv);
            }
        }
        Ok((col_stats, virtual_col_stats, cluster_stats))
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
            return Ok(Event::Sync);
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
                    return Ok(Event::Async);
                }
                AnalyzeStep::Finished => {
                    return Ok(Event::Finished);
                }
            }
        }

        if self.input_port.has_data() {
            self.input_data = Some(self.input_port.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        self.input_port.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            AnalyzeStep::CollectNDV => {
                if let Some(mut data_block) = self.input_data.take() {
                    assert!(data_block.is_empty());
                    if let Some(meta) = data_block
                        .take_meta()
                        .and_then(AnalyzeNDVMeta::downcast_from)
                    {
                        if meta.row_count > 0 {
                            for (column_id, column_hll) in meta.column_hlls.iter() {
                                self.ndv_states
                                    .entry(*column_id)
                                    .and_modify(|hll| hll.merge(column_hll))
                                    .or_insert_with(|| column_hll.clone());
                            }
                            for column_id in meta.dropped_top_n_columns {
                                self.dropped_top_n_columns.insert(column_id);
                                if let Some(top_n) = &mut self.top_n {
                                    top_n.remove(&column_id);
                                }
                            }
                            if self.top_n_size.is_some() {
                                match (&mut self.top_n, meta.top_n) {
                                    (Some(top_n), Some(mut meta_top_n)) => {
                                        meta_top_n.retain(|column_id, _| {
                                            !self.dropped_top_n_columns.contains(column_id)
                                        });
                                        merge_column_top_n_mut(top_n, meta_top_n)?;
                                    }
                                    (_, None) => {
                                        self.top_n = None;
                                    }
                                    (None, Some(_)) => {}
                                }
                            }
                            match (&mut self.count_min_sketch, meta.count_min_sketch) {
                                (Some(count_min_sketch), Some(meta_count_min_sketch)) => {
                                    merge_column_count_min_sketch_mut(
                                        count_min_sketch,
                                        meta_count_min_sketch,
                                    );
                                }
                                (_, None) => {
                                    self.count_min_sketch = None;
                                }
                                (None, Some(_)) => {}
                            }
                            self.row_count += meta.row_count;
                        }
                        for (column_id, sketch) in meta.kll_histograms {
                            if let Some(existing) = self.kll_histograms.get_mut(&column_id) {
                                existing.merge(sketch)?;
                            } else {
                                self.kll_histograms.insert(column_id, sketch);
                            }
                        }
                        self.unstats_rows += meta.unstats_rows;
                    }
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match self.step {
            AnalyzeStep::CollectHistogram => {
                let should_commit = match &self.histogram_info {
                    SinkAnalyzeHistogramInfo::Window(receivers) => {
                        let mut finished_count = 0;
                        let receivers = receivers.clone();
                        for (id, receiver) in receivers.iter() {
                            if let Ok(res) = receiver.recv().await {
                                self.create_histogram(*id, res).await?;
                            } else {
                                finished_count += 1;
                            }
                        }
                        finished_count == receivers.len()
                    }
                    SinkAnalyzeHistogramInfo::KllFast => {
                        self.collect_kll_fast_histograms()?;
                        true
                    }
                    SinkAnalyzeHistogramInfo::KllFull => {
                        self.collect_kll_full_histograms().await?;
                        true
                    }
                    SinkAnalyzeHistogramInfo::None => true,
                };
                if should_commit {
                    self.step = AnalyzeStep::CommitStatistics;
                }
            }
            AnalyzeStep::CommitStatistics => {
                self.commit_statistics_with_retry().await?;
                self.step = AnalyzeStep::Finished;
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

struct KllHistogramCollector {
    column_id: ColumnId,
    buckets: Vec<KllBucketStats>,
}

impl KllHistogramCollector {
    fn new(
        column_id: ColumnId,
        bounds: impl IntoIterator<Item = Result<KllBucketBounds>>,
    ) -> Result<Self> {
        let buckets = bounds
            .into_iter()
            .map(|bounds| bounds.map(KllBucketStats::new))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { column_id, buckets })
    }

    fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    fn add_value<T: ?Sized + Hash>(&mut self, value: Datum, ndv_value: &T) -> Result<()> {
        let bucket_index = self.locate_bucket(&value)?;
        self.buckets[bucket_index].add_value(value, ndv_value)
    }

    fn locate_bucket(&self, value: &Datum) -> Result<usize> {
        for (idx, bucket) in self.buckets.iter().enumerate() {
            if !matches!(
                value.compare(&bucket.routing_upper_bound)?,
                Ordering::Greater
            ) {
                return Ok(idx);
            }
        }
        Ok(self.buckets.len().saturating_sub(1))
    }

    fn into_histogram_buckets(self) -> Result<Vec<HistogramBucket>> {
        self.buckets
            .into_iter()
            .filter_map(KllBucketStats::into_histogram_bucket)
            .collect()
    }
}

struct KllBucketStats {
    routing_upper_bound: Datum,
    observed_lower_bound: Option<Datum>,
    observed_upper_bound: Option<Datum>,
    count: u64,
    ndv: MetaHLL,
}

impl KllBucketStats {
    fn new(bounds: KllBucketBounds) -> Self {
        Self {
            routing_upper_bound: bounds.upper,
            observed_lower_bound: None,
            observed_upper_bound: None,
            count: 0,
            ndv: MetaHLL::new(),
        }
    }

    fn add_value<T: ?Sized + Hash>(&mut self, value: Datum, ndv_value: &T) -> Result<()> {
        self.observed_lower_bound = match self.observed_lower_bound.take() {
            Some(lower_bound) => {
                if value.compare(&lower_bound)?.is_lt() {
                    Some(value.clone())
                } else {
                    Some(lower_bound)
                }
            }
            None => Some(value.clone()),
        };
        self.observed_upper_bound = match self.observed_upper_bound.take() {
            Some(upper_bound) => {
                if value.compare(&upper_bound)?.is_gt() {
                    Some(value.clone())
                } else {
                    Some(upper_bound)
                }
            }
            None => Some(value.clone()),
        };
        self.count += 1;
        self.ndv.add_object(ndv_value);
        Ok(())
    }

    fn into_histogram_bucket(self) -> Option<Result<HistogramBucket>> {
        if self.count == 0 {
            return None;
        }
        let lower_bound = self.observed_lower_bound?;
        let upper_bound = self.observed_upper_bound?;
        Some(
            HistogramBucket::try_from_bounds(
                lower_bound,
                upper_bound,
                self.count as f64,
                self.ndv.count() as f64,
            )
            .map_err(ErrorCode::Internal),
        )
    }
}

fn update_kll_histogram_collectors(
    block: DataBlock,
    collector_offsets: &HashMap<usize, usize>,
    collectors: &mut [KllHistogramCollector],
) -> Result<()> {
    for (column_offset, entry) in block.take_columns().into_iter().enumerate() {
        let Some(collector_index) = collector_offsets.get(&column_offset) else {
            continue;
        };
        let collector = &mut collectors[*collector_index];
        match entry {
            BlockEntry::Const(scalar, _, num_rows) => {
                let Some(value) = scalar.clone().to_datum() else {
                    continue;
                };
                for _ in 0..num_rows {
                    collector.add_value(value.clone(), &scalar)?;
                }
            }
            BlockEntry::Column(column) => {
                for value in column.iter() {
                    let Some(datum) = value.clone().to_datum() else {
                        continue;
                    };
                    collector.add_value(datum, &value)?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::types::DecimalScalar;
    use databend_common_expression::types::DecimalSize;

    use super::*;

    #[test]
    fn kll_bucket_ndv_hashes_original_decimal_value() {
        let size = DecimalSize::new(38, 0).unwrap();
        let left = Scalar::Decimal(DecimalScalar::Decimal128(9_007_199_254_740_992, size));
        let right = Scalar::Decimal(DecimalScalar::Decimal128(9_007_199_254_740_993, size));
        let left_datum = left.clone().to_datum().unwrap();
        let right_datum = right.clone().to_datum().unwrap();

        assert_eq!(left_datum, right_datum);

        let mut bucket = KllBucketStats::new(KllBucketBounds {
            lower: left_datum.clone(),
            upper: right_datum.clone(),
            num_values: 2,
        });
        bucket.add_value(left_datum, &left).unwrap();
        bucket.add_value(right_datum, &right).unwrap();
        let histogram_bucket = bucket.into_histogram_bucket().unwrap().unwrap();

        assert_eq!(histogram_bucket.num_values(), 2.0);
        assert_eq!(histogram_bucket.num_distinct(), 2.0);
    }
}
