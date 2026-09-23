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
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

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
use databend_common_storage::MetaHLL;
use databend_storages_common_cache::Partitions;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use databend_storages_common_table_meta::meta::column_oriented_segment::AbstractSegment;
use databend_storages_common_table_meta::meta::encode_column_hll;
use databend_storages_common_table_meta::table::OPT_KEY_ANALYZE_FREQUENCY_COLUMNS;
use databend_storages_common_table_meta::table::analyze_count_min_sketch_error_rate_from_options;
use databend_storages_common_table_meta::table::analyze_top_n_size_from_options;

use crate::FuseLazyPartInfo;
use crate::FuseTable;
use crate::io::SegmentsIO;
use crate::operations::analyze::AnalyzeAccumulator;
use crate::operations::analyze::AnalyzeCollectSource;
use crate::operations::analyze::AnalyzeSegmentProgress;
use crate::operations::analyze::SegmentAnalyzer;
use crate::operations::common::ConflictResolveContext;
use crate::operations::util::set_backoff;

/// Which histogram, if any, ANALYZE produces.
#[derive(Clone)]
pub enum AnalyzeHistogramInfo {
    None,
    /// Buckets computed by window queries running alongside the analyze pipeline, one
    /// receiver per column id.
    Window(HashMap<u32, Receiver<DataBlock>>),
    /// Equal-depth buckets derived from KLL sketches gathered while scanning blocks.
    KllFast {
        relative_error: f64,
    },
    /// Bucket bounds derived from KLL sketches, then exact counts from a second block scan.
    KllFull {
        relative_error: f64,
    },
}

impl AnalyzeHistogramInfo {
    pub fn kll_relative_error(&self) -> Option<f64> {
        match self {
            AnalyzeHistogramInfo::KllFast { relative_error }
            | AnalyzeHistogramInfo::KllFull { relative_error } => Some(*relative_error),
            AnalyzeHistogramInfo::None | AnalyzeHistogramInfo::Window(_) => None,
        }
    }
}

/// Frequency statistics (Top-N and count-min sketch) requested for a set of columns.
#[derive(Clone, Debug)]
pub struct FrequencyOptions {
    /// Comma separated column list, as written in `analyze_frequency_columns`.
    pub columns: String,
    pub top_n_size: Option<usize>,
    pub count_min_sketch_error_rate: Option<f64>,
}

/// Everything that shapes one ANALYZE run.
#[derive(Clone)]
pub struct AnalyzeOptions {
    pub histogram: AnalyzeHistogramInfo,
    pub frequency: Option<FrequencyOptions>,
    /// Only reuse persisted block statistics; blocks without them count as unanalyzed rows.
    pub no_scan: bool,
}

impl AnalyzeOptions {
    /// Frequency statistics as configured on the table; no histogram, full scan.
    pub fn from_table_options(options: &BTreeMap<String, String>) -> Result<Self> {
        let top_n_size = analyze_top_n_size_from_options(options)?;
        let count_min_sketch_error_rate =
            analyze_count_min_sketch_error_rate_from_options(options)?;
        let frequency = options
            .get(OPT_KEY_ANALYZE_FREQUENCY_COLUMNS)
            .filter(|columns| !columns.trim().is_empty())
            .filter(|_| top_n_size.is_some() || count_min_sketch_error_rate.is_some())
            .map(|columns| FrequencyOptions {
                columns: columns.clone(),
                top_n_size,
                count_min_sketch_error_rate,
            });
        Ok(Self {
            histogram: AnalyzeHistogramInfo::None,
            frequency,
            no_scan: false,
        })
    }

    pub fn with_histogram(mut self, histogram: AnalyzeHistogramInfo) -> Self {
        self.histogram = histogram;
        self
    }

    /// Frequency statistics need block data, so NOSCAN drops them.
    pub fn no_scan(mut self) -> Self {
        self.no_scan = true;
        self.frequency = None;
        self
    }
}

impl FuseTable {
    /// Build the ANALYZE pipeline for `snapshot`.
    ///
    /// The snapshot is the collection baseline; if the table moves on through append-only
    /// commits before the statistics are committed, the sink catches up incrementally.
    pub fn do_analyze(
        &self,
        ctx: Arc<dyn TableContext>,
        snapshot: Arc<TableSnapshot>,
        pipeline: &mut Pipeline,
        options: AnalyzeOptions,
    ) -> Result<()> {
        let parts = snapshot
            .segments
            .iter()
            .enumerate()
            .map(|(idx, location)| FuseLazyPartInfo::create(idx, location.clone()))
            .collect();
        ctx.set_partitions(Partitions::create(PartitionsShuffleKind::Mod, parts))?;

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let analyzer = SegmentAnalyzer::try_create(self, &ctx, &options)?;
        let progress = AnalyzeSegmentProgress::new(snapshot.segments.len(), max_threads);
        pipeline.add_source(
            |output| {
                AnalyzeCollectSource::try_create(
                    output,
                    ctx.clone(),
                    analyzer.clone(),
                    progress.clone(),
                )
            },
            max_threads,
        )?;
        pipeline.try_resize(1)?;
        pipeline.add_sink(|input| {
            SinkAnalyzeState::create(
                ctx.clone(),
                self,
                snapshot.clone(),
                input,
                analyzer.clone(),
                options.histogram.clone(),
            )
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AnalyzeStep {
    /// Merge the accumulators shipped by the collect sources.
    MergeSources,
    /// Finish the histogram inputs that need the merged accumulator or a second scan.
    CollectHistogram,
    CommitStatistics,
    Finished,
}

/// Histogram material kept until commit, so a rebase can still extend it.
enum HistogramState {
    None,
    /// Buckets received from the window queries. They describe the collection baseline
    /// and cannot be extended; consumers scale them by row count anyway.
    Window {
        receivers: HashMap<u32, Receiver<DataBlock>>,
        buckets: HashMap<ColumnId, Vec<HistogramBucket>>,
    },
    /// Buckets are derived at commit time from the KLL sketches in the accumulator.
    KllFast,
    /// Bucket bounds are fixed from the first sketch seen for a column; counts come from
    /// block scans and keep accumulating over appended blocks. A column that had no
    /// non-NULL value in the baseline gets its bounds from the first appended rows.
    KllFull {
        collectors: Vec<KllHistogramCollector>,
    },
}

impl HistogramState {
    fn new(histogram: AnalyzeHistogramInfo) -> Self {
        match histogram {
            AnalyzeHistogramInfo::None => HistogramState::None,
            AnalyzeHistogramInfo::Window(receivers) => HistogramState::Window {
                receivers,
                buckets: HashMap::new(),
            },
            AnalyzeHistogramInfo::KllFast { .. } => HistogramState::KllFast,
            AnalyzeHistogramInfo::KllFull { .. } => HistogramState::KllFull {
                collectors: Vec::new(),
            },
        }
    }

    fn enabled(&self) -> bool {
        !matches!(self, HistogramState::None)
    }

    /// Window buckets come from exact SQL; the KLL variants are sketches.
    fn accurate(&self) -> bool {
        matches!(self, HistogramState::Window { .. })
    }
}

struct SinkAnalyzeState {
    ctx: Arc<dyn TableContext>,
    input_port: Arc<InputPort>,

    table: Arc<FuseTable>,
    /// The snapshot the accumulated statistics describe. Advances when the sink rebases
    /// onto concurrently appended segments.
    snapshot: Arc<TableSnapshot>,
    analyzer: Arc<SegmentAnalyzer>,
    input_data: Option<DataBlock>,
    acc: AnalyzeAccumulator,
    histogram: HistogramState,
    step: AnalyzeStep,
}

impl SinkAnalyzeState {
    fn create(
        ctx: Arc<dyn TableContext>,
        table: &FuseTable,
        snapshot: Arc<TableSnapshot>,
        input_port: Arc<InputPort>,
        analyzer: Arc<SegmentAnalyzer>,
        histogram: AnalyzeHistogramInfo,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(SinkAnalyzeState {
            ctx,
            input_port,
            table: Arc::new(table.clone()),
            snapshot,
            analyzer,
            input_data: None,
            acc: AnalyzeAccumulator::default(),
            histogram: HistogramState::new(histogram),
            step: AnalyzeStep::MergeSources,
        })))
    }

    /// Drain the window histogram queries. Returns whether all of them finished.
    async fn receive_window_histograms(&mut self) -> Result<bool> {
        let HistogramState::Window { receivers, buckets } = &mut self.histogram else {
            return Ok(true);
        };
        let mut finished = 0;
        for (column_id, receiver) in receivers.iter() {
            match receiver.recv().await {
                Ok(block) => {
                    parse_window_histogram_buckets(block, buckets.entry(*column_id).or_default())?
                }
                Err(_) => finished += 1,
            }
        }
        Ok(finished == receivers.len())
    }

    /// Fix the KLL full bucket boundaries from the sketches gathered over the base snapshot
    /// and count every block of it into them.
    async fn collect_kll_full_histograms(&mut self) -> Result<()> {
        let mut collectors = self.take_new_kll_collectors(&[])?;
        if collectors.is_empty() {
            return Ok(());
        }

        let segments = self.snapshot.segments.clone();
        self.scan_kll_histogram_buckets(&mut collectors, &segments)
            .await?;
        self.histogram = HistogramState::KllFull { collectors };
        Ok(())
    }

    /// Build KLL full collectors from the accumulated sketches for the columns that
    /// `existing` does not cover, and drop all accumulated sketches.
    ///
    /// For segment versions >= 2, a non-empty sketch yields at least one bucket, so a
    /// missing collector means all previously analyzed rows were NULL. The new sketch
    /// then covers the rows the caller is about to scan. Legacy segments (version < 2)
    /// are skipped by `SegmentAnalyzer` and are outside this rebase assumption; a
    /// collector first created after such segments may not cover their non-NULL rows.
    fn take_new_kll_collectors(
        &mut self,
        existing: &[KllHistogramCollector],
    ) -> Result<Vec<KllHistogramCollector>> {
        let mut sketches = std::mem::take(&mut self.acc.kll_histograms);
        let mut collectors = Vec::new();
        for field in self.table.schema().fields() {
            let column_id = field.column_id();
            if existing
                .iter()
                .any(|collector| collector.column_id == column_id)
            {
                continue;
            }
            let Some(sketch) = sketches.remove(&column_id) else {
                continue;
            };
            let bounds = sketch.into_equal_depth_bounds(DEFAULT_HISTOGRAM_BUCKETS)?;
            let collector = KllHistogramCollector::new(column_id, bounds)?;
            if !collector.is_empty() {
                collectors.push(collector);
            }
        }
        Ok(collectors)
    }

    async fn scan_kll_histogram_buckets(
        &self,
        collectors: &mut [KllHistogramCollector],
        segments: &[Location],
    ) -> Result<()> {
        let table = self.table.as_ref();
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

        for chunk in segments.chunks(chunk_size) {
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

    /// Histogram buckets for the statistics currently accumulated; derived on every commit
    /// attempt because a rebase may have extended the underlying sketches or collectors.
    fn histogram_buckets(&self) -> Result<HashMap<ColumnId, Vec<HistogramBucket>>> {
        let mut histograms = HashMap::new();
        match &self.histogram {
            HistogramState::None => {}
            HistogramState::Window { buckets, .. } => histograms = buckets.clone(),
            HistogramState::KllFast => {
                for (column_id, sketch) in &self.acc.kll_histograms {
                    let column_ndv = self
                        .acc
                        .column_hlls
                        .get(column_id)
                        .map(|hll| hll.count() as f64);
                    let buckets = sketch
                        .clone()
                        .into_equal_depth_buckets(DEFAULT_HISTOGRAM_BUCKETS, column_ndv)?;
                    if !buckets.is_empty() {
                        histograms.insert(*column_id, buckets);
                    }
                }
            }
            HistogramState::KllFull { collectors } => {
                for collector in collectors {
                    let buckets = collector.histogram_buckets()?;
                    if !buckets.is_empty() {
                        histograms.insert(collector.column_id, buckets);
                    }
                }
            }
        }
        Ok(histograms)
    }

    /// Advance the accumulated statistics from the snapshot they describe to `latest`.
    ///
    /// Only append-only changes can be applied incrementally: the appended segments are fed
    /// through the same [`SegmentAnalyzer`] the collect sources used, so HLL, column
    /// statistics, Top-N, count-min sketches and KLL sketches all end up covering `latest`.
    /// Window histograms have no mergeable form and keep describing the base snapshot, which
    /// is how they are consumed between two ANALYZE runs anyway. Everything else is an
    /// unresolvable conflict.
    async fn rebase_statistics(
        &mut self,
        table: &FuseTable,
        latest: Arc<TableSnapshot>,
    ) -> Result<()> {
        if latest.snapshot_id == self.snapshot.snapshot_id {
            return Ok(());
        }
        // Compare snapshot to snapshot: schema DDL always writes a new snapshot carrying the
        // new schema, while `TableMeta.schema` may legitimately differ from an older
        // snapshot's copy for reasons unrelated to this ANALYZE run.
        if latest.schema != self.snapshot.schema {
            return Err(ErrorCode::UnresolvableConflict(
                "cannot rebase ANALYZE statistics after the table schema changed",
            ));
        }
        if table.cluster_key_info().as_ref() != self.analyzer.cluster_key_info() {
            return Err(ErrorCode::UnresolvableConflict(
                "cannot rebase ANALYZE statistics after the cluster key changed",
            ));
        }
        let Some(appended_range) =
            ConflictResolveContext::is_latest_snapshot_append_only(&self.snapshot, &latest)
        else {
            return Err(ErrorCode::UnresolvableConflict(
                "cannot rebase ANALYZE statistics over a non-append table change",
            ));
        };

        let appended = latest.segments[appended_range].to_vec();
        for location in &appended {
            self.analyzer.analyze(location, &mut self.acc).await?;
        }
        if let HistogramState::KllFull { collectors } = &mut self.histogram {
            let mut collectors = std::mem::take(collectors);
            // Columns without a collector were all NULL so far; seed their bounds from the
            // appended rows, then count the appended blocks into every collector.
            let new_collectors = self.take_new_kll_collectors(&collectors)?;
            collectors.extend(new_collectors);
            self.scan_kll_histogram_buckets(&mut collectors, &appended)
                .await?;
            self.histogram = HistogramState::KllFull { collectors };
        }
        log::info!(
            "ANALYZE rebased statistics over {} appended segments",
            appended.len()
        );
        self.snapshot = latest;
        Ok(())
    }

    async fn commit_statistics(&mut self) -> Result<()> {
        let table = self.table.refresh(self.ctx.as_ref()).await?;
        let table = FuseTable::try_from_table(table.as_ref())?;
        let Some(snapshot) = table.read_table_snapshot().await? else {
            return Ok(());
        };
        self.rebase_statistics(table, snapshot.clone()).await?;

        let table_statistics = self.table_statistics(&snapshot)?;
        let new_snapshot = self.build_snapshot(table, snapshot, table_statistics.as_ref())?;
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
            .await?;
        // ANALYZE bypasses the mutation sinks but still buffers a snapshot in an explicit
        // transaction. Record a known zero so commit retries preserve the source's
        // cumulative counters instead of treating them as missing.
        self.ctx
            .txn_mgr()
            .lock()
            .add_logical_change_delta(table.get_id(), (0, 0));
        Ok(())
    }

    /// The new snapshot: `base` with the accumulated statistics written into its summary.
    fn build_snapshot(
        &mut self,
        table: &FuseTable,
        base: Arc<TableSnapshot>,
        table_statistics: Option<&TableSnapshotStatistics>,
    ) -> Result<TableSnapshot> {
        let column_ids = base.schema.to_leaf_column_id_set();
        self.acc.column_hlls.retain(|k, _| column_ids.contains(k));
        let dropped = &self.acc.dropped_top_n_columns;
        self.acc
            .top_n
            .retain(|k, _| column_ids.contains(k) && !dropped.contains(k));
        self.acc
            .count_min_sketch
            .retain(|k, _| column_ids.contains(k));

        let mut snapshot = TableSnapshot::try_from_previous(
            base.clone(),
            table.cluster_key_info(),
            Some(table.get_table_info().ident.seq),
            self.ctx.get_table_meta_timestamps(table, Some(base))?,
        )?;
        snapshot.summary.additional_stats_meta = Some(AdditionalStatsMeta {
            hll: Some(encode_column_hll(&self.acc.column_hlls)?),
            row_count: self.acc.row_count,
            unstats_rows: self.acc.unstats_rows,
            ..Default::default()
        });
        let mut col_stats = self.acc.segment_stats.col_stats.clone();
        for (id, hll) in &self.acc.column_hlls {
            if let Some(stats) = col_stats.get_mut(id) {
                stats.distinct_of_values = Some(hll.count() as u64);
            }
        }
        snapshot.summary.col_stats = col_stats;
        snapshot.summary.virtual_col_stats = self.acc.segment_stats.virtual_col_stats.clone();
        snapshot.summary.cluster_stats = self.acc.segment_stats.cluster_stats.clone();
        if let Some(stats) = table_statistics {
            snapshot.table_statistics_location = Some(
                table
                    .meta_location_generator
                    .snapshot_statistics_location_from_uuid(
                        &stats.snapshot_id,
                        stats.format_version(),
                    )?,
            );
        }
        Ok(snapshot)
    }

    /// The statistics file to write alongside the snapshot, if anything in it is wanted.
    fn table_statistics(
        &self,
        snapshot: &TableSnapshot,
    ) -> Result<Option<TableSnapshotStatistics>> {
        let wanted = self.ctx.get_settings().get_enable_table_snapshot_stats()?
            || self.histogram.enabled()
            || !self.acc.top_n.is_empty()
            || !self.acc.count_min_sketch.is_empty();
        if !wanted {
            return Ok(None);
        }

        let accurate = self.histogram.accurate();
        let histograms = self
            .histogram_buckets()?
            .into_iter()
            .map(|(column_id, buckets)| {
                Ok((
                    column_id,
                    Histogram::try_from_buckets(accurate, buckets, None)
                        .map_err(ErrorCode::Internal)?,
                ))
            })
            .collect::<Result<_>>()?;
        Ok(Some(TableSnapshotStatistics::new(
            self.acc.column_hlls.clone(),
            self.acc.top_n.clone(),
            self.acc.count_min_sketch.clone(),
            histograms,
            snapshot.snapshot_id,
            self.acc.row_count,
        )))
    }

    /// Commit, and on `TableVersionMismatched` rebase onto the new snapshot and try again
    /// under the same OCC backoff as data commits. Conflicts that cannot be rebased surface
    /// immediately from `commit_statistics`.
    async fn commit_statistics_with_retry(&mut self) -> Result<()> {
        // Created on the first failure so the collection phase does not eat into the budget.
        let mut backoff = None;
        let mut retries = 0;
        loop {
            match self.commit_statistics().await {
                Ok(()) => {
                    log::info!("Committed ANALYZE statistics after {retries} retries");
                    return Ok(());
                }
                Err(e) if e.code() == ErrorCode::TABLE_VERSION_MISMATCHED => {
                    let backoff = backoff.get_or_insert_with(|| set_backoff(None, None, None));
                    let Some(delay) = backoff.next_backoff() else {
                        return Err(e);
                    };
                    retries += 1;
                    log::warn!(
                        "Retry analyze statistics commit after TableVersionMismatched, sleep {} ms, retrying {} times",
                        delay.as_millis(),
                        retries
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }
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
            return Ok(match self.step {
                AnalyzeStep::MergeSources => {
                    self.step = AnalyzeStep::CollectHistogram;
                    Event::Async
                }
                AnalyzeStep::CollectHistogram | AnalyzeStep::CommitStatistics => Event::Async,
                AnalyzeStep::Finished => Event::Finished,
            });
        }

        if self.input_port.has_data() {
            self.input_data = Some(self.input_port.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        self.input_port.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        debug_assert_eq!(self.step, AnalyzeStep::MergeSources);
        if let Some(mut data_block) = self.input_data.take() {
            assert!(data_block.is_empty());
            if let Some(acc) = data_block
                .take_meta()
                .and_then(AnalyzeAccumulator::downcast_from)
            {
                self.acc.merge(acc, self.analyzer.cluster_key_info())?;
            }
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match self.step {
            AnalyzeStep::CollectHistogram => {
                let ready = match &self.histogram {
                    HistogramState::Window { .. } => self.receive_window_histograms().await?,
                    HistogramState::KllFull { .. } => {
                        self.collect_kll_full_histograms().await?;
                        true
                    }
                    HistogramState::None | HistogramState::KllFast => true,
                };
                if ready {
                    self.step = AnalyzeStep::CommitStatistics;
                }
            }
            AnalyzeStep::CommitStatistics => {
                self.commit_statistics_with_retry().await?;
                self.step = AnalyzeStep::Finished;
            }
            AnalyzeStep::MergeSources | AnalyzeStep::Finished => unreachable!(),
        }
        Ok(())
    }
}

/// One row per bucket: `(quantile, ndv, max_value, min_value, count)`, as produced by the
/// window queries built in the ANALYZE interpreter.
fn parse_window_histogram_buckets(
    block: DataBlock,
    buckets: &mut Vec<HistogramBucket>,
) -> Result<()> {
    const NDV: usize = 1;
    const MAX_VALUE: usize = 2;
    const MIN_VALUE: usize = 3;
    const COUNT: usize = 4;

    let column_u64 = |offset: usize, row: usize| -> Result<u64> {
        block
            .get_by_offset(offset)
            .index(row)
            .and_then(|scalar| {
                scalar
                    .as_number()
                    .and_then(|number| number.as_u_int64())
                    .copied()
            })
            .ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "window histogram column {offset} row {row} is not an unsigned integer"
                ))
            })
    };
    let column_datum = |offset: usize, row: usize| -> Result<Datum> {
        block
            .get_by_offset(offset)
            .index(row)
            .and_then(|scalar| scalar.to_owned().to_datum())
            .ok_or_else(|| ErrorCode::Internal("Don't support the type to generate histogram"))
    };

    for row in 0..block.num_rows() {
        let bucket = HistogramBucket::try_from_bounds(
            column_datum(MIN_VALUE, row)?,
            column_datum(MAX_VALUE, row)?,
            column_u64(COUNT, row)? as f64,
            column_u64(NDV, row)? as f64,
        )
        .map_err(ErrorCode::Internal)?;
        buckets.push(bucket);
    }
    Ok(())
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

    fn histogram_buckets(&self) -> Result<Vec<HistogramBucket>> {
        self.buckets
            .iter()
            .filter_map(KllBucketStats::to_histogram_bucket)
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

    fn to_histogram_bucket(&self) -> Option<Result<HistogramBucket>> {
        if self.count == 0 {
            return None;
        }
        let lower_bound = self.observed_lower_bound.clone()?;
        let upper_bound = self.observed_upper_bound.clone()?;
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
    fn frequency_options_require_columns_and_a_statistic() {
        let mut options = BTreeMap::new();
        assert!(
            AnalyzeOptions::from_table_options(&options)
                .unwrap()
                .frequency
                .is_none()
        );

        options.insert("analyze_frequency_columns".to_string(), "c".to_string());
        assert!(
            AnalyzeOptions::from_table_options(&options)
                .unwrap()
                .frequency
                .is_none()
        );

        options.insert("analyze_top_n_size".to_string(), "3".to_string());
        let frequency = AnalyzeOptions::from_table_options(&options)
            .unwrap()
            .frequency
            .unwrap();
        assert_eq!(frequency.columns, "c");
        assert_eq!(frequency.top_n_size, Some(3));
        assert_eq!(frequency.count_min_sketch_error_rate, None);

        assert!(
            AnalyzeOptions::from_table_options(&options)
                .unwrap()
                .no_scan()
                .frequency
                .is_none()
        );
    }

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
        let histogram_bucket = bucket.to_histogram_bucket().unwrap().unwrap();

        assert_eq!(histogram_bucket.num_values(), 2.0);
        assert_eq!(histogram_bucket.num_distinct(), 2.0);
    }
}
