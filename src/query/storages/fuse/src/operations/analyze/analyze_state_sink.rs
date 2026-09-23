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
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Instant;

use async_channel::Receiver;
use backoff::backoff::Backoff;
use databend_common_base::runtime::JoinHandle;
use databend_common_base::runtime::Runtime;
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
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use databend_storages_common_table_meta::meta::column_oriented_segment::AbstractSegment;
use databend_storages_common_table_meta::meta::encode_column_hll;
use databend_storages_common_table_meta::table::OPT_KEY_ANALYZE_FREQUENCY_COLUMNS;
use databend_storages_common_table_meta::table::analyze_count_min_sketch_error_rate_from_options;
use databend_storages_common_table_meta::table::analyze_top_n_size_from_options;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use parking_lot::Mutex;

use crate::FuseLazyPartInfo;
use crate::FuseStorageFormat;
use crate::FuseTable;
use crate::io::BlockReader;
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
    /// and cannot be extended, so after an append rebase they miss the appended values
    /// while the rest of the statistics cover the latest snapshot. They are still
    /// published, as they would be after any later append: histograms are not gated by
    /// statistics freshness and the optimizer scales them by row count. They also stay
    /// flagged accurate, so the optimizer still caps a column's NDV by the histogram NDV
    /// and may underestimate it by the distinct values only present in appended rows;
    /// this is the same drift a later append causes and is accepted.
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

    /// Window buckets come from exact SQL; the KLL variants are sketches. Stays true for
    /// rebased Window buckets, see [`HistogramState::Window`].
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

    /// Count `segments` into every KLL full collector, first creating collectors for the
    /// columns that have none yet. Each collector thus counts exactly the segments passed
    /// since it was created: the whole base snapshot, then the segments of every rebase.
    async fn extend_kll_full_histograms(&mut self, segments: &[Location]) -> Result<()> {
        let HistogramState::KllFull { collectors } = &mut self.histogram else {
            return Ok(());
        };
        let mut collectors = std::mem::take(collectors);
        let new_collectors = self.take_new_kll_collectors(&collectors)?;
        collectors.extend(new_collectors);
        if let Some(scanner) = KllBucketScanner::try_create(&self.ctx, &self.table, &collectors)? {
            let scanner = Arc::new(scanner);
            scanner.scan(&self.ctx, &self.table, segments).await?;
            scanner.merge_into(&mut collectors)?;
        }
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
        // Columns without a collector were all NULL so far; their bounds come from the
        // appended rows.
        self.extend_kll_full_histograms(&appended).await?;
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
        // Virtual column ids are segment-local and cannot be merged across segments, matching
        // `merge_statistics_mut` on the write path.
        snapshot.summary.virtual_col_stats = None;
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
                        let segments = self.snapshot.segments.clone();
                        self.extend_kll_full_histograms(&segments).await?;
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

    /// Same bucket bounds, no values recorded.
    fn empty_like(&self) -> Self {
        Self {
            column_id: self.column_id,
            buckets: self
                .buckets
                .iter()
                .map(KllBucketStats::empty_like)
                .collect(),
        }
    }

    /// Fold in a collector built from `empty_like` of this one.
    fn merge(&mut self, other: KllHistogramCollector) -> Result<()> {
        debug_assert_eq!(self.column_id, other.column_id);
        debug_assert_eq!(self.buckets.len(), other.buckets.len());
        for (bucket, other) in self.buckets.iter_mut().zip(other.buckets) {
            bucket.merge(other)?;
        }
        Ok(())
    }

    /// Record `count` occurrences of `value`; a constant column is recorded in one call.
    fn add_value<T: ?Sized + Hash>(
        &mut self,
        value: &Datum,
        ndv_value: &T,
        count: u64,
    ) -> Result<()> {
        let bucket_index = self.locate_bucket(value)?;
        self.buckets[bucket_index].add_value(value, ndv_value, count)
    }

    /// The first bucket whose routing upper bound is not less than `value`; values above
    /// every bound go to the last bucket. The bounds are taken at increasing ranks of one
    /// sketch, so they are non-decreasing and can be binary searched.
    fn locate_bucket(&self, value: &Datum) -> Result<usize> {
        let (mut low, mut high) = (0, self.buckets.len());
        while low < high {
            let mid = low + (high - low) / 2;
            if value
                .compare(&self.buckets[mid].routing_upper_bound)?
                .is_gt()
            {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        Ok(low.min(self.buckets.len().saturating_sub(1)))
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
        Self::with_routing_upper_bound(bounds.upper)
    }

    fn empty_like(&self) -> Self {
        Self::with_routing_upper_bound(self.routing_upper_bound.clone())
    }

    fn with_routing_upper_bound(routing_upper_bound: Datum) -> Self {
        Self {
            routing_upper_bound,
            observed_lower_bound: None,
            observed_upper_bound: None,
            count: 0,
            ndv: MetaHLL::new(),
        }
    }

    fn merge(&mut self, other: KllBucketStats) -> Result<()> {
        let (Some(lower), Some(upper)) = (&other.observed_lower_bound, &other.observed_upper_bound)
        else {
            return Ok(());
        };
        self.widen_observed_bounds(lower, upper)?;
        self.count += other.count;
        self.ndv.merge(&other.ndv);
        Ok(())
    }

    fn add_value<T: ?Sized + Hash>(
        &mut self,
        value: &Datum,
        ndv_value: &T,
        count: u64,
    ) -> Result<()> {
        self.widen_observed_bounds(value, value)?;
        self.count += count;
        self.ndv.add_object(ndv_value);
        Ok(())
    }

    /// Widen the observed range to cover `[lower, upper]`, cloning a bound only when it
    /// changes: this runs once per counted value.
    fn widen_observed_bounds(&mut self, lower: &Datum, upper: &Datum) -> Result<()> {
        match &self.observed_lower_bound {
            Some(bound) if !lower.compare(bound)?.is_lt() => {}
            _ => self.observed_lower_bound = Some(lower.clone()),
        }
        match &self.observed_upper_bound {
            Some(bound) if !upper.compare(bound)?.is_gt() => {}
            _ => self.observed_upper_bound = Some(upper.clone()),
        }
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

/// Counts table blocks into a set of KLL full collectors: reads only their columns, one
/// block per task on a scan-scoped runtime, each task counting into a local collector set
/// borrowed from `pool`. A set is borrowed only while counting, without awaiting, so at
/// most one exists per runtime worker. Counts, bounds and HLLs merge order-independently,
/// so the merged result matches a sequential scan.
struct KllBucketScanner {
    block_reader: Arc<BlockReader>,
    settings: ReadSettings,
    storage_format: FuseStorageFormat,
    /// Collector index of each projected column.
    collector_indices: Vec<usize>,
    empty: Vec<KllHistogramCollector>,
    pool: Mutex<Vec<Vec<KllHistogramCollector>>>,
}

impl KllBucketScanner {
    /// `None` when no collector column exists in the table schema.
    fn try_create(
        ctx: &Arc<dyn TableContext>,
        table: &FuseTable,
        collectors: &[KllHistogramCollector],
    ) -> Result<Option<Self>> {
        let mut field_indices = Vec::with_capacity(collectors.len());
        let mut collector_indices = Vec::with_capacity(collectors.len());
        for (field_index, field) in table.schema().fields().iter().enumerate() {
            if let Some(collector_index) = collectors
                .iter()
                .position(|collector| collector.column_id == field.column_id())
            {
                field_indices.push(field_index as FieldIndex);
                collector_indices.push(collector_index);
            }
        }
        if field_indices.is_empty() {
            return Ok(None);
        }
        let projection = Projection::Columns(field_indices);
        Ok(Some(Self {
            block_reader: table.create_block_reader(ctx.clone(), projection, false)?,
            settings: ReadSettings::from_ctx(ctx)?,
            storage_format: table.get_storage_format(),
            collector_indices,
            empty: collectors
                .iter()
                .map(KllHistogramCollector::empty_like)
                .collect(),
            pool: Mutex::default(),
        }))
    }

    /// Count every block of `segments`. One scan-scoped runtime keeps up to `max_in_flight`
    /// blocks in flight, refilling as each one finishes. Counting is CPU bound, so it runs
    /// here instead of on the shared IO runtime.
    async fn scan(
        self: &Arc<Self>,
        ctx: &Arc<dyn TableContext>,
        table: &FuseTable,
        segments: &[Location],
    ) -> Result<()> {
        // A rebase over a snapshot that only replaced statistics appends nothing.
        if segments.is_empty() {
            return Ok(());
        }
        let max_threads = ctx.get_settings().get_max_threads()?.max(1) as usize;
        let segments_io = SegmentsIO::create(ctx.clone(), table.operator.clone(), table.schema());
        let runtime =
            Runtime::with_worker_threads(max_threads, Some("analyze-kll-histogram".to_owned()))?;
        // Declared after `runtime` so the outstanding tasks are aborted, on error or
        // cancellation, before the runtime is dropped.
        let mut tasks = KllBucketTasks::default();
        let max_in_flight = max_threads * 2;

        let started = Instant::now();
        let (mut num_blocks, mut num_rows) = (0, 0);
        for chunk in segments.chunks(max_threads * 4) {
            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;
            for segment in segments {
                for block_meta in segment?.block_metas()? {
                    ctx.check_aborting()
                        .map_err(|e| e.with_context("failed to build KLL histogram buckets"))?;
                    if tasks.pending.len() >= max_in_flight {
                        if let Some(rows) = tasks.next().await {
                            num_blocks += 1;
                            num_rows += rows?;
                        }
                    }
                    let scanner = self.clone();
                    tasks
                        .pending
                        .push(runtime.spawn(async move { scanner.count_block(block_meta).await }));
                }
            }
        }
        while let Some(rows) = tasks.next().await {
            num_blocks += 1;
            num_rows += rows?;
        }
        log::info!(
            "ANALYZE KLL full bucket scan: {} segments, {} blocks, {} rows, {} columns in {:?}",
            segments.len(),
            num_blocks,
            num_rows,
            self.collector_indices.len(),
            started.elapsed()
        );
        Ok(())
    }

    /// Count one block; returns its row count.
    async fn count_block(&self, block_meta: Arc<BlockMeta>) -> Result<usize> {
        let block = self
            .block_reader
            .read_by_meta(&self.settings, &block_meta, &self.storage_format)
            .await?;
        let num_rows = block.num_rows();
        let mut local = self.pool.lock().pop().unwrap_or_else(|| {
            self.empty
                .iter()
                .map(KllHistogramCollector::empty_like)
                .collect()
        });
        let counted = update_kll_histogram_collectors(block, &self.collector_indices, &mut local);
        self.pool.lock().push(local);
        counted.map(|_| num_rows)
    }

    /// Merge the counted local collector sets into `collectors`.
    fn merge_into(&self, collectors: &mut [KllHistogramCollector]) -> Result<()> {
        for local in std::mem::take(&mut *self.pool.lock()) {
            for (collector, local) in collectors.iter_mut().zip(local) {
                collector.merge(local)?;
            }
        }
        Ok(())
    }
}

/// In-flight block tasks of a KLL bucket scan, aborted when dropped.
#[derive(Default)]
struct KllBucketTasks {
    pending: FuturesUnordered<JoinHandle<Result<usize>>>,
}

impl KllBucketTasks {
    async fn next(&mut self) -> Option<Result<usize>> {
        let joined = self.pending.next().await?;
        Some(joined.unwrap_or_else(|e| {
            Err(ErrorCode::Internal(format!(
                "[ANALYZE-TABLE] KLL histogram bucket task failed: {e}"
            )))
        }))
    }
}

impl Drop for KllBucketTasks {
    fn drop(&mut self) {
        for task in self.pending.iter() {
            task.abort();
        }
    }
}

/// Count the columns of `block`, projected as for [`KllBucketScanner`], into `collectors`.
fn update_kll_histogram_collectors(
    block: DataBlock,
    collector_indices: &[usize],
    collectors: &mut [KllHistogramCollector],
) -> Result<()> {
    for (entry, collector_index) in block.take_columns().into_iter().zip(collector_indices) {
        let collector = &mut collectors[*collector_index];
        match entry {
            BlockEntry::Const(scalar, _, num_rows) => {
                let Some(value) = scalar.clone().to_datum() else {
                    continue;
                };
                if num_rows > 0 {
                    collector.add_value(&value, &scalar, num_rows as u64)?;
                }
            }
            BlockEntry::Column(column) => {
                for value in column.iter() {
                    let Some(datum) = value.clone().to_datum() else {
                        continue;
                    };
                    collector.add_value(&datum, &value, 1)?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::DecimalScalar;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::Int64Type;

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

    fn collector_with_upper_bounds(uppers: &[Datum]) -> KllHistogramCollector {
        KllHistogramCollector::new(
            0,
            uppers.iter().map(|upper| {
                Ok(KllBucketBounds {
                    lower: upper.clone(),
                    upper: upper.clone(),
                    num_values: 1,
                })
            }),
        )
        .unwrap()
    }

    #[test]
    fn kll_locate_bucket_matches_linear_scan() {
        fn linear(collector: &KllHistogramCollector, value: &Datum) -> usize {
            collector
                .buckets
                .iter()
                .position(|bucket| !value.compare(&bucket.routing_upper_bound).unwrap().is_gt())
                .unwrap_or(collector.buckets.len() - 1)
        }

        let int = |v: i64| Datum::Int(v);
        let float = |v: f64| Datum::Float(v.into());
        let bytes = |v: &str| Datum::Bytes(v.as_bytes().to_vec());
        let cases: Vec<(Vec<Datum>, Vec<Datum>)> = vec![
            // Single bucket.
            (vec![int(5)], (0..=10).map(int).collect()),
            // Repeated bounds must route to the first of the run.
            (
                [1, 3, 3, 3, 7, 7, 9].into_iter().map(int).collect(),
                (-2..=12).map(int).collect(),
            ),
            (
                (0..100).map(|v| int(v * 10)).collect(),
                (-5..=1005).map(int).collect(),
            ),
            (
                [-1.5, 0.0, 0.0, 2.25, 8.0].into_iter().map(float).collect(),
                [-9.0, -1.5, -1.0, 0.0, 1.0, 2.25, 3.0, 8.0, 9.0]
                    .into_iter()
                    .map(float)
                    .collect(),
            ),
            (
                ["b", "d", "d", "f"].into_iter().map(bytes).collect(),
                ["", "a", "b", "c", "d", "e", "f", "g"]
                    .into_iter()
                    .map(bytes)
                    .collect(),
            ),
        ];
        for (uppers, values) in cases {
            let collector = collector_with_upper_bounds(&uppers);
            for value in &values {
                assert_eq!(
                    collector.locate_bucket(value).unwrap(),
                    linear(&collector, value),
                    "value {value:?} with upper bounds {uppers:?}"
                );
            }
        }
    }

    #[test]
    fn kll_const_column_counts_like_repeated_values() {
        use databend_common_expression::types::DataType;
        use databend_common_expression::types::NumberDataType;
        use databend_common_expression::types::NumberScalar;

        let uppers: Vec<_> = [10, 20, 30].into_iter().map(Datum::Int).collect();
        let collector_indices = [0];
        let buckets = |block: DataBlock| {
            let mut collectors = vec![collector_with_upper_bounds(&uppers)];
            update_kll_histogram_collectors(block, &collector_indices, &mut collectors).unwrap();
            collectors[0].histogram_buckets().unwrap()
        };

        let rows = 7;
        let from_const = buckets(DataBlock::new(
            vec![BlockEntry::new_const_column(
                DataType::Number(NumberDataType::Int64),
                Scalar::Number(NumberScalar::Int64(15)),
                rows,
            )],
            rows,
        ));
        let from_column = buckets(DataBlock::new_from_columns(vec![Int64Type::from_data(
            vec![15i64; rows],
        )]));

        assert_eq!(from_const.len(), 1);
        assert_eq!(from_const, from_column);
        assert_eq!(from_const[0].num_values(), rows as f64);
        assert_eq!(from_const[0].num_distinct(), 1.0);
    }

    /// Counting blocks into local collectors and merging them, onto a collector that already
    /// holds values as in a rebase, equals counting every block into one collector.
    #[test]
    fn kll_merged_collectors_match_single_collector() {
        let uppers: Vec<_> = [10, 20, 30, 40].into_iter().map(Datum::Int).collect();
        let parts = [vec![3, 15, 15, 44], vec![1, 22, 38, 15, 50], vec![39, 12]];
        let count = |collector: &mut KllHistogramCollector, values: &[i64]| {
            let block = DataBlock::new_from_columns(vec![Int64Type::from_data(values.to_vec())]);
            update_kll_histogram_collectors(block, &[0], std::slice::from_mut(collector)).unwrap();
        };

        let mut single = collector_with_upper_bounds(&uppers);
        for part in &parts {
            count(&mut single, part);
        }
        let mut merged = collector_with_upper_bounds(&uppers);
        count(&mut merged, &parts[0]);
        for part in &parts[1..] {
            let mut local = merged.empty_like();
            count(&mut local, part);
            merged.merge(local).unwrap();
        }

        let buckets = merged.histogram_buckets().unwrap();
        assert_eq!(buckets, single.histogram_buckets().unwrap());
        // Observed bounds, not routing bounds: (-inf, 10] holds {1, 3}; (10, 20] holds
        // {12, 15}; the last bucket also takes 44 and 50, above every routing bound.
        let bounds: Vec<_> = buckets
            .iter()
            .map(|bucket| {
                (
                    bucket.lower_bound(),
                    bucket.upper_bound(),
                    bucket.num_values(),
                )
            })
            .collect();
        assert_eq!(bounds, vec![
            (Datum::Int(1), Datum::Int(3), 2.0),
            (Datum::Int(12), Datum::Int(15), 4.0),
            (Datum::Int(22), Datum::Int(22), 1.0),
            (Datum::Int(38), Datum::Int(50), 4.0),
        ]);
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
        bucket.add_value(&left_datum, &left, 1).unwrap();
        bucket.add_value(&right_datum, &right, 1).unwrap();
        let histogram_bucket = bucket.to_histogram_bucket().unwrap().unwrap();

        assert_eq!(histogram_bucket.num_values(), 2.0);
        assert_eq!(histogram_bucket.num_distinct(), 2.0);
    }
}
