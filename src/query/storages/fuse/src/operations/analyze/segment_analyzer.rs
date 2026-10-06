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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnId;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_sql::ApproxDistinctColumns;
use databend_common_statistics::KllSketch;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CachedObject;
use databend_storages_common_cache::CompactSegmentInfo;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_cache::SegmentStatistics;
use databend_storages_common_index::Index;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::BlockTopN;
use databend_storages_common_table_meta::meta::ClusterKeyInfo;
use databend_storages_common_table_meta::meta::ColumnTopN;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::decode_column_hll;
use databend_storages_common_table_meta::meta::encode_column_hll;
use databend_storages_common_table_meta::meta::merge_column_count_min_sketch_mut;
use databend_storages_common_table_meta::meta::merge_column_hll_mut;
use databend_storages_common_table_meta::meta::merge_column_top_n_mut;
use opendal::Operator;
use tokio::sync::Semaphore;

use crate::FuseStorageFormat;
use crate::FuseTable;
use crate::io::BlockReader;
use crate::io::BlockStats;
use crate::io::BlockStatsBuilder;
use crate::io::CachedMetaWriter;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;
use crate::io::TableMetaLocationGenerator;
use crate::io::read::meta::SegmentStatsReader;
use crate::operations::acquire_task_permit;
use crate::operations::analyze::AnalyzeAccumulator;
use crate::operations::analyze::AnalyzeOptions;

/// Which statistics one ANALYZE run collects, and from where.
///
/// Column maps are keyed by the block offset of the projected column, see
/// [`build_analyze_column_projection`].
struct CollectPolicy {
    no_scan: bool,
    top_n_size: Option<usize>,
    count_min_sketch_error_rate: Option<f64>,
    kll_relative_error: Option<f64>,
    ndv_columns_map: BTreeMap<FieldIndex, TableField>,
    frequency_columns_map: BTreeMap<FieldIndex, TableField>,
    kll_columns_map: BTreeMap<FieldIndex, TableField>,
}

/// Where a segment's Top-N comes from.
#[derive(Debug, PartialEq, Eq)]
enum TopNSource {
    /// Top-N is not collected.
    Skip,
    /// Every block has a complete persisted Top-N for the requested columns.
    Persisted,
    /// Rebuilt with this capacity while scanning every block.
    Rescan { capacity: usize },
}

/// How one segment is folded into the accumulator.
#[derive(Debug, PartialEq, Eq)]
enum SegmentPlan {
    /// The segment cannot be analyzed without scanning and scanning is not allowed.
    Unstats,
    Fold {
        /// HLL merged from persisted block statistics.
        persisted_hlls: BlockHLL,
        top_n: TopNSource,
        /// Blocks whose data must be read.
        scan_blocks: Vec<usize>,
    },
}

impl CollectPolicy {
    fn collect_ndv(&self) -> bool {
        !self.ndv_columns_map.is_empty()
    }

    fn collect_top_n(&self) -> bool {
        self.top_n_size.is_some() && !self.frequency_columns_map.is_empty()
    }

    fn collect_count_min_sketch(&self) -> bool {
        self.count_min_sketch_error_rate.is_some() && !self.frequency_columns_map.is_empty()
    }

    fn collect_kll(&self) -> bool {
        self.kll_relative_error.is_some() && !self.kll_columns_map.is_empty()
    }

    /// Decide what to reuse and what to scan for a segment with `block_count` blocks.
    /// `persisted.block_hlls` is padded to `block_count`; `block_top_ns` may be shorter.
    fn plan(&self, block_count: usize, persisted: &SegmentStatistics) -> Result<SegmentPlan> {
        let collect_top_n = self.collect_top_n();
        let persisted_top_n_complete = self.top_n_size.is_some_and(|capacity| {
            persisted.block_top_ns.len() == block_count
                && persisted
                    .block_top_ns
                    .iter()
                    .all(|top_n| block_top_n_covers(top_n, &self.frequency_columns_map, capacity))
        });
        // Count-min sketches and KLL sketches are never persisted, and Top-N has to be
        // rebuilt when the persisted one does not cover the requested columns.
        let rescan_all = self.collect_kll()
            || self.collect_count_min_sketch()
            || (collect_top_n && !persisted_top_n_complete);

        let mut scan_blocks: Vec<usize> = if rescan_all {
            (0..block_count).collect()
        } else {
            Vec::new()
        };
        let mut persisted_hlls = BlockHLL::new();
        if !rescan_all && self.collect_ndv() {
            for (idx, raw) in persisted.block_hlls.iter().enumerate() {
                match decode_column_hll(raw)? {
                    Some(hlls) => merge_column_hll_mut(&mut persisted_hlls, &hlls),
                    None => scan_blocks.push(idx),
                }
            }
        }

        // NOSCAN only ever asks for HLL; anything that needs block data was already stripped
        // from the options, so an incomplete persisted HLL makes the whole segment unanalyzed.
        let scan_requested = collect_top_n || self.collect_count_min_sketch() || self.collect_kll();
        if !scan_blocks.is_empty() && self.no_scan && !scan_requested {
            return Ok(SegmentPlan::Unstats);
        }

        let top_n = match (self.top_n_size.filter(|_| collect_top_n), rescan_all) {
            (None, _) => TopNSource::Skip,
            (Some(capacity), true) => TopNSource::Rescan { capacity },
            (Some(_), false) => TopNSource::Persisted,
        };
        Ok(SegmentPlan::Fold {
            persisted_hlls,
            top_n,
            scan_blocks,
        })
    }
}

/// Collects ANALYZE statistics from one segment at a time.
///
/// The collect sources drive it over the snapshot being analyzed, and the sink drives it
/// over segments appended concurrently before the statistics are committed, so both paths
/// observe exactly the same reuse-or-scan decisions.
pub struct SegmentAnalyzer {
    policy: Arc<CollectPolicy>,
    io_request_semaphore: Arc<Semaphore>,

    block_reader: Arc<BlockReader>,
    dal: Operator,
    settings: ReadSettings,
    storage_format: FuseStorageFormat,
    segment_reader: CompactSegmentInfoReader,
    stats_reader: SegmentStatsReader,
    cluster_key_info: Option<ClusterKeyInfo>,
}

impl SegmentAnalyzer {
    pub fn try_create(
        table: &FuseTable,
        ctx: &Arc<dyn TableContext>,
        options: &AnalyzeOptions,
    ) -> Result<Arc<Self>> {
        let table_schema = table.schema();
        let (projection, policy) =
            build_analyze_column_projection(table, table_schema.clone(), options)?;
        let block_reader = table.create_block_reader(ctx.clone(), projection, false)?;
        let dal = table.get_operator();
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        Ok(Arc::new(Self {
            policy: Arc::new(policy),
            io_request_semaphore: Arc::new(Semaphore::new(std::cmp::max(max_threads * 2, 10))),
            block_reader,
            settings: ReadSettings::from_ctx(ctx)?,
            storage_format: table.get_storage_format(),
            segment_reader: MetaReaders::segment_info_reader(dal.clone(), table_schema),
            stats_reader: MetaReaders::segment_stats_reader(dal.clone()),
            dal,
            cluster_key_info: table.cluster_key_info(),
        }))
    }

    pub fn cluster_key_info(&self) -> Option<&ClusterKeyInfo> {
        self.cluster_key_info.as_ref()
    }

    /// Fold one segment into `acc`, reusing persisted block statistics where possible and
    /// scanning block data otherwise. Blocks that had to be scanned get their HLL and Top-N
    /// written back to the segment so later runs can reuse them.
    #[async_backtrace::framed]
    pub async fn analyze(&self, location: &Location, acc: &mut AnalyzeAccumulator) -> Result<()> {
        let segment = self.read_segment(location).await?;
        acc.segment_stats
            .fold(&segment.summary, self.cluster_key_info.as_ref());
        if location.1 < 2 {
            // Legacy segments carry no block statistics at all.
            acc.unstats_rows += segment.summary.row_count;
            return Ok(());
        }

        let block_count = segment.summary.block_count as usize;
        let mut persisted = self
            .read_persisted_block_stats(&segment, block_count)
            .await?;
        let SegmentPlan::Fold {
            persisted_hlls,
            top_n,
            scan_blocks,
        } = self.policy.plan(block_count, &persisted)?
        else {
            acc.unstats_rows += segment.summary.row_count;
            return Ok(());
        };

        merge_column_hll_mut(&mut acc.column_hlls, &persisted_hlls);
        if top_n == TopNSource::Persisted {
            for block_top_n in &persisted.block_top_ns {
                let top_n = project_block_top_n(block_top_n, &self.policy.frequency_columns_map);
                merge_column_top_n_mut(&mut acc.top_n, top_n)?;
            }
        }
        acc.row_count += segment.summary.row_count;

        if scan_blocks.is_empty() {
            return Ok(());
        }
        let block_metas = segment.block_metas()?;
        let rescan_top_n = match top_n {
            TopNSource::Rescan { capacity } => Some(capacity),
            TopNSource::Skip | TopNSource::Persisted => None,
        };
        let scanned = self
            .scan_blocks(&scan_blocks, &block_metas, rescan_top_n)
            .await?;
        let changed =
            self.fold_scanned_blocks(acc, &mut persisted, scan_blocks, scanned, rescan_top_n)?;
        if changed {
            self.write_persisted_block_stats(
                location,
                segment.summary.clone(),
                block_metas,
                persisted,
            )
            .await?;
        }
        Ok(())
    }

    async fn read_segment(&self, location: &Location) -> Result<Arc<CompactSegmentInfo>> {
        self.segment_reader
            .read(&LoadParams {
                location: location.0.clone(),
                len_hint: None,
                ver: location.1,
                put_cache: true,
            })
            .await
    }

    async fn read_persisted_block_stats(
        &self,
        segment: &CompactSegmentInfo,
        block_count: usize,
    ) -> Result<SegmentStatistics> {
        let mut stats = match segment.summary.additional_stats_loc() {
            Some((location, ver)) => {
                let stats = self
                    .stats_reader
                    .read(&LoadParams {
                        location,
                        len_hint: None,
                        ver,
                        put_cache: true,
                    })
                    .await?;
                SegmentStatistics::new(stats.block_hlls.clone(), stats.block_top_ns.clone())
            }
            None => SegmentStatistics::new(Vec::new(), Vec::new()),
        };
        stats.block_hlls.resize(block_count, Vec::new());
        Ok(stats)
    }

    /// Read the listed blocks and build the requested statistics for each of them.
    /// `rescan_top_n` is the Top-N capacity when Top-N has to be rebuilt from block data.
    async fn scan_blocks(
        &self,
        block_indexes: &[usize],
        block_metas: &[Arc<BlockMeta>],
        rescan_top_n: Option<usize>,
    ) -> Result<Vec<(Option<BlockStats>, HashMap<ColumnId, KllSketch>)>> {
        let runtime = GlobalIORuntime::instance();
        let mut handlers = Vec::with_capacity(block_indexes.len());
        for &idx in block_indexes {
            let permit = acquire_task_permit(self.io_request_semaphore.clone()).await?;
            let block_reader = self.block_reader.clone();
            let settings = self.settings;
            let storage_format = self.storage_format;
            let block_meta = block_metas[idx].clone();
            let policy = self.policy.clone();
            handlers.push(runtime.spawn(async move {
                let block = block_reader
                    .read_by_meta(&settings, &block_meta, &storage_format)
                    .await?;
                let top_n = rescan_top_n.map(|capacity| (&policy.frequency_columns_map, capacity));
                let count_min_sketch = policy
                    .count_min_sketch_error_rate
                    .map(|rate| (&policy.frequency_columns_map, rate));
                let mut builder =
                    BlockStatsBuilder::new(&policy.ndv_columns_map, top_n, count_min_sketch)?;
                builder.add_block(&block)?;
                let stats = builder.finalize_with_top_n()?;
                let kll_histograms = build_kll_histograms(
                    &block,
                    &policy.kll_columns_map,
                    policy.kll_relative_error,
                )?;
                drop(permit);
                Ok::<_, ErrorCode>((stats, kll_histograms))
            }));
        }

        let joint = futures::future::try_join_all(handlers).await.map_err(|e| {
            ErrorCode::StorageOther(format!(
                "[ANALYZE-TABLE] Failed to build NDV statistics: {}",
                e
            ))
        })?;
        joint.into_iter().collect()
    }

    /// Merge scanned block statistics into `acc` and refresh the persisted copies.
    /// Returns whether any persisted block statistic changed.
    fn fold_scanned_blocks(
        &self,
        acc: &mut AnalyzeAccumulator,
        persisted: &mut SegmentStatistics,
        scan_blocks: Vec<usize>,
        scanned: Vec<(Option<BlockStats>, HashMap<ColumnId, KllSketch>)>,
        rescan_top_n: Option<usize>,
    ) -> Result<bool> {
        let block_top_n_template = rescan_top_n
            .map(|capacity| empty_block_top_n(&self.policy.frequency_columns_map, capacity));
        if block_top_n_template.is_some() {
            persisted
                .block_top_ns
                .resize(persisted.block_hlls.len(), HashMap::new());
        }

        let mut changed = false;
        for ((stats, kll_histograms), idx) in scanned.into_iter().zip(scan_blocks) {
            let (hll, top_n, count_min_sketch, dropped_top_n) = split_block_stats(stats);
            for column_id in dropped_top_n {
                acc.dropped_top_n_columns.insert(column_id);
                acc.top_n.remove(&column_id);
            }
            if let Some(hll) = hll {
                merge_column_hll_mut(&mut acc.column_hlls, &hll);
                persisted.block_hlls[idx] = encode_column_hll(&hll)?;
                changed = true;
            }
            if let Some(template) = &block_top_n_template {
                let mut complete_top_n = template.clone();
                merge_column_top_n_mut(&mut complete_top_n, top_n)?;
                let mut projected =
                    project_block_top_n(&complete_top_n, &self.policy.frequency_columns_map);
                projected.retain(|column_id, _| !acc.dropped_top_n_columns.contains(column_id));
                merge_column_top_n_mut(&mut acc.top_n, projected)?;
                persisted.block_top_ns[idx] = complete_top_n;
                changed = true;
            }
            merge_column_count_min_sketch_mut(&mut acc.count_min_sketch, count_min_sketch);
            for (column_id, sketch) in kll_histograms {
                match acc.kll_histograms.get_mut(&column_id) {
                    Some(existing) => existing.merge(sketch)?,
                    None => {
                        acc.kll_histograms.insert(column_id, sketch);
                    }
                }
            }
        }
        Ok(changed)
    }

    async fn write_persisted_block_stats(
        &self,
        location: &Location,
        mut summary: databend_storages_common_table_meta::meta::Statistics,
        block_metas: Vec<Arc<BlockMeta>>,
        persisted: SegmentStatistics,
    ) -> Result<()> {
        let segment_loc = location.0.as_str();
        let data = persisted.to_bytes()?;
        let size = data.len() as u64;
        let stats_location =
            TableMetaLocationGenerator::gen_segment_stats_location_from_segment_location(
                segment_loc,
            );
        self.dal.write(&stats_location, data).await?;
        if let Some(cache) = SegmentStatistics::cache() {
            cache.evict(&stats_location);
        }

        summary.additional_stats_meta = Some(AdditionalStatsMeta {
            size,
            location: (stats_location, SegmentStatistics::VERSION),
            ..Default::default()
        });
        SegmentInfo::new(block_metas, summary)
            .write_meta_through_cache(&self.dal, segment_loc)
            .await
    }
}

/// The statistics one table column takes part in.
#[derive(Default)]
struct ColumnRoles {
    ndv: Option<TableField>,
    frequency: Option<TableField>,
    kll: Option<TableField>,
}

/// Resolve the columns each statistic needs and build one projection covering all of them.
///
/// The returned maps are keyed by the projected block offset (0..N), because after projection
/// columns are accessed by position, not by table field index.
fn build_analyze_column_projection(
    table: &FuseTable,
    table_schema: TableSchemaRef,
    options: &AnalyzeOptions,
) -> Result<(Projection, CollectPolicy)> {
    let ndv_columns_map = table
        .approx_distinct_cols()
        .distinct_column_fields(table_schema.clone(), RangeIndex::supported_table_type)?;
    let frequency_columns_map = match &options.frequency {
        Some(frequency) => frequency
            .columns
            .parse::<ApproxDistinctColumns>()?
            .distinct_column_fields(table_schema.clone(), RangeIndex::supported_table_type)?,
        None => BTreeMap::new(),
    };
    let kll_relative_error = options.histogram.kll_relative_error();

    let mut analyze_columns: BTreeMap<FieldIndex, ColumnRoles> = BTreeMap::new();
    for (field_index, field) in ndv_columns_map {
        analyze_columns.entry(field_index).or_default().ndv = Some(field);
    }
    for (field_index, field) in frequency_columns_map {
        analyze_columns.entry(field_index).or_default().frequency = Some(field);
    }
    if kll_relative_error.is_some() {
        for (field_index, field) in kll_column_fields(&table_schema) {
            analyze_columns.entry(field_index).or_default().kll = Some(field);
        }
    }

    let mut field_indices = Vec::with_capacity(analyze_columns.len());
    let mut ndv_columns_map = BTreeMap::new();
    let mut frequency_columns_map = BTreeMap::new();
    let mut kll_columns_map = BTreeMap::new();
    for (field_index, roles) in analyze_columns {
        let offset = field_indices.len();
        field_indices.push(field_index);
        if let Some(field) = roles.ndv {
            ndv_columns_map.insert(offset, field);
        }
        if let Some(field) = roles.frequency {
            frequency_columns_map.insert(offset, field);
        }
        if let Some(field) = roles.kll {
            kll_columns_map.insert(offset, field);
        }
    }

    let policy = CollectPolicy {
        no_scan: options.no_scan,
        top_n_size: options.frequency.as_ref().and_then(|f| f.top_n_size),
        count_min_sketch_error_rate: options
            .frequency
            .as_ref()
            .and_then(|f| f.count_min_sketch_error_rate),
        kll_relative_error,
        ndv_columns_map,
        frequency_columns_map,
        kll_columns_map,
    };
    Ok((Projection::Columns(field_indices), policy))
}

fn kll_column_fields(table_schema: &TableSchemaRef) -> BTreeMap<FieldIndex, TableField> {
    table_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !matches!(field.computed_expr(), Some(ComputedExpr::Virtual(_))))
        .filter(|(_, field)| RangeIndex::supported_type(&field.data_type().into()))
        .map(|(index, field)| (index, field.clone()))
        .collect()
}

fn empty_block_top_n(columns: &BTreeMap<FieldIndex, TableField>, capacity: usize) -> BlockTopN {
    columns
        .values()
        .map(|field| (field.column_id(), ColumnTopN::with_capacity(capacity)))
        .collect()
}

fn block_top_n_covers(
    top_n: &BlockTopN,
    columns: &BTreeMap<FieldIndex, TableField>,
    capacity: usize,
) -> bool {
    columns.values().all(|field| {
        top_n
            .get(&field.column_id())
            .is_some_and(|top_n| top_n.capacity == capacity)
    })
}

fn project_block_top_n(top_n: &BlockTopN, columns: &BTreeMap<FieldIndex, TableField>) -> BlockTopN {
    columns
        .values()
        .filter_map(|field| {
            top_n
                .get(&field.column_id())
                .filter(|top_n| !top_n.values.is_empty())
                .cloned()
                .map(|top_n| (field.column_id(), top_n))
        })
        .collect()
}

fn split_block_stats(
    block_stats: Option<BlockStats>,
) -> (
    Option<BlockHLL>,
    BlockTopN,
    databend_storages_common_table_meta::meta::BlockCountMinSketch,
    Vec<ColumnId>,
) {
    match block_stats {
        Some(stats) => (
            (!stats.hll.is_empty()).then_some(stats.hll),
            stats.top_n,
            stats.count_min_sketch,
            stats.dropped_top_n,
        ),
        None => (None, HashMap::new(), HashMap::new(), vec![]),
    }
}

fn build_kll_histograms(
    block: &DataBlock,
    kll_columns_map: &BTreeMap<FieldIndex, TableField>,
    relative_error: Option<f64>,
) -> Result<HashMap<ColumnId, KllSketch>> {
    let Some(relative_error) = relative_error else {
        return Ok(HashMap::new());
    };
    if kll_columns_map.is_empty() {
        return Ok(HashMap::new());
    }

    let mut sketches = kll_columns_map
        .iter()
        .map(|(offset, field)| {
            Ok((
                *offset,
                field.column_id(),
                KllSketch::with_relative_error(relative_error)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    for (offset, _, sketch) in sketches.iter_mut() {
        match block.get_by_offset(*offset) {
            BlockEntry::Const(scalar, _, num_rows) => {
                if let Some(datum) = scalar.as_ref().to_datum() {
                    for _ in 0..*num_rows {
                        sketch.insert(datum.clone())?;
                    }
                }
            }
            BlockEntry::Column(column) => {
                for row in 0..column.len() {
                    let Some(datum) = column.index(row).and_then(|value| value.to_datum()) else {
                        continue;
                    };
                    sketch.insert(datum)?;
                }
            }
        }
    }

    let mut histograms = HashMap::with_capacity(sketches.len());
    for (_, column_id, sketch) in sketches {
        if !sketch.is_empty() {
            histograms.insert(column_id, sketch);
        }
    }
    Ok(histograms)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_expression::Scalar;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_storage::MetaHLL;
    use databend_storages_common_table_meta::meta::RawBlockHLL;

    use super::*;

    fn int_field(name: &str, column_id: ColumnId) -> TableField {
        TableField::new_from_column_id(
            name,
            TableDataType::Number(NumberDataType::Int32),
            column_id,
        )
    }

    fn policy(no_scan: bool) -> CollectPolicy {
        CollectPolicy {
            no_scan,
            top_n_size: None,
            count_min_sketch_error_rate: None,
            kll_relative_error: None,
            ndv_columns_map: BTreeMap::from([(0, int_field("a", 10))]),
            frequency_columns_map: BTreeMap::new(),
            kll_columns_map: BTreeMap::new(),
        }
    }

    fn encoded_hll() -> RawBlockHLL {
        let mut hll = MetaHLL::new();
        hll.add_object(&1);
        encode_column_hll(&BlockHLL::from([(10, hll)])).unwrap()
    }

    fn persisted(hlls: Vec<RawBlockHLL>, top_ns: Vec<BlockTopN>) -> SegmentStatistics {
        SegmentStatistics::new(hlls, top_ns)
    }

    #[test]
    fn plan_reuses_complete_persisted_hll() {
        let plan = policy(true)
            .plan(2, &persisted(vec![encoded_hll(), encoded_hll()], vec![]))
            .unwrap();
        let SegmentPlan::Fold {
            persisted_hlls,
            top_n,
            scan_blocks,
        } = plan
        else {
            panic!("expected a fold plan");
        };
        assert_eq!(persisted_hlls.len(), 1);
        assert_eq!(top_n, TopNSource::Skip);
        assert!(scan_blocks.is_empty());
    }

    #[test]
    fn plan_scans_only_blocks_missing_hll_when_scanning_is_allowed() {
        let plan = policy(false)
            .plan(
                3,
                &persisted(vec![encoded_hll(), vec![], encoded_hll()], vec![]),
            )
            .unwrap();
        assert!(matches!(
            plan,
            SegmentPlan::Fold { scan_blocks, .. } if scan_blocks == vec![1]
        ));
    }

    #[test]
    fn plan_counts_segment_as_unstats_under_no_scan_when_hll_is_missing() {
        let plan = policy(true)
            .plan(2, &persisted(vec![encoded_hll(), vec![]], vec![]))
            .unwrap();
        assert_eq!(plan, SegmentPlan::Unstats);
    }

    #[test]
    fn plan_without_ndv_columns_needs_no_block_data() {
        let mut policy = policy(true);
        policy.ndv_columns_map.clear();
        let plan = policy
            .plan(2, &persisted(vec![vec![], vec![]], vec![]))
            .unwrap();
        assert!(matches!(
            plan,
            SegmentPlan::Fold { scan_blocks, .. } if scan_blocks.is_empty()
        ));
    }

    #[test]
    fn plan_reuses_persisted_top_n_only_when_it_covers_every_block() {
        let mut policy = policy(false);
        policy.top_n_size = Some(3);
        policy.frequency_columns_map = BTreeMap::from([(0, int_field("a", 10))]);

        let complete = vec![
            BlockTopN::from([(10, ColumnTopN::with_capacity(3))]),
            BlockTopN::from([(10, ColumnTopN::with_capacity(3))]),
        ];
        let plan = policy
            .plan(2, &persisted(vec![encoded_hll(), encoded_hll()], complete))
            .unwrap();
        assert!(matches!(
            plan,
            SegmentPlan::Fold { top_n: TopNSource::Persisted, scan_blocks, .. } if scan_blocks.is_empty()
        ));

        let wrong_capacity = vec![
            BlockTopN::from([(10, ColumnTopN::with_capacity(3))]),
            BlockTopN::from([(10, ColumnTopN::with_capacity(8))]),
        ];
        let plan = policy
            .plan(
                2,
                &persisted(vec![encoded_hll(), encoded_hll()], wrong_capacity),
            )
            .unwrap();
        assert!(matches!(
            plan,
            SegmentPlan::Fold { top_n: TopNSource::Rescan { capacity: 3 }, scan_blocks, .. } if scan_blocks == vec![0, 1]
        ));
    }

    #[test]
    fn plan_rescans_everything_for_count_min_sketch_and_kll() {
        let mut policy = policy(true);
        policy.count_min_sketch_error_rate = Some(0.01);
        policy.frequency_columns_map = BTreeMap::from([(0, int_field("a", 10))]);
        // NOSCAN never reaches here with frequency statistics, but the plan must still
        // honour an explicit request rather than silently counting rows as unanalyzed.
        let plan = policy
            .plan(2, &persisted(vec![encoded_hll(), encoded_hll()], vec![]))
            .unwrap();
        assert!(matches!(
            plan,
            SegmentPlan::Fold { scan_blocks, persisted_hlls, .. }
                if scan_blocks == vec![0, 1] && persisted_hlls.is_empty()
        ));

        policy.count_min_sketch_error_rate = None;
        policy.kll_relative_error = Some(0.01);
        policy.kll_columns_map = BTreeMap::from([(0, int_field("a", 10))]);
        let plan = policy.plan(1, &persisted(vec![vec![]], vec![])).unwrap();
        assert!(matches!(
            plan,
            SegmentPlan::Fold { scan_blocks, .. } if scan_blocks == vec![0]
        ));
    }

    #[test]
    fn kll_column_fields_skip_virtual_computed_columns() {
        let schema = Arc::new(TableSchema::new(vec![
            TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("b", TableDataType::Number(NumberDataType::Int32))
                .with_computed_expr(Some(ComputedExpr::Virtual("(a + 1)".to_string()))),
            TableField::new("c", TableDataType::Number(NumberDataType::Int32))
                .with_computed_expr(Some(ComputedExpr::Stored("(a + 2)".to_string()))),
        ]));

        let fields = kll_column_fields(&schema);

        assert_eq!(fields.keys().copied().collect::<Vec<_>>(), vec![0, 2]);
        assert_eq!(
            fields
                .values()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            vec!["a", "c"]
        );
    }

    #[test]
    fn split_block_stats_keeps_frequency_only_out_of_hll_slots() {
        let top_n = HashMap::from([(1, Default::default())]);
        let count_min_sketch = HashMap::from([(1, Default::default())]);

        let (hll, top_n, count_min_sketch, dropped_top_n) = split_block_stats(Some(BlockStats {
            hll: HashMap::new(),
            top_n,
            count_min_sketch,
            dropped_top_n: vec![7],
        }));

        assert!(hll.is_none());
        assert_eq!(top_n.len(), 1);
        assert_eq!(count_min_sketch.len(), 1);
        assert_eq!(dropped_top_n, vec![7]);
    }

    #[test]
    fn block_top_n_coverage_requires_every_requested_column() {
        let columns = BTreeMap::from([(0, int_field("a", 10)), (1, int_field("b", 20))]);
        let top_n = HashMap::from([
            (10, ColumnTopN::with_capacity(8)),
            (20, ColumnTopN::with_capacity(8)),
        ]);

        assert!(block_top_n_covers(&top_n, &columns, 8));
        assert!(!block_top_n_covers(&top_n, &columns, 4));

        let missing_column = HashMap::from([(10, ColumnTopN::with_capacity(8))]);
        assert!(!block_top_n_covers(&missing_column, &columns, 8));
    }

    #[test]
    fn project_block_top_n_ignores_empty_and_unrequested_entries() {
        let columns = BTreeMap::from([(0, int_field("a", 10))]);
        let mut requested = ColumnTopN::with_capacity(8);
        requested.add(Scalar::Number(NumberScalar::Int32(1)).as_ref(), 3);
        let top_n = HashMap::from([(10, requested.clone()), (20, ColumnTopN::with_capacity(8))]);

        assert_eq!(
            project_block_top_n(&top_n, &columns),
            HashMap::from([(10, requested)])
        );
    }
}
