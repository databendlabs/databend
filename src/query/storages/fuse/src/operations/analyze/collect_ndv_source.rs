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
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::plan::PartInfoPtr;
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
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_sql::ApproxDistinctColumns;
use databend_common_statistics::KllSketch;
use databend_common_storage::MetaHLL;
use databend_storages_common_cache::BlockMeta;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CachedObject;
use databend_storages_common_cache::CompactSegmentInfo;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_cache::SegmentStatistics;
use databend_storages_common_index::Index;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::BlockCountMinSketch;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::BlockTopN;
use databend_storages_common_table_meta::meta::ColumnTopN;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::decode_column_hll;
use databend_storages_common_table_meta::meta::encode_column_hll;
use databend_storages_common_table_meta::meta::merge_column_count_min_sketch_mut;
use databend_storages_common_table_meta::meta::merge_column_top_n_mut;
use opendal::Operator;
use tokio::sync::Semaphore;

use crate::FuseLazyPartInfo;
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
use crate::operations::analyze::AnalyzeNDVMeta;

struct SegmentWithHLL {
    segment_location: Location,
    block_metas: Vec<Arc<BlockMeta>>,
    origin_summary: Statistics,
    raw_block_hlls: Vec<RawBlockHLL>,
    block_top_ns: Vec<BlockTopN>,
    collect_top_n: bool,

    new_block_hlls: Vec<Option<BlockHLL>>,
    new_block_top_n: Vec<BlockTopN>,
    new_block_count_min_sketch: Vec<BlockCountMinSketch>,
    block_indexes: Vec<usize>,
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
    BlockCountMinSketch,
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

enum State {
    ReadData(Option<PartInfoPtr>),
    CollectNDV {
        segment_location: Location,
        segment_info: Arc<CompactSegmentInfo>,
        block_hlls: Vec<RawBlockHLL>,
        block_top_ns: Vec<BlockTopN>,
    },
    BuildHLL,
    MergeHLL,
    WriteMeta,
    Finish,
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            State::ReadData(_) => "ReadData",
            State::CollectNDV { .. } => "CollectNDV",
            State::BuildHLL => "BuildHLL",
            State::MergeHLL => "MergeHLL",
            State::WriteMeta => "WriteMeta",
            State::Finish => "Finish",
        };
        f.write_str(name)
    }
}

pub struct AnalyzeCollectNDVSource {
    state: State,
    output: Arc<OutputPort>,
    io_request_semaphore: Arc<Semaphore>,
    column_hlls: HashMap<ColumnId, MetaHLL>,
    top_n: Option<BlockTopN>,
    count_min_sketch: Option<BlockCountMinSketch>,
    dropped_top_n_columns: BTreeSet<ColumnId>,
    kll_histograms: HashMap<ColumnId, KllSketch>,
    row_count: u64,
    unstats_rows: u64,
    no_scan: bool,
    histogram_info: AnalyzeCollectHistogramInfo,
    top_n_size: Option<usize>,
    count_min_sketch_error_rate: Option<f64>,

    segment_with_hll: Option<SegmentWithHLL>,

    ctx: Arc<dyn TableContext>,
    block_reader: Arc<BlockReader>,
    dal: Operator,
    settings: ReadSettings,
    storage_format: FuseStorageFormat,
    segment_reader: CompactSegmentInfoReader,
    stats_reader: SegmentStatsReader,
    ndv_columns_map: BTreeMap<FieldIndex, TableField>,
    frequency_columns_map: BTreeMap<FieldIndex, TableField>,
    kll_columns_map: BTreeMap<FieldIndex, TableField>,
}

#[derive(Clone, Copy)]
pub enum AnalyzeCollectHistogramInfo {
    None,
    Kll { relative_error: f64 },
}

impl AnalyzeCollectHistogramInfo {
    fn kll_relative_error(&self) -> Option<f64> {
        match self {
            AnalyzeCollectHistogramInfo::Kll { relative_error } => Some(*relative_error),
            AnalyzeCollectHistogramInfo::None => None,
        }
    }
}

impl AnalyzeCollectNDVSource {
    pub fn try_create(
        output: Arc<OutputPort>,
        table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        io_request_semaphore: Arc<Semaphore>,
        no_scan: bool,
        histogram_info: AnalyzeCollectHistogramInfo,
        top_n_size: Option<usize>,
        frequency_columns: Option<String>,
        count_min_sketch_error_rate: Option<f64>,
    ) -> Result<ProcessorPtr> {
        let table_schema = table.schema();
        let AnalyzeColumnProjection {
            projection,
            ndv_columns_map,
            frequency_columns_map,
            kll_columns_map,
        } = build_analyze_column_projection(
            table,
            table_schema.clone(),
            histogram_info,
            top_n_size,
            frequency_columns.as_deref(),
            count_min_sketch_error_rate,
        )?;
        let block_reader = table.create_block_reader(ctx.clone(), projection, false)?;
        let dal = table.get_operator();
        let settings = ReadSettings::from_ctx(&ctx)?;
        let segment_reader = MetaReaders::segment_info_reader(dal.clone(), table_schema.clone());
        let stats_reader = MetaReaders::segment_stats_reader(dal.clone());
        Ok(ProcessorPtr::create(Box::new(Self {
            state: State::ReadData(None),
            output,
            io_request_semaphore,
            column_hlls: HashMap::new(),
            top_n: (top_n_size.is_some() && !frequency_columns_map.is_empty()).then(HashMap::new),
            count_min_sketch: (count_min_sketch_error_rate.is_some()
                && !frequency_columns_map.is_empty())
            .then(HashMap::new),
            dropped_top_n_columns: BTreeSet::new(),
            kll_histograms: HashMap::new(),
            row_count: 0,
            segment_with_hll: None,
            ctx,
            block_reader,
            dal,
            settings,
            storage_format: table.get_storage_format(),
            segment_reader,
            stats_reader,
            ndv_columns_map,
            frequency_columns_map,
            kll_columns_map,
            histogram_info,
            top_n_size,
            count_min_sketch_error_rate,
            no_scan,
            unstats_rows: 0,
        })))
    }
}

struct AnalyzeColumnProjection {
    projection: Projection,
    ndv_columns_map: BTreeMap<FieldIndex, TableField>,
    frequency_columns_map: BTreeMap<FieldIndex, TableField>,
    kll_columns_map: BTreeMap<FieldIndex, TableField>,
}

type AnalyzeColumnFields = (Option<TableField>, Option<TableField>, Option<TableField>);

fn frequency_column_fields_from_options(
    table_schema: TableSchemaRef,
    collect_frequency_stats: bool,
    frequency_columns: Option<&str>,
) -> Result<BTreeMap<FieldIndex, TableField>> {
    if !collect_frequency_stats {
        return Ok(BTreeMap::new());
    }
    let Some(columns) = frequency_columns else {
        return Ok(BTreeMap::new());
    };
    columns
        .parse::<ApproxDistinctColumns>()?
        .distinct_column_fields(table_schema, RangeIndex::supported_table_type)
}

fn build_analyze_column_projection(
    table: &FuseTable,
    table_schema: TableSchemaRef,
    histogram_info: AnalyzeCollectHistogramInfo,
    top_n_size: Option<usize>,
    frequency_columns: Option<&str>,
    count_min_sketch_error_rate: Option<f64>,
) -> Result<AnalyzeColumnProjection> {
    let ndv_columns_map = table
        .approx_distinct_cols
        .distinct_column_fields(table_schema.clone(), RangeIndex::supported_table_type)?;
    let frequency_columns_map = frequency_column_fields_from_options(
        table_schema.clone(),
        top_n_size.is_some() || count_min_sketch_error_rate.is_some(),
        frequency_columns,
    )?;
    let mut analyze_columns: BTreeMap<FieldIndex, AnalyzeColumnFields> = BTreeMap::new();
    for (field_index, field) in ndv_columns_map {
        analyze_columns.insert(field_index, (Some(field), None, None));
    }
    for (field_index, field) in frequency_columns_map {
        analyze_columns
            .entry(field_index)
            .and_modify(|(_, top_n_field, _)| *top_n_field = Some(field.clone()))
            .or_insert_with(|| (None, Some(field), None));
    }
    if histogram_info.kll_relative_error().is_some() {
        for (index, field) in kll_column_fields(&table_schema) {
            analyze_columns
                .entry(index)
                .and_modify(|(_, _, kll_field)| *kll_field = Some(field.clone()))
                .or_insert_with(|| (None, None, Some(field)));
        }
    }

    // Rebuild maps so that keys correspond to the column order in the projection.
    //
    // The original maps are ordered by table field index ascending, but after projection,
    // columns are loaded in that order and accessed by *position*. Therefore, re-index
    // both maps with the projected block offsets (0..N).
    let mut field_indices = Vec::with_capacity(analyze_columns.len());
    let mut ndv_columns_map = BTreeMap::new();
    let mut frequency_columns_map = BTreeMap::new();
    let mut kll_columns_map = BTreeMap::new();
    for (field_index, (ndv_field, top_n_field, kll_field)) in analyze_columns {
        let offset = field_indices.len();
        field_indices.push(field_index);
        if let Some(field) = ndv_field {
            ndv_columns_map.insert(offset, field);
        }
        if let Some(field) = top_n_field {
            frequency_columns_map.insert(offset, field);
        }
        if let Some(field) = kll_field {
            kll_columns_map.insert(offset, field);
        }
    }

    Ok(AnalyzeColumnProjection {
        projection: Projection::Columns(field_indices),
        ndv_columns_map,
        frequency_columns_map,
        kll_columns_map,
    })
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

#[async_trait::async_trait]
impl Processor for AnalyzeCollectNDVSource {
    fn name(&self) -> String {
        "AnalyzeCollectNDVSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
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

        if matches!(self.state, State::ReadData(None)) {
            if let Some(part) = self.ctx.get_partition() {
                self.state = State::ReadData(Some(part));
            } else {
                self.output
                    .push_data(Ok(DataBlock::empty_with_meta(AnalyzeNDVMeta::create(
                        self.row_count,
                        self.unstats_rows,
                        std::mem::take(&mut self.column_hlls),
                        std::mem::take(&mut self.top_n),
                        std::mem::take(&mut self.count_min_sketch),
                        std::mem::take(&mut self.dropped_top_n_columns)
                            .into_iter()
                            .collect(),
                        std::mem::take(&mut self.kll_histograms),
                    ))));
                self.state = State::Finish;
                return Ok(Event::NeedConsume);
            }
        }

        if matches!(
            self.state,
            State::ReadData(_) | State::BuildHLL | State::WriteMeta
        ) {
            Ok(Event::Async)
        } else {
            Ok(Event::Sync)
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::CollectNDV {
                segment_location,
                segment_info,
                mut block_hlls,
                mut block_top_ns,
            } => {
                let block_count = segment_info.summary.block_count as usize;
                block_hlls.resize(block_count, Vec::new());
                let mut merged_hlls: HashMap<ColumnId, MetaHLL> = HashMap::new();
                let collect_kll = !self.kll_columns_map.is_empty();
                let collect_top_n =
                    self.top_n_size.is_some() && !self.frequency_columns_map.is_empty();
                let collect_count_min_sketch = self.count_min_sketch_error_rate.is_some()
                    && !self.frequency_columns_map.is_empty();
                let reuse_top_n = self.top_n_size.is_some_and(|capacity| {
                    collect_top_n
                        && block_top_ns.len() == block_count
                        && block_top_ns.iter().all(|top_n| {
                            block_top_n_covers(top_n, &self.frequency_columns_map, capacity)
                        })
                });
                let rescan_all_blocks =
                    collect_kll || collect_count_min_sketch || (collect_top_n && !reuse_top_n);
                let refresh_top_n = collect_top_n && rescan_all_blocks;
                let mut block_indexes = if rescan_all_blocks {
                    (0..block_count).collect()
                } else {
                    Vec::new()
                };
                if !rescan_all_blocks && !self.ndv_columns_map.is_empty() {
                    for (idx, data) in block_hlls.iter().enumerate() {
                        if let Some(column_hlls) = decode_column_hll(data)? {
                            for (column_id, column_hll) in column_hlls.iter() {
                                merged_hlls
                                    .entry(*column_id)
                                    .and_modify(|hll| hll.merge(column_hll))
                                    .or_insert_with(|| column_hll.clone());
                            }
                        } else {
                            block_indexes.push(idx);
                        }
                    }
                }

                if reuse_top_n && !refresh_top_n {
                    if let Some(table_top_n) = &mut self.top_n {
                        for block_top_n in &block_top_ns {
                            let top_n =
                                project_block_top_n(block_top_n, &self.frequency_columns_map);
                            merge_column_top_n_mut(table_top_n, top_n)?;
                        }
                    }
                }

                if !block_indexes.is_empty()
                    && self.no_scan
                    && !collect_kll
                    && !collect_top_n
                    && !collect_count_min_sketch
                {
                    self.unstats_rows += segment_info.summary.row_count;
                    self.state = State::ReadData(None);
                    return Ok(());
                }

                for (column_id, column_hll) in merged_hlls {
                    self.column_hlls
                        .entry(column_id)
                        .and_modify(|hll| hll.merge(&column_hll))
                        .or_insert_with(|| column_hll);
                }
                self.row_count += segment_info.summary.row_count;

                if block_indexes.is_empty() {
                    self.state = State::ReadData(None);
                } else {
                    assert!(self.segment_with_hll.is_none());
                    let new_hlls = Vec::with_capacity(block_indexes.len());
                    block_top_ns.resize(block_count, HashMap::new());
                    self.segment_with_hll = Some(SegmentWithHLL {
                        segment_location,
                        block_metas: segment_info.block_metas()?,
                        origin_summary: segment_info.summary.clone(),
                        raw_block_hlls: block_hlls,
                        block_top_ns,
                        collect_top_n: refresh_top_n,
                        new_block_hlls: new_hlls,
                        new_block_top_n: Vec::with_capacity(block_indexes.len()),
                        new_block_count_min_sketch: Vec::with_capacity(block_indexes.len()),
                        block_indexes,
                    });
                    self.state = State::BuildHLL;
                }
            }
            State::MergeHLL => {
                let segment_with_hll = self.segment_with_hll.as_mut().unwrap();
                let new_hlls = std::mem::take(&mut segment_with_hll.new_block_hlls);
                let new_top_n = std::mem::take(&mut segment_with_hll.new_block_top_n);
                let new_count_min_sketch =
                    std::mem::take(&mut segment_with_hll.new_block_count_min_sketch);
                let new_indexes = std::mem::take(&mut segment_with_hll.block_indexes);
                for (((new, top_n), count_min_sketch), idx) in new_hlls
                    .into_iter()
                    .zip(new_top_n.into_iter())
                    .zip(new_count_min_sketch.into_iter())
                    .zip(new_indexes.into_iter())
                {
                    if let Some(column_hlls) = new {
                        for (column_id, column_hll) in column_hlls.iter() {
                            self.column_hlls
                                .entry(*column_id)
                                .and_modify(|hll| hll.merge(column_hll))
                                .or_insert_with(|| column_hll.clone());
                        }
                        segment_with_hll.raw_block_hlls[idx] = encode_column_hll(&column_hlls)?;
                    }
                    if segment_with_hll.collect_top_n {
                        if let Some(block_top_n) = &mut self.top_n {
                            let top_n = project_block_top_n(&top_n, &self.frequency_columns_map);
                            merge_column_top_n_mut(block_top_n, top_n)?;
                        }
                        segment_with_hll.block_top_ns[idx] = top_n;
                    }
                    if let Some(block_count_min_sketch) = &mut self.count_min_sketch {
                        merge_column_count_min_sketch_mut(block_count_min_sketch, count_min_sketch);
                    }
                }
                self.state = State::WriteMeta;
            }
            state => {
                return Err(ErrorCode::Internal(format!(
                    "Invalid state reached in sync process: {:?}. This is a bug.",
                    state
                )));
            }
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Finish) {
            State::ReadData(Some(part)) => {
                let part = FuseLazyPartInfo::from_part(&part)?;
                let (path, ver) = &part.segment_location;
                let load_param = LoadParams {
                    location: path.clone(),
                    len_hint: None,
                    ver: *ver,
                    put_cache: true,
                };
                let compact_segment_info = self.segment_reader.read(&load_param).await?;
                if *ver < 2 {
                    self.unstats_rows += compact_segment_info.summary.row_count;
                    self.state = State::ReadData(None);
                    return Ok(());
                }

                let block_count = compact_segment_info.summary.block_count as usize;
                let (block_hlls, block_top_ns) =
                    match compact_segment_info.summary.additional_stats_loc() {
                        Some((path, ver)) => {
                            let load_param = LoadParams {
                                location: path,
                                len_hint: None,
                                ver,
                                put_cache: true,
                            };
                            let stats = self.stats_reader.read(&load_param).await?;
                            (stats.block_hlls.clone(), stats.block_top_ns.clone())
                        }
                        _ => (vec![vec![]; block_count], Vec::new()),
                    };
                self.state = State::CollectNDV {
                    segment_location: part.segment_location.clone(),
                    segment_info: compact_segment_info,
                    block_hlls,
                    block_top_ns,
                };
            }
            State::BuildHLL => {
                let collect_top_n = self.segment_with_hll.as_ref().unwrap().collect_top_n;
                let block_top_n_template = collect_top_n.then(|| {
                    empty_block_top_n(&self.frequency_columns_map, self.top_n_size.unwrap())
                });
                let segment_with_hll = self.segment_with_hll.as_mut().unwrap();
                let runtime = GlobalIORuntime::instance();
                let mut handlers = Vec::with_capacity(segment_with_hll.block_indexes.len());
                for &idx in &segment_with_hll.block_indexes {
                    let permit = acquire_task_permit(self.io_request_semaphore.clone()).await?;
                    let block_reader = self.block_reader.clone();
                    let settings = self.settings;
                    let storage_format = self.storage_format;
                    let block_meta = segment_with_hll.block_metas[idx].clone();
                    let ndv_columns_map = self.ndv_columns_map.clone();
                    let frequency_columns_map = self.frequency_columns_map.clone();
                    let kll_columns_map = self.kll_columns_map.clone();
                    let histogram_info = self.histogram_info;
                    let top_n_size = collect_top_n.then_some(self.top_n_size).flatten();
                    let count_min_sketch_error_rate = self.count_min_sketch_error_rate;
                    let count_min_sketch = count_min_sketch_error_rate
                        .map(|error_rate| (frequency_columns_map.clone(), error_rate));
                    let handler = runtime.spawn(async move {
                        let block = block_reader
                            .read_by_meta(&settings, &block_meta, &storage_format)
                            .await?;
                        let top_n =
                            top_n_size.map(|top_n_size| (&frequency_columns_map, top_n_size));
                        let count_min_sketch = count_min_sketch
                            .as_ref()
                            .map(|(columns, error_rate)| (columns, *error_rate));
                        let mut builder =
                            BlockStatsBuilder::new(&ndv_columns_map, top_n, count_min_sketch)?;
                        builder.add_block(&block)?;
                        let column_hlls = builder.finalize_with_top_n()?;
                        let kll_histograms = build_kll_histograms(
                            &block,
                            &kll_columns_map,
                            histogram_info.kll_relative_error(),
                        )?;
                        drop(permit);
                        Ok::<_, ErrorCode>((column_hlls, kll_histograms))
                    });
                    handlers.push(handler);
                }

                let joint = futures::future::try_join_all(handlers).await.map_err(|e| {
                    ErrorCode::StorageOther(format!(
                        "[ANALYZE-TABLE] Failed to build NDV statistics: {}",
                        e
                    ))
                })?;
                let block_stats = joint.into_iter().collect::<Result<Vec<_>>>()?;
                let mut new_hlls = Vec::with_capacity(block_stats.len());
                let mut new_top_n = Vec::with_capacity(block_stats.len());
                let mut new_count_min_sketch = Vec::with_capacity(block_stats.len());
                for (column_hlls, kll_histograms) in block_stats {
                    let (hll, mut top_n, count_min_sketch, dropped_top_n) =
                        split_block_stats(column_hlls);
                    if let Some(template) = &block_top_n_template {
                        let mut complete_top_n = template.clone();
                        merge_column_top_n_mut(&mut complete_top_n, top_n)?;
                        top_n = complete_top_n;
                    }
                    for column_id in dropped_top_n {
                        self.dropped_top_n_columns.insert(column_id);
                        if let Some(top_n) = &mut self.top_n {
                            top_n.remove(&column_id);
                        }
                    }
                    new_hlls.push(hll);
                    new_top_n.push(top_n);
                    new_count_min_sketch.push(count_min_sketch);
                    for (column_id, sketch) in kll_histograms {
                        if let Some(existing) = self.kll_histograms.get_mut(&column_id) {
                            existing.merge(sketch)?;
                        } else {
                            self.kll_histograms.insert(column_id, sketch);
                        }
                    }
                }
                if new_hlls.iter().all(|v| v.is_none())
                    && new_top_n.iter().all(|top_n| top_n.is_empty())
                    && new_count_min_sketch
                        .iter()
                        .all(|count_min_sketch| count_min_sketch.is_empty())
                    && !collect_top_n
                {
                    self.segment_with_hll = None;
                    self.state = State::ReadData(None);
                } else {
                    segment_with_hll.new_block_hlls = new_hlls;
                    segment_with_hll.new_block_top_n = new_top_n;
                    segment_with_hll.new_block_count_min_sketch = new_count_min_sketch;
                    self.state = State::MergeHLL;
                }
            }
            State::WriteMeta => {
                let SegmentWithHLL {
                    segment_location,
                    block_metas,
                    mut origin_summary,
                    raw_block_hlls,
                    block_top_ns,
                    ..
                } = std::mem::take(&mut self.segment_with_hll).unwrap();

                let segment_loc = segment_location.0.as_str();
                let data = SegmentStatistics::new(raw_block_hlls, block_top_ns).to_bytes()?;
                let size = data.len() as u64;
                let segment_stats_location =
                    TableMetaLocationGenerator::gen_segment_stats_location_from_segment_location(
                        segment_loc,
                    );
                self.dal.write(&segment_stats_location, data).await?;
                // remove the old cache.
                if let Some(cache) = SegmentStatistics::cache() {
                    cache.evict(&segment_stats_location);
                }

                let additional_stats_meta = AdditionalStatsMeta {
                    size,
                    location: (segment_stats_location, SegmentStatistics::VERSION),
                    ..Default::default()
                };
                origin_summary.additional_stats_meta = Some(additional_stats_meta);
                let new_segment = SegmentInfo::new(block_metas, origin_summary);
                new_segment
                    .write_meta_through_cache(&self.dal, segment_loc)
                    .await?;
                self.state = State::ReadData(None);
            }
            state => {
                return Err(ErrorCode::Internal(format!(
                    "Invalid state reached in async process: {:?}. This is a bug.",
                    state
                )));
            }
        }
        Ok(())
    }
}

fn build_kll_histograms(
    block: &DataBlock,
    kll_columns_map: &BTreeMap<FieldIndex, TableField>,
    relative_error: Option<f64>,
) -> Result<HashMap<ColumnId, KllSketch>> {
    if kll_columns_map.is_empty() {
        return Ok(HashMap::new());
    }
    let Some(relative_error) = relative_error else {
        return Ok(HashMap::new());
    };

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

    use databend_common_expression::TableDataType;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;

    use super::*;

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
        let columns = BTreeMap::from([
            (
                0,
                TableField::new_from_column_id(
                    "a",
                    TableDataType::Number(NumberDataType::Int32),
                    10,
                ),
            ),
            (
                1,
                TableField::new_from_column_id(
                    "b",
                    TableDataType::Number(NumberDataType::Int32),
                    20,
                ),
            ),
        ]);
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
        let columns = BTreeMap::from([(
            0,
            TableField::new_from_column_id("a", TableDataType::Number(NumberDataType::Int32), 10),
        )]);
        let mut requested = ColumnTopN::with_capacity(8);
        requested.add(
            databend_common_expression::Scalar::Number(NumberScalar::Int32(1)).as_ref(),
            3,
        );
        let top_n = HashMap::from([(10, requested.clone()), (20, ColumnTopN::with_capacity(8))]);

        assert_eq!(
            project_block_top_n(&top_n, &columns),
            HashMap::from([(10, requested)])
        );
    }
}
