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

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_statistics::KllSketch;
use databend_common_statistics::KllSketchBuilder;
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
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::decode_column_hll;
use databend_storages_common_table_meta::meta::encode_column_hll;
use opendal::Operator;
use tokio::sync::Semaphore;

use crate::FuseLazyPartInfo;
use crate::FuseStorageFormat;
use crate::FuseTable;
use crate::io::BlockReader;
use crate::io::CachedMetaWriter;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;
use crate::io::TableMetaLocationGenerator;
use crate::io::build_column_hlls;
use crate::io::read::meta::SegmentStatsReader;
use crate::operations::acquire_task_permit;
use crate::operations::analyze::AnalyzeNDVMeta;

struct SegmentWithHLL {
    segment_location: Location,
    block_metas: Vec<Arc<BlockMeta>>,
    origin_summary: Statistics,
    raw_block_hlls: Vec<RawBlockHLL>,

    new_block_hlls: Vec<Option<BlockHLL>>,
    block_indexes: Vec<usize>,
}

enum State {
    ReadData(Option<PartInfoPtr>),
    CollectNDV {
        segment_location: Location,
        segment_info: Arc<CompactSegmentInfo>,
        block_hlls: Vec<RawBlockHLL>,
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
    kll_histograms: HashMap<ColumnId, KllSketch>,
    row_count: u64,
    unstats_rows: u64,
    no_scan: bool,
    histogram_info: AnalyzeCollectHistogramInfo,

    segment_with_hll: Option<SegmentWithHLL>,

    ctx: Arc<dyn TableContext>,
    block_reader: Arc<BlockReader>,
    dal: Operator,
    settings: ReadSettings,
    storage_format: FuseStorageFormat,
    segment_reader: CompactSegmentInfoReader,
    stats_reader: SegmentStatsReader,
    ndv_columns_map: BTreeMap<FieldIndex, TableField>,
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
    ) -> Result<ProcessorPtr> {
        let table_schema = table.schema();
        let (projection, ndv_columns_map, kll_columns_map) =
            build_analyze_column_projection(table, table_schema.clone(), histogram_info)?;
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
            kll_columns_map,
            histogram_info,
            no_scan,
            unstats_rows: 0,
        })))
    }
}

fn build_analyze_column_projection(
    table: &FuseTable,
    table_schema: TableSchemaRef,
    histogram_info: AnalyzeCollectHistogramInfo,
) -> Result<(
    Projection,
    BTreeMap<FieldIndex, TableField>,
    BTreeMap<FieldIndex, TableField>,
)> {
    let ndv_columns_map = table
        .approx_distinct_cols
        .distinct_column_fields(table_schema.clone(), RangeIndex::supported_table_type)?;
    let mut analyze_columns = BTreeMap::new();
    for (field_index, field) in ndv_columns_map {
        analyze_columns.insert(field_index, (Some(field), None));
    }
    if histogram_info.kll_relative_error().is_some() {
        for (index, field) in table_schema.fields().iter().enumerate() {
            if RangeIndex::supported_type(&field.data_type().into()) {
                analyze_columns
                    .entry(index)
                    .and_modify(|(_, kll_field)| *kll_field = Some(field.clone()))
                    .or_insert_with(|| (None, Some(field.clone())));
            }
        }
    }

    // Rebuild maps so that keys correspond to the column order in the projection.
    //
    // The original maps are ordered by table field index ascending, but after projection,
    // columns are loaded in that order and accessed by *position*. Therefore, re-index
    // both maps with the projected block offsets (0..N).
    let mut field_indices = Vec::with_capacity(analyze_columns.len());
    let mut ndv_columns_map = BTreeMap::new();
    let mut kll_columns_map = BTreeMap::new();
    for (field_index, (ndv_field, kll_field)) in analyze_columns {
        let offset = field_indices.len();
        field_indices.push(field_index);
        if let Some(field) = ndv_field {
            ndv_columns_map.insert(offset, field);
        }
        if let Some(field) = kll_field {
            kll_columns_map.insert(offset, field);
        }
    }

    Ok((
        Projection::Columns(field_indices),
        ndv_columns_map,
        kll_columns_map,
    ))
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
                block_hlls,
            } => {
                let mut indexes = vec![];
                let mut merged_hlls: HashMap<ColumnId, MetaHLL> = HashMap::new();
                for (idx, data) in block_hlls.iter().enumerate() {
                    let block_hll = decode_column_hll(data)?;
                    if let Some(column_hlls) = &block_hll {
                        for (column_id, column_hll) in column_hlls.iter() {
                            merged_hlls
                                .entry(*column_id)
                                .and_modify(|hll| hll.merge(column_hll))
                                .or_insert_with(|| column_hll.clone());
                        }
                    } else {
                        indexes.push(idx);
                    }
                }

                let collect_kll = !self.kll_columns_map.is_empty();
                if !indexes.is_empty() && self.no_scan && !collect_kll {
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

                if indexes.is_empty() && !collect_kll {
                    self.state = State::ReadData(None);
                } else {
                    assert!(self.segment_with_hll.is_none());
                    let block_indexes = if collect_kll {
                        (0..segment_info.summary.block_count as usize).collect()
                    } else {
                        indexes
                    };
                    let new_hlls = Vec::with_capacity(block_indexes.len());
                    self.segment_with_hll = Some(SegmentWithHLL {
                        segment_location,
                        block_metas: segment_info.block_metas()?,
                        origin_summary: segment_info.summary.clone(),
                        raw_block_hlls: block_hlls,
                        new_block_hlls: new_hlls,
                        block_indexes,
                    });
                    self.state = State::BuildHLL;
                }
            }
            State::MergeHLL => {
                let segment_with_hll = self.segment_with_hll.as_mut().unwrap();
                let new_hlls = std::mem::take(&mut segment_with_hll.new_block_hlls);
                let new_indexes = std::mem::take(&mut segment_with_hll.block_indexes);
                for (new, idx) in new_hlls.into_iter().zip(new_indexes.into_iter()) {
                    if let Some(column_hlls) = new {
                        for (column_id, column_hll) in column_hlls.iter() {
                            self.column_hlls
                                .entry(*column_id)
                                .and_modify(|hll| hll.merge(column_hll))
                                .or_insert_with(|| column_hll.clone());
                        }
                        segment_with_hll.raw_block_hlls[idx] = encode_column_hll(&column_hlls)?;
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
                let block_hlls = match compact_segment_info.summary.additional_stats_loc() {
                    Some((path, ver)) => {
                        let load_param = LoadParams {
                            location: path,
                            len_hint: None,
                            ver,
                            put_cache: true,
                        };
                        let stats = self.stats_reader.read(&load_param).await?;
                        stats.block_hlls.clone()
                    }
                    _ => vec![vec![]; block_count],
                };
                self.state = State::CollectNDV {
                    segment_location: part.segment_location.clone(),
                    segment_info: compact_segment_info,
                    block_hlls,
                };
            }
            State::BuildHLL => {
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
                    let kll_columns_map = self.kll_columns_map.clone();
                    let histogram_info = self.histogram_info;
                    let handler = runtime.spawn(async move {
                        let block = block_reader
                            .read_by_meta(&settings, &block_meta, &storage_format)
                            .await?;
                        let column_hlls = build_column_hlls(&block, &ndv_columns_map)?;
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
                for (column_hlls, kll_histograms) in block_stats {
                    new_hlls.push(column_hlls);
                    for (column_id, sketch) in kll_histograms {
                        if let Some(existing) = self.kll_histograms.get_mut(&column_id) {
                            existing.merge(sketch)?;
                        } else {
                            self.kll_histograms.insert(column_id, sketch);
                        }
                    }
                }
                if new_hlls.iter().all(|v| v.is_none()) {
                    self.segment_with_hll = None;
                    self.state = State::ReadData(None);
                } else {
                    segment_with_hll.new_block_hlls = new_hlls;
                    self.state = State::MergeHLL;
                }
            }
            State::WriteMeta => {
                let SegmentWithHLL {
                    segment_location,
                    block_metas,
                    mut origin_summary,
                    raw_block_hlls,
                    ..
                } = std::mem::take(&mut self.segment_with_hll).unwrap();

                let segment_loc = segment_location.0.as_str();
                let data = SegmentStatistics::new(raw_block_hlls).to_bytes()?;
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

    let mut builders = kll_columns_map
        .iter()
        .map(|(offset, field)| {
            Ok((
                *offset,
                field.column_id(),
                KllSketchBuilder::with_relative_error(relative_error)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    for (offset, _, builder) in builders.iter_mut() {
        match block.get_by_offset(*offset) {
            BlockEntry::Const(scalar, _, num_rows) => {
                if let Some(datum) = scalar.clone().to_datum() {
                    for _ in 0..*num_rows {
                        builder.insert(datum.clone())?;
                    }
                }
            }
            BlockEntry::Column(column) => {
                for row in 0..column.len() {
                    let Some(datum) = column
                        .index(row)
                        .and_then(|value| value.to_owned().to_datum())
                    else {
                        continue;
                    };
                    builder.insert(datum)?;
                }
            }
        }
    }

    let mut histograms = HashMap::with_capacity(builders.len());
    for (_, column_id, builder) in builders {
        let sketch = builder.build()?;
        if !sketch.is_empty() {
            histograms.insert(column_id, sketch);
        }
    }
    Ok(histograms)
}
