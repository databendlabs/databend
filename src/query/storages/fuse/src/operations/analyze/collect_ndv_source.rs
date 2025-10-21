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

use databend_common_base::base::tokio::sync::Semaphore;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_storage::MetaHLL;
use databend_storages_common_cache::BlockMeta;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CachedObject;
use databend_storages_common_cache::CompactSegmentInfo;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_cache::SegmentStatistics;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::decode_column_hll;
use databend_storages_common_table_meta::meta::encode_column_hll;
use databend_storages_common_table_meta::meta::AdditionalStatsMeta;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::RawBlockHLL;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::Versioned;
use opendal::Operator;

use crate::io::build_column_hlls;
use crate::io::read::meta::SegmentStatsReader;
use crate::io::BlockReader;
use crate::io::CachedMetaWriter;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;
use crate::io::TableMetaLocationGenerator;
use crate::operations::acquire_task_permit;
use crate::operations::analyze::AnalyzeNDVMeta;
use crate::FuseLazyPartInfo;
use crate::FuseStorageFormat;
use crate::FuseTable;

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
    row_count: u64,
    unstats_rows: u64,
    no_scan: bool,

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

impl AnalyzeCollectNDVSource {
    pub fn try_create(
        output: Arc<OutputPort>,
        table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        io_request_semaphore: Arc<Semaphore>,
        no_scan: bool,
    ) -> Result<ProcessorPtr> {
        let table_schema = table.schema();
        let ndv_columns_map = table
            .approx_distinct_cols
            .distinct_column_fields(table_schema.clone(), RangeIndex::supported_table_type)?;
        let field_indices = ndv_columns_map.keys().cloned().collect();
        let projection = Projection::Columns(field_indices);
        let block_reader =
            table.create_block_reader(ctx.clone(), projection, false, false, false)?;

        // Rebuild `ndv_columns_map` so that keys correspond to the column order in the projection.
        //
        // The original `BTreeMap<FieldIndex, TableField>` is ordered by field index (ascending),
        // but after projection, columns are loaded in that order and accessed by *position*.
        // Therefore, we re-index it with `enumerate()` to align with block column offsets (0..N).
        let ndv_columns_map = ndv_columns_map.values().cloned().enumerate().collect();
        let dal = table.get_operator();
        let settings = ReadSettings::from_ctx(&ctx)?;
        let segment_reader = MetaReaders::segment_info_reader(dal.clone(), table_schema.clone());
        let stats_reader = MetaReaders::segment_stats_reader(dal.clone());
        Ok(ProcessorPtr::create(Box::new(Self {
            state: State::ReadData(None),
            output,
            io_request_semaphore,
            column_hlls: HashMap::new(),
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
            no_scan,
            unstats_rows: 0,
        })))
    }
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

                if !indexes.is_empty() && self.no_scan {
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

                if indexes.is_empty() {
                    self.state = State::ReadData(None);
                } else {
                    assert!(self.segment_with_hll.is_none());
                    let new_hlls = Vec::with_capacity(indexes.len());
                    self.segment_with_hll = Some(SegmentWithHLL {
                        segment_location,
                        block_metas: segment_info.block_metas()?,
                        origin_summary: segment_info.summary.clone(),
                        raw_block_hlls: block_hlls,
                        new_block_hlls: new_hlls,
                        block_indexes: indexes,
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
                )))
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
                )))
            }
        }
        Ok(())
    }
}
