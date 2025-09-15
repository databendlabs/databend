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
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_storage::MetaHLL;
use databend_storages_common_cache::BlockMeta;
use databend_storages_common_cache::SegmentStatistics;
use databend_storages_common_cache::Table;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_io::ReadSettings;
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
use crate::io::BlockReader;
use crate::io::CachedMetaWriter;
use crate::io::TableMetaLocationGenerator;
use crate::operations::acquire_task_permit;
use crate::operations::analyzes::AnalyzeExtraMeta;
use crate::operations::analyzes::AnalyzeNDVMeta;
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
    Consume,
    MergeExtra(AnalyzeExtraMeta),
    BuildHLL,
    MergeHLL,
    WriteMeta,
}

pub struct CollectNDVTransform {
    state: State,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    io_request_semaphore: Arc<Semaphore>,
    column_hlls: HashMap<ColumnId, MetaHLL>,
    row_count: u64,

    segment_with_hll: Option<SegmentWithHLL>,
    called_on_finish: bool,

    block_reader: Arc<BlockReader>,
    dal: Operator,
    settings: ReadSettings,
    storage_format: FuseStorageFormat,
    ndv_columns_map: BTreeMap<FieldIndex, TableField>,
}

impl CollectNDVTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        io_request_semaphore: Arc<Semaphore>,
        ctx: Arc<dyn TableContext>,
    ) -> Result<ProcessorPtr> {
        let table_schema = table.schema();
        let ndv_columns_map = table
            .approx_distinct_cols
            .distinct_column_fields(table_schema.clone(), RangeIndex::supported_table_type)?;
        let field_indices = ndv_columns_map.keys().cloned().collect();
        let projection = Projection::Columns(field_indices);
        let block_reader =
            table.create_block_reader(ctx.clone(), projection, false, false, false)?;
        let dal = table.get_operator();
        let settings = ReadSettings::from_ctx(&ctx)?;
        Ok(ProcessorPtr::create(Box::new(Self {
            state: State::Consume,
            input,
            output,
            io_request_semaphore,
            column_hlls: HashMap::new(),
            row_count: 0,
            segment_with_hll: None,
            called_on_finish: false,
            block_reader,
            dal,
            settings,
            storage_format: table.get_storage_format(),
            ndv_columns_map,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for CollectNDVTransform {
    fn name(&self) -> String {
        "CollectNDVTransform".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::MergeExtra(_) | State::MergeHLL) {
            return Ok(Event::Sync);
        }
        if matches!(self.state, State::BuildHLL | State::WriteMeta) {
            return Ok(Event::Async);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if !self.input.has_data() {
            return match !self.called_on_finish {
                true => {
                    self.called_on_finish = true;
                    self.output
                        .push_data(Ok(DataBlock::empty_with_meta(Box::new(AnalyzeExtraMeta {
                            row_count: self.row_count,
                            column_hlls: std::mem::take(&mut self.column_hlls),
                        }))));
                    return Ok(Event::NeedConsume);
                }
                false => {
                    self.output.finish();
                    Ok(Event::Finished)
                }
            };
        }

        let mut input_data = self.input.pull_data().unwrap()?;
        if let Some(meta) = input_data.take_meta() {
            if let Some(meta) = AnalyzeNDVMeta::downcast_from(meta) {
                let extra = match meta {
                    AnalyzeNDVMeta::Extras(extra) => extra,
                    AnalyzeNDVMeta::Segment(info) => {
                        assert!(self.segment_with_hll.is_none());
                        let row_count = info.origin_summary.row_count;
                        let new_hlls = Vec::with_capacity(info.block_hll_indexes.len());
                        self.segment_with_hll = Some(SegmentWithHLL {
                            segment_location: info.segment_location,
                            block_metas: info.blocks.block_metas()?,
                            origin_summary: info.origin_summary,
                            raw_block_hlls: info.raw_block_hlls,
                            new_block_hlls: new_hlls,
                            block_indexes: info.block_hll_indexes,
                        });
                        AnalyzeExtraMeta {
                            row_count,
                            column_hlls: info.merged_hlls,
                        }
                    }
                };
                self.state = State::MergeExtra(extra);
                return Ok(Event::Sync);
            }
        }

        unreachable!()
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::MergeExtra(extra) => {
                self.row_count += extra.row_count;
                for (column_id, column_hll) in extra.column_hlls.iter() {
                    self.column_hlls
                        .entry(*column_id)
                        .and_modify(|hll| hll.merge(column_hll))
                        .or_insert_with(|| column_hll.clone());
                }
                if self.segment_with_hll.is_some() {
                    self.state = State::BuildHLL;
                }
            }
            State::MergeHLL => {
                let segment_with_hll = self.segment_with_hll.as_mut().unwrap();
                let new_hlls = std::mem::take(&mut segment_with_hll.new_block_hlls);
                let indexes = std::mem::take(&mut segment_with_hll.block_indexes);
                for (idx, new) in indexes.into_iter().zip(new_hlls.into_iter()) {
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
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
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
                    self.state = State::Consume;
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
                let segment_stats_location =
                    TableMetaLocationGenerator::gen_segment_stats_location_from_segment_location(
                        segment_loc,
                    );
                let additional_stats_meta = AdditionalStatsMeta {
                    size: data.len() as u64,
                    location: (segment_stats_location.clone(), SegmentStatistics::VERSION),
                    ..Default::default()
                };
                self.dal.write(&segment_stats_location, data).await?;
                origin_summary.additional_stats_meta = Some(additional_stats_meta);
                let new_segment = SegmentInfo::new(block_metas, origin_summary);
                new_segment
                    .write_meta_through_cache(&self.dal, segment_loc)
                    .await?;
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
