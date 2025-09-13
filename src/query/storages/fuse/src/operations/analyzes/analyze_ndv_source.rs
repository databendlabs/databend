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
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_storage::MetaHLL;
use databend_storages_common_cache::CompactSegmentInfo;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::decode_column_hll;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::RawBlockHLL;

use crate::io::read::meta::SegmentStatsReader;
use crate::io::CompactSegmentInfoReader;
use crate::operations::analyzes::AnalyzeExtraMeta;
use crate::operations::analyzes::AnalyzeNDVMeta;
use crate::operations::analyzes::AnalyzeSegmentMeta;
use crate::FuseLazyPartInfo;

enum State {
    ReadData(Option<PartInfoPtr>),
    CollectNDV {
        segment_location: Location,
        segment_info: Arc<CompactSegmentInfo>,
        block_hlls: Vec<RawBlockHLL>,
    },
    Finish,
}

pub struct ReadHllSource {
    state: State,
    output: Arc<OutputPort>,

    column_hlls: HashMap<ColumnId, MetaHLL>,
    row_count: u64,

    output_data: Option<DataBlock>,
    output_done: Option<Arc<AtomicBool>>,

    ctx: Arc<dyn TableContext>,
    segment_reader: CompactSegmentInfoReader,
    stats_reader: SegmentStatsReader,
}

impl ReadHllSource {
    fn is_done(&self) -> bool {
        self.output_done
            .as_ref()
            .map(|f| f.load(Ordering::Relaxed))
            .unwrap_or(false)
    }

    fn set_done(&self) {
        if let Some(flag) = &self.output_done {
            flag.store(true, Ordering::Relaxed);
        }
    }
}

#[async_trait::async_trait]
impl Processor for ReadHllSource {
    fn name(&self) -> String {
        "ReadHllSource".to_string()
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

        if let Some(block) = self.output_data.take() {
            self.output.push_data(Ok(block));
            return Ok(Event::NeedConsume);
        }

        if matches!(self.state, State::ReadData(None)) {
            if let Some(part) = self.ctx.get_partition() {
                self.state = State::ReadData(Some(part));
                Ok(Event::Async)
            } else {
                self.output
                    .push_data(Ok(DataBlock::empty_with_meta(Box::new(
                        AnalyzeNDVMeta::Extras(AnalyzeExtraMeta {
                            row_count: self.row_count,
                            column_hlls: std::mem::take(&mut self.column_hlls),
                        }),
                    ))));
                self.state = State::Finish;
                Ok(Event::NeedConsume)
            }
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

                if !indexes.is_empty() {
                    if self.is_done() {
                        return Ok(());
                    }
                    assert!(self.output_data.is_none());
                    self.output_data = Some(DataBlock::empty_with_meta(Box::new(
                        AnalyzeNDVMeta::Segment(AnalyzeSegmentMeta {
                            segment_location,
                            blocks: segment_info.raw_block_metas.clone(),
                            origin_summary: segment_info.summary.clone(),
                            raw_block_hlls: block_hlls,
                            block_hll_indexes: indexes,
                            merged_hlls,
                        }),
                    )));
                    self.set_done();
                } else {
                    for (column_id, column_hll) in merged_hlls {
                        self.column_hlls
                            .entry(column_id)
                            .and_modify(|hll| hll.merge(&column_hll))
                            .or_insert_with(|| column_hll);
                        self.row_count += segment_info.summary.row_count;
                    }
                    self.state = State::ReadData(None);
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
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
