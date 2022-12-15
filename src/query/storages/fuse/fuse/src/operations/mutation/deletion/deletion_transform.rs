// Copyright 2022 Datafuse Labs.
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

use common_cache::Cache;
use common_catalog::table_context::TableContext;
use common_datablocks::BlockCompactThresholds;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_table_meta::caches::CacheManager;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::SegmentInfo;
use common_storages_table_meta::meta::Statistics;
use opendal::Operator;

use crate::io::try_join_futures;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::operations::mutation::deletion::deletion_meta::DeletionSourceMeta;
use crate::operations::mutation::deletion::Deletion;
use crate::operations::mutation::AbortOperation;
use crate::operations::mutation::MutationMeta;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Processor;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::reducers::reduce_block_metas;

type DeletionMap = HashMap<usize, (Vec<(usize, Arc<BlockMeta>)>, Vec<usize>)>;

struct SerializedData {
    data: Vec<u8>,
    location: String,
    segment: Arc<SegmentInfo>,
}

enum State {
    None,
    GatherMeta(DataBlock),
    ReadSegments,
    GenerateSegments(Vec<Arc<SegmentInfo>>),
    SerializedSegments {
        serialized_data: Vec<SerializedData>,
        segments: Vec<Location>,
        summary: Statistics,
    },
    Output {
        segments: Vec<Location>,
        summary: Statistics,
    },
}

pub struct DeletionTransform {
    state: State,
    ctx: Arc<dyn TableContext>,
    dal: Operator,
    location_gen: TableMetaLocationGenerator,

    base_segments: Vec<Location>,
    thresholds: BlockCompactThresholds,
    abort_operation: AbortOperation,

    inputs: Vec<Arc<InputPort>>,
    input_metas: DeletionMap,
    cur_input_index: usize,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
}

impl DeletionTransform {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        inputs: Vec<Arc<InputPort>>,
        output: Arc<OutputPort>,
        dal: Operator,
        location_gen: TableMetaLocationGenerator,
        base_segments: Vec<Location>,
        thresholds: BlockCompactThresholds,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(DeletionTransform {
            state: State::None,
            ctx,
            dal,
            location_gen,
            base_segments,
            thresholds,
            abort_operation: AbortOperation::default(),
            inputs,
            input_metas: HashMap::new(),
            cur_input_index: 0,
            output,
            output_data: None,
        })))
    }

    fn get_current_input(&mut self) -> Option<Arc<InputPort>> {
        let mut finished = true;
        let mut index = self.cur_input_index;

        loop {
            let input = &self.inputs[index];

            if !input.is_finished() {
                finished = false;
                input.set_need_data();

                if input.has_data() {
                    self.cur_input_index = index;
                    return Some(input.clone());
                }
            }

            index += 1;
            if index == self.inputs.len() {
                index = 0;
            }

            if index == self.cur_input_index {
                return match finished {
                    true => Some(input.clone()),
                    false => None,
                };
            }
        }
    }

    async fn write_segments(&self, segments: Vec<SerializedData>) -> Result<()> {
        let mut handles = Vec::with_capacity(segments.len());
        for segment in segments {
            let op = self.dal.clone();
            handles.push(async move {
                op.object(&segment.location).write(segment.data).await?;
                if let Some(segment_cache) = CacheManager::instance().get_table_segment_cache() {
                    let cache = &mut segment_cache.write();
                    cache.put(segment.location.clone(), segment.segment.clone());
                }
                Ok::<_, ErrorCode>(())
            });
        }

        try_join_futures(
            self.ctx.clone(),
            handles,
            "deletion-write-segments-worker".to_owned(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Processor for DeletionTransform {
    fn name(&self) -> String {
        "DeletionTransform".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(
            self.state,
            State::GenerateSegments(_) | State::Output { .. }
        ) {
            return Ok(Event::Sync);
        }

        if matches!(self.state, State::SerializedSegments { .. }) {
            return Ok(Event::Async);
        }

        if self.output.is_finished() {
            for input in &self.inputs {
                input.finish();
            }
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        let current_input = self.get_current_input();
        if let Some(cur_input) = current_input {
            if cur_input.is_finished() {
                self.state = State::ReadSegments;
                return Ok(Event::Async);
            }

            self.state = State::GatherMeta(cur_input.pull_data().unwrap()?);
            cur_input.set_need_data();
            return Ok(Event::Sync);
        }
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::GatherMeta(input) => {
                let input_meta = input
                    .get_meta()
                    .cloned()
                    .ok_or_else(|| ErrorCode::Internal("No block meta. It's a bug"))?;
                let meta = DeletionSourceMeta::from_meta(&input_meta)?;
                match &meta.op {
                    Deletion::Replaced(block_meta) => {
                        self.input_metas
                            .entry(meta.index.0)
                            .and_modify(|v| v.0.push((meta.index.1, block_meta.clone())))
                            .or_insert((vec![(meta.index.1, block_meta.clone())], vec![]));
                        self.abort_operation.add_block(block_meta);
                    }
                    Deletion::Deleted => {
                        self.input_metas
                            .entry(meta.index.0)
                            .and_modify(|v| v.1.push(meta.index.1))
                            .or_insert((vec![], vec![meta.index.1]));
                    }
                    Deletion::DoNothing => (),
                }
            }
            State::GenerateSegments(segment_infos) => {
                let segments = self.base_segments.clone();
                let mut summary = Statistics::default();
                let mut serialized_data = Vec::with_capacity(self.input_metas.len());
                let mut segments_editor =
                    BTreeMap::<_, _>::from_iter(segments.into_iter().enumerate());
                for (seg_idx, seg_info) in segment_infos.iter().enumerate() {
                    if let Some((replaced, deleted)) = self.input_metas.get(&seg_idx) {
                        // prepare the new segment
                        let mut new_segment =
                            SegmentInfo::new(seg_info.blocks.clone(), seg_info.summary.clone());
                        // take away the blocks, they are being mutated
                        let mut block_editor = BTreeMap::<_, _>::from_iter(
                            std::mem::take(&mut new_segment.blocks)
                                .into_iter()
                                .enumerate(),
                        );

                        for (idx, new_meta) in replaced {
                            block_editor.insert(*idx, new_meta.clone());
                        }
                        for idx in deleted {
                            block_editor.remove(idx);
                        }
                        // assign back the mutated blocks to segment
                        new_segment.blocks = block_editor.into_values().collect();
                        if new_segment.blocks.is_empty() {
                            segments_editor.remove(&seg_idx);
                        } else {
                            // re-calculate the segment statistics
                            let new_summary =
                                reduce_block_metas(&new_segment.blocks, self.thresholds)?;
                            merge_statistics_mut(&mut summary, &new_summary)?;
                            new_segment.summary = new_summary;

                            let location = self.location_gen.gen_segment_info_location();
                            self.abort_operation.add_segment(location.clone());
                            segments_editor
                                .insert(seg_idx, (location.clone(), new_segment.format_version()));
                            serialized_data.push(SerializedData {
                                data: serde_json::to_vec(&new_segment)?,
                                location,
                                segment: Arc::new(new_segment),
                            });
                        }
                    } else {
                        merge_statistics_mut(&mut summary, &seg_info.summary)?;
                    }
                }

                // assign back the mutated segments to snapshot
                let segments = segments_editor.into_values().collect();
                self.state = State::SerializedSegments {
                    serialized_data,
                    segments,
                    summary,
                };
            }
            State::Output { segments, summary } => {
                let meta = MutationMeta::create(
                    segments,
                    summary,
                    std::mem::take(&mut self.abort_operation),
                );
                self.output_data = Some(DataBlock::empty_with_meta(meta));
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::ReadSegments => {
                // Read all segments information in parallel.
                let segments_io = SegmentsIO::create(self.ctx.clone(), self.dal.clone());
                let segment_locations = &self.base_segments;
                let segments = segments_io
                    .read_segments(segment_locations)
                    .await?
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;
                self.state = State::GenerateSegments(segments);
            }
            State::SerializedSegments {
                serialized_data,
                segments,
                summary,
            } => {
                self.write_segments(serialized_data).await?;
                self.state = State::Output { segments, summary };
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
