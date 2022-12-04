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

use common_base::base::tokio::sync::Semaphore;
use common_base::base::Runtime;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datablocks::BlockCompactThresholds;
use common_datablocks::BlockMetaInfoPtr;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::SegmentInfo;
use common_storages_table_meta::meta::Statistics;
use common_storages_table_meta::meta::TableSnapshot;
use common_storages_table_meta::meta::Versioned;
use futures_util::future;
use opendal::Operator;

use super::deletion_meta::DeletionSourceMeta;
use crate::io::MetaReaders;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::operations::mutation::deletion::Deletion;
use crate::operations::mutation::AbortOperation;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::Processor;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::reducers::reduce_block_metas;

struct SegmentData {
    data: Vec<u8>,
    location: String,
    segment: Arc<SegmentInfo>,
}

enum State {
    None,
    ReadSegments,
    GenerateSegments(Vec<Arc<SegmentInfo>>),
    SerializedSegments(Vec<SegmentData>),
    TryCommit(TableSnapshot),
    AbortOperation,
    Finish,
}

pub struct DeletionSink {
    state: State,
    ctx: Arc<dyn TableContext>,
    dal: Operator,
    location_gen: TableMetaLocationGenerator,

    table: Arc<dyn Table>,
    base_snapshot: Arc<TableSnapshot>,
    thresholds: BlockCompactThresholds,
    // locations all the merged segments.
    merged_segments: Vec<Location>,
    // summarised statistics of all the merged segments.
    merged_statistics: Statistics,
    retries: u64,
    abort_operation: AbortOperation,

    inputs: Vec<Arc<InputPort>>,
    input_metas: HashMap<usize, (Vec<(usize, Arc<BlockMeta>)>, Vec<usize>)>,
    cur_input_index: usize,
}

impl DeletionSink {
    fn insert_meta(&mut self, input_meta: BlockMetaInfoPtr) -> Result<()> {
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
        Ok(())
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

    async fn write_segments(
        ctx: Arc<dyn TableContext>,
        dal: &'static Operator,
        segments: Vec<SegmentData>,
    ) -> Result<()> {
        let max_runtime_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;

        let mut handles = Vec::with_capacity(segments.len());
        for segment in segments {
            handles.push(async move { dal.object(&segment.location).write(segment.data).await });
        }

        // 1.2 build the runtime.
        let semaphore = Semaphore::new(max_io_requests);
        let segments_runtime = Arc::new(Runtime::with_worker_threads(
            max_runtime_threads,
            Some("deletion-write-segments-worker".to_owned()),
        )?);

        // 1.3 spawn all the tasks to the runtime.
        let join_handlers = segments_runtime.try_spawn_batch(semaphore, handles).await?;

        // 1.4 get all the result.
        if let Some(e) = future::try_join_all(join_handlers)
            .await
            .map_err(|e| ErrorCode::StorageOther(format!("write segments failure, {}", e)))?
            .iter()
            .find(|v| v.is_err())
        {
            Err(ErrorCode::StorageOther(format!(
                "write segments failure, {:?}",
                e
            )))
        } else {
            Ok(())
        }
    }
}

#[async_trait::async_trait]
impl Processor for DeletionSink {
    fn name(&self) -> String {
        "DeletionSink".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::Finish) {
            return Ok(Event::Finished);
        }

        let current_input = self.get_current_input();
        if let Some(cur_input) = current_input {
            if cur_input.is_finished() {
                self.state = State::ReadSegments;
                return Ok(Event::Async);
            }

            let input_meta = cur_input
                .pull_data()
                .unwrap()?
                .get_meta()
                .cloned()
                .ok_or_else(|| ErrorCode::Internal("No block meta. It's a bug"))?;
            self.insert_meta(input_meta)?;
            cur_input.set_need_data();
        }
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::GenerateSegments(segment_infos) => {
                let segments = self.base_snapshot.segments.clone();
                let mut new_segments = Vec::with_capacity(self.input_metas.len());
                let mut segments_editor =
                    BTreeMap::<_, _>::from_iter(segments.clone().into_iter().enumerate());
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
                            merge_statistics_mut(&mut self.merged_statistics, &new_summary)?;
                            new_segment.summary = new_summary;

                            let location = self.location_gen.gen_segment_info_location();
                            segments_editor
                                .insert(seg_idx, (location.clone(), new_segment.format_version()));
                            new_segments.push(SegmentData {
                                data: serde_json::to_vec(&new_segment)?,
                                location,
                                segment: Arc::new(new_segment),
                            });
                        }
                    } else {
                        merge_statistics_mut(&mut self.merged_statistics, &seg_info.summary)?;
                    }
                }

                // assign back the mutated segments to snapshot
                self.merged_segments = segments_editor.into_values().collect();
                self.state = State::SerializedSegments(new_segments);
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
                let segment_locations = &self.base_snapshot.segments;
                let segments = segments_io
                    .read_segments(segment_locations)
                    .await?
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;
                self.state = State::GenerateSegments(segments);
            }
            State::SerializedSegments(segments) => {
                Self::write_segments(self.ctx.clone(), &self.dal, segments).await?;
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
