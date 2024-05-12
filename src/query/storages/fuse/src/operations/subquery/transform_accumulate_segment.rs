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
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_storages_common_table_meta::meta::Location;

use crate::operations::AbortOperation;
use crate::operations::CommitMeta;
use crate::operations::ConflictResolveContext;
use crate::operations::SnapshotChanges;

pub struct TransformAccumulateSegment {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    appended_segments: Vec<Location>,
    abort_operation: AbortOperation,
    table_id: u64,
    finished: bool,
}

impl TransformAccumulateSegment {
    pub fn new(input: Arc<InputPort>, output: Arc<OutputPort>, table_id: u64) -> Self {
        TransformAccumulateSegment {
            input,
            output,
            output_data: None,
            appended_segments: vec![],
            abort_operation: AbortOperation::default(),
            table_id,
            finished: false,
        }
    }

    fn read_meta(&mut self) -> Result<()> {
        let input_meta = self
            .input
            .pull_data()
            .unwrap()?
            .get_meta()
            .cloned()
            .ok_or_else(|| ErrorCode::Internal("No block meta. It's a bug"))?;

        let meta = CommitMeta::downcast_from(input_meta)
            .ok_or_else(|| ErrorCode::Internal("No commit meta. It's a bug"))?;

        if let ConflictResolveContext::ModifiedSegmentExistsInLatest(changes) =
            meta.conflict_resolve_context
        {
            self.abort_operation
                .segments
                .extend(meta.abort_operation.segments);
            self.abort_operation
                .blocks
                .extend(meta.abort_operation.blocks);
            self.abort_operation
                .bloom_filter_indexes
                .extend(meta.abort_operation.bloom_filter_indexes);
            self.appended_segments.extend(changes.appended_segments);
        } else {
            return Err(ErrorCode::Internal(
                "Error commit meta to TransformAccumulateSegment",
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Processor for TransformAccumulateSegment {
    fn name(&self) -> String {
        "TransformAccumulateSegment".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.finished {
            self.output.finish();
            return Ok(Event::Finished);
        }
        // 1. if there is no data and input_port is finished, this processor has finished
        // it's work
        if self.input.is_finished() {
            let changes = SnapshotChanges {
                appended_segments: self.appended_segments.clone(),
                ..Default::default()
            };
            let commit_meta = CommitMeta {
                conflict_resolve_context: ConflictResolveContext::ModifiedSegmentExistsInLatest(
                    changes,
                ),
                abort_operation: self.abort_operation.clone(),
                table_id: self.table_id,
            };
            let block_meta: BlockMetaInfoPtr = Box::new(commit_meta);
            let data_block = DataBlock::empty_with_meta(block_meta);

            if self.output.can_push() {
                self.output.push_data(Ok(data_block));
                self.finished = true;
                return Ok(Event::NeedConsume);
            } else {
                self.output_data = Some(data_block);
                return Ok(Event::Sync);
            }
        }

        // 2. process data stage here
        if self.output.can_push() && self.output_data.is_some() {
            if let Some(data) = self.output_data.take() {
                self.output.push_data(Ok(data));
                self.finished = true;
                return Ok(Event::NeedConsume);
            }
        }

        // 2. trigger down stream pipeItem to consume if we pushed data
        if self.input.has_data() {
            self.read_meta()?;
            return Ok(Event::Sync);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        Ok(())
    }
}
