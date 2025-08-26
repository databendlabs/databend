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
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;

use crate::pipelines::processors::transforms::new_hash_join::build_join_keys;
use crate::pipelines::processors::transforms::new_hash_join::Join;
use crate::pipelines::processors::transforms::new_hash_join::JoinParams;
use crate::pipelines::processors::transforms::new_hash_join::ProbeData;
use crate::pipelines::processors::transforms::new_hash_join::Progress;
use crate::pipelines::processors::transforms::new_hash_join::TryCompleteFuture;
use crate::pipelines::processors::transforms::new_hash_join::TryCompleteStream;

enum HashJoinStream<T> {
    Stream(TryCompleteStream<T>),
    Future((TryCompleteFuture<T>, TryCompleteStream<T>)),
}

pub struct TransformHashJoin<T: Join> {
    build_input: Arc<InputPort>,
    probe_input: Arc<InputPort>,
    joined_output: Arc<OutputPort>,

    output_data: Option<DataBlock>,
    build_input_data: Option<DataBlock>,
    probe_input_data: Option<DataBlock>,

    build_finish: bool,
    probe_finish: bool,
    build_stream: Option<HashJoinStream<Progress>>,
    probe_stream: Option<HashJoinStream<ProbeData>>,

    join: Arc<T>,

    params: Arc<JoinParams>,
}

#[async_trait::async_trait]
impl<T: Join> Processor for TransformHashJoin<T> {
    fn name(&self) -> String {
        String::from("HashJoin")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.joined_output.is_finished() {
            self.build_input.finish();
            self.probe_input.finish();
            return Ok(Event::Finished);
        }

        if !self.joined_output.can_push() {
            self.build_input.set_not_need_data();
            self.probe_input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(output_data) = self.output_data.take() {
            self.joined_output.push_data(Ok(output_data));
            return Ok(Event::NeedConsume);
        }

        if let Some(stream) = self.build_stream.as_ref() {
            return match stream {
                HashJoinStream::Stream(_) => Ok(Event::Sync),
                HashJoinStream::Future((_, _)) => Ok(Event::Async),
            };
        }

        if self.build_input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.build_input.has_data() {
            let build_data = self.build_input.pull_data().unwrap()?;
            self.build_input_data = Some(build_data);
            return Ok(Event::Sync);
        }

        if !self.build_input.is_finished() {
            self.build_input.set_need_data();
            return Ok(Event::NeedData);
        }

        if !self.build_finish {
            self.build_finish = true;
            let stream = self.join.finish_build()?;
            self.build_stream = Some(HashJoinStream::Stream(stream));
            return Ok(Event::Sync);
        }

        if let Some(stream) = self.probe_stream.as_ref() {
            return match stream {
                HashJoinStream::Stream(_) => Ok(Event::Sync),
                HashJoinStream::Future((_, _)) => Ok(Event::Async),
            };
        }

        if self.probe_input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.probe_input.has_data() {
            let probe_data = self.probe_input.pull_data().unwrap()?;
            self.probe_input_data = Some(probe_data);
            return Ok(Event::Sync);
        }

        if self.probe_input.is_finished() {
            if !self.probe_finish {
                self.probe_finish = true;
                self.probe_stream = Some(HashJoinStream::Stream(self.join.finish_probe()?));
                return Ok(Event::Sync);
            }

            self.joined_output.finish();
            return Ok(Event::Finished);
        }

        self.probe_input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.build_input_data.take() {
            assert!(self.build_stream.is_none());
            let data_block = build_join_keys(data_block, &self.params)?;
            // TODO: fast stream compact
            self.build_stream = Some(HashJoinStream::Stream(self.join.add_block(data_block)?));
        }

        if let Some(build_stream) = self.build_stream.take() {
            let HashJoinStream::Stream(mut stream) = build_stream else {
                return Err(ErrorCode::Internal("Expect build stream"));
            };

            while let Some(mut try_future) = stream.next_try_complete()? {
                if let Some(_build_progress) = try_future.try_complete()? {
                    continue;
                }

                self.build_stream = Some(HashJoinStream::Future((try_future, stream)));
                return Ok(());
            }
        }

        if let Some(data_block) = self.probe_input_data.take() {
            assert!(self.probe_stream.is_none());

            // TODO: build join key and projection datablock
            self.probe_stream = Some(HashJoinStream::Stream(self.join.probe(data_block)?));
        }

        if let Some(probe_stream) = self.probe_stream.take() {
            let HashJoinStream::Stream(mut stream) = probe_stream else {
                return Err(ErrorCode::Internal("Expect probe stream"));
            };

            while let Some(mut try_future) = stream.next_try_complete()? {
                if let Some(probe_data) = try_future.try_complete()? {
                    match probe_data {
                        ProbeData::Next => {
                            continue;
                        }
                        ProbeData::DataBlock(joined_data) => {
                            self.output_data = Some(joined_data);
                            self.probe_stream = Some(HashJoinStream::Stream(stream));
                            return Ok(());
                        }
                    }
                }

                self.probe_stream = Some(HashJoinStream::Future((try_future, stream)));
                return Ok(());
            }
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if let Some(build_stream) = self.build_stream.take() {
            let HashJoinStream::Future((try_future, stream)) = build_stream else {
                return Err(ErrorCode::Internal("Expect build future"));
            };

            let _build_progress = try_future.await?;
            self.build_stream = Some(HashJoinStream::Stream(stream));
            return Ok(());
        }

        if let Some(probe_stream) = self.probe_stream.take() {
            let HashJoinStream::Future((try_future, stream)) = probe_stream else {
                return Err(ErrorCode::Internal("Expect probe future"));
            };

            match try_future.await? {
                ProbeData::Next => {
                    self.probe_stream = Some(HashJoinStream::Stream(stream));
                }
                ProbeData::DataBlock(joined_data) => {
                    self.output_data = Some(joined_data);
                    self.probe_stream = Some(HashJoinStream::Stream(stream));
                }
            }

            return Ok(());
        }

        Ok(())
    }
}
