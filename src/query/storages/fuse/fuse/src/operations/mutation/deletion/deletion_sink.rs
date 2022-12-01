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
use std::sync::Arc;

use common_catalog::table::Table;
use common_exception::ErrorCode;
use common_exception::Result;
use common_catalog::table_context::TableContext;
use common_datablocks::BlockMetaInfos;
use common_storages_table_meta::meta::TableSnapshot;
use opendal::Operator;

use crate::io::TableMetaLocationGenerator;
use crate::operations::mutation::AbortOperation;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::Processor;

enum State {
    None,
    GatherDeletion,
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

    retries: u64,
    abort_operation: AbortOperation,

    inputs: Vec<Arc<InputPort>>,
    input_metas: BlockMetaInfos,
    cur_input_index: usize,
}

impl DeletionSink {
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
                self.state = State::GatherDeletion;
                return Ok(Event::Sync);
            }

            let input_meta = cur_input
                .pull_data()
                .unwrap()?
                .get_meta()
                .cloned()
                .ok_or_else(|| ErrorCode::Internal("No block meta. It's a bug"))?;
            self.input_metas.push(input_meta);
            cur_input.set_need_data();
        }
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::GatherDeletion => {
                
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}