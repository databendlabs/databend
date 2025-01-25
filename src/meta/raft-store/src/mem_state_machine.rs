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

use databend_common_meta_types::sys_data::SysData;

use crate::leveled_store::level::Level;
use crate::state_machine::ExpireKey;
use crate::state_machine_api::SMEventSender;
use crate::state_machine_api::StateMachineApi;
/// A pure in-memory state machine as mock for testing.
#[derive(Debug, Default)]
pub struct MemStateMachine {
    level: Level,
    expire_cursor: ExpireKey,
}

impl StateMachineApi for MemStateMachine {
    type Map = Level;

    fn get_expire_cursor(&self) -> ExpireKey {
        self.expire_cursor
    }

    fn set_expire_cursor(&mut self, cursor: ExpireKey) {
        self.expire_cursor = cursor;
    }

    fn map_ref(&self) -> &Self::Map {
        &self.level
    }

    fn map_mut(&mut self) -> &mut Self::Map {
        &mut self.level
    }

    fn sys_data_mut(&mut self) -> &mut SysData {
        &mut self.level.sys_data
    }

    fn event_sender(&self) -> Option<&dyn SMEventSender> {
        None
    }
}
