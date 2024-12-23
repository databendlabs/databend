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

use std::fmt::Debug;

use databend_common_meta_types::sys_data::SysData;
use databend_common_meta_types::Change;

use crate::leveled_store::map_api::MapApi;
use crate::state_machine::ExpireKey;

/// Send a key-value change event to subscribers.
pub trait SMEventSender: Debug + Sync + Send {
    fn send(&self, change: Change<Vec<u8>, String>);
}

/// The API a state machine implements
pub trait StateMachineApi: Send + Sync {
    type Map: MapApi<String> + MapApi<ExpireKey> + 'static;

    fn get_expire_cursor(&self) -> ExpireKey;

    fn set_expire_cursor(&mut self, cursor: ExpireKey);

    /// Return a reference to the map that stores app data.
    fn map_ref(&self) -> &Self::Map;

    /// Return a mutable reference to the map that stores app data.
    fn map_mut(&mut self) -> &mut Self::Map;

    fn sys_data_mut(&mut self) -> &mut SysData;

    fn event_sender(&self) -> Option<&dyn SMEventSender>;
}
