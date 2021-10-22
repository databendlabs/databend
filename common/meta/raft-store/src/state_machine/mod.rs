// Copyright 2021 Datafuse Labs.
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

pub use applied_state::AppliedState;
pub use client_last_resp::ClientLastRespValue;
pub use sm::SerializableSnapshot;
pub use sm::SnapshotKeyValue;
pub use sm::StateMachine;
pub use snapshot::Snapshot;
pub use state_machine_meta::StateMachineMetaKey;
pub use state_machine_meta::StateMachineMetaValue;
pub use table_lookup::TableLookupKey;
pub use table_lookup::TableLookupValue;

pub mod applied_state;
pub mod client_last_resp;
pub mod sm;
pub mod snapshot;
pub mod state_machine_meta;
pub mod table_lookup;

pub mod placement;
#[cfg(test)]
mod placement_test;
#[cfg(test)]
mod state_machine_test;

// will be accessed by other crate, can not cfg(test)
pub mod testing;
