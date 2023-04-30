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

pub(crate) use crate::state::raft_state_kv::compat_with_07::RaftStateValueCompat as RaftStateValue;
pub(crate) use crate::state_machine::log_meta::compat_with_07::LogMetaValueCompat as LogMetaValue;
pub(crate) use crate::state_machine::state_machine_meta::compat_with_07::StateMachineMetaValueCompat as StateMachineMetaValue;
