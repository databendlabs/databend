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

use std::io;

use databend_common_meta_types::raft_types::ErrorSubject;
use databend_common_meta_types::raft_types::ErrorVerb;

use crate::raft_log_v004::io_phase::IOPhase;

/// Describe the error that occurred during IO operations to RaftLog.
#[derive(Debug, thiserror::Error)]
#[error("RaftLogIOError: {verb}-{subject:?}: {ctx}: failed to {phase}; error: {error}")]
pub struct RaftLogIOError {
    pub subject: ErrorSubject,
    pub verb: ErrorVerb,
    pub phase: IOPhase,
    pub error: io::Error,
    pub ctx: String,
}
