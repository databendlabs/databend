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

use std::fmt;

use display_more::DisplayUnixTimeStampExt;

use crate::raft_types::LogId;
use crate::Time;

/// A context used when executing a [`Cmd`], to provide additional environment information.
///
/// [`Cmd`]: crate::Cmd
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CmdContext {
    time: Time,

    /// Current log id to apply
    log_id: LogId,
}

impl fmt::Display for CmdContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[time: {}, log_id: {}]",
            self.time.to_duration().display_unix_timestamp_short(),
            self.log_id
        )
    }
}

impl CmdContext {
    pub fn from_millis_and_log_id(millis: u64, log_id: LogId) -> Self {
        Self {
            time: Time::from_millis(millis),
            log_id,
        }
    }

    pub fn from_millis(millis: u64) -> Self {
        Self::new(Time::from_millis(millis))
    }

    pub fn new(time: Time) -> Self {
        CmdContext {
            time,
            log_id: LogId::default(),
        }
    }

    /// Returns the time since 1970-01-01 when this log is proposed by the leader.
    pub fn time(&self) -> Time {
        self.time
    }
}
