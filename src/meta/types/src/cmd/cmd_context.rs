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

use crate::Time;

/// A context used when executing a [`Cmd`], to provide additional environment information.
///
/// [`Cmd`]: crate::Cmd
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CmdContext {
    time: Time,
}

impl CmdContext {
    pub fn from_millis(millis: u64) -> Self {
        Self::new(Time::from_millis(millis))
    }

    pub fn new(time: Time) -> Self {
        CmdContext { time }
    }

    /// Returns the time since 1970-01-01 when this log is proposed by the leader.
    pub fn time(&self) -> Time {
        self.time
    }
}
