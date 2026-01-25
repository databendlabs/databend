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

/// Describe the phase during performing an IO.
///
/// This is used to provide more context in the error message.
#[derive(Debug)]
pub enum IOPhase {
    Submit,
    SubmitFlush,
    AwaitFlush,
    ReceiveFlushCallback,
    Done,
}

impl fmt::Display for IOPhase {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IOPhase::Submit => write!(f, "submit io"),
            IOPhase::SubmitFlush => write!(f, "submit flush"),
            IOPhase::AwaitFlush => write!(f, "await flush"),
            IOPhase::ReceiveFlushCallback => write!(f, "receive flush callback"),
            IOPhase::Done => write!(f, "done"),
        }
    }
}
