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

use crate::PermitSeq;

/// A simple description of a received event.
///
/// Includes the type of the event and the sequence number of the permit in the event.
#[derive(Debug, Clone)]
pub struct EventDesc {
    pub typ: &'static str,
    pub seq: PermitSeq,
}

impl fmt::Display for EventDesc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.typ, self.seq)
    }
}

impl EventDesc {
    pub fn new(typ: &'static str, seq: PermitSeq) -> Self {
        Self { typ, seq }
    }
}
