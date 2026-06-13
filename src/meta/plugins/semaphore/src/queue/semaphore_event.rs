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

use crate::PermitEntry;
use crate::PermitSeq;
use crate::queue::event_desc::EventDesc;

// TODO: consider adding a state `Waiting` when a semaphore enters waiting queue?
/// The event of a semaphore permit,
/// acquired or removed(including either removed after it is acquired or before it is acquired).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PermitEvent {
    /// Removed, before or after Released, because the [`PermitEntry`] may be removed before it is acquired.
    Removed((PermitSeq, PermitEntry)),
    /// The [`PermitEntry`] enters the `acquired` queue.
    Acquired((PermitSeq, PermitEntry)),
}

impl fmt::Display for PermitEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PermitEvent::Removed((seq, entry)) => {
                write!(f, "Removed({}->{})", seq, entry)
            }
            PermitEvent::Acquired((seq, entry)) => {
                write!(f, "Acquired({}->{})", seq, entry)
            }
        }
    }
}

impl PermitEvent {
    pub(crate) fn new_removed(seq: PermitSeq, entry: PermitEntry) -> Self {
        PermitEvent::Removed((seq, entry))
    }

    pub(crate) fn new_acquired(seq: PermitSeq, entry: PermitEntry) -> Self {
        PermitEvent::Acquired((seq, entry))
    }

    /// Returns the type of the event and the sequence number.
    pub fn desc(&self) -> EventDesc {
        match self {
            PermitEvent::Removed((seq, _)) => EventDesc::new("D", *seq),
            PermitEvent::Acquired((seq, _)) => EventDesc::new("A", *seq),
        }
    }
}
