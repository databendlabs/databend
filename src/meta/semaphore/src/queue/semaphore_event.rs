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

use crate::SemaphoreEntry;
use crate::SemaphoreSeq;

// TODO: consider adding a state `Waiting` when a semaphore enters waiting queue?
/// The event of a semaphore, acquired or removed(removed after it is acquired or before it is acquired).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SemaphoreEvent {
    /// Removed not Released, because the semaphore may be removed before it is acquired.
    Removed((SemaphoreSeq, SemaphoreEntry)),
    /// The semaphore entry enters the `acquired`.
    Acquired((SemaphoreSeq, SemaphoreEntry)),
}

impl fmt::Display for SemaphoreEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SemaphoreEvent::Removed((seq, entry)) => {
                write!(f, "Removed({}->{})", seq, entry)
            }
            SemaphoreEvent::Acquired((seq, entry)) => {
                write!(f, "Acquired({}->{})", seq, entry)
            }
        }
    }
}

impl SemaphoreEvent {
    pub(crate) fn new_removed(seq: SemaphoreSeq, entry: SemaphoreEntry) -> Self {
        SemaphoreEvent::Removed((seq, entry))
    }

    pub(crate) fn new_acquired(seq: SemaphoreSeq, entry: SemaphoreEntry) -> Self {
        SemaphoreEvent::Acquired((seq, entry))
    }
}
