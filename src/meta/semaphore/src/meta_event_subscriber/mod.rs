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

mod processor;
mod subscriber;

use std::fmt;

use chrono::Local;
use databend_common_base::display::display_option::DisplayOptionExt;
use databend_common_meta_types::SeqV;
pub(crate) use processor::Processor;
pub(crate) use subscriber::MetaEventSubscriber;

use crate::SemaphoreEntry;

/// A wrapper to implement `fmt::Display` for `Option<SeqV<SemaphoreEntry>>`.
#[allow(dead_code)]
struct DisplaySeqEntry<'a>(&'a Option<SeqV<SemaphoreEntry>>);

impl fmt::Display for DisplaySeqEntry<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            None => {
                write!(f, "None")
            }
            Some(seqv) => {
                write!(
                    f,
                    "(seq={}[{}]:{})",
                    seqv.seq,
                    seqv.meta.display(),
                    seqv.data
                )
            }
        }
    }
}

pub(crate) fn now_str() -> impl fmt::Display + 'static {
    Local::now().format("%Y-%m-%d %H:%M:%S%.3f")
}
