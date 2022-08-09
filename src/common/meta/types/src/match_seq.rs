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

use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

use crate::ConflictSeq;
use crate::SeqV;

/// Describes what `seq` an operation must match to take effect.
/// Every value written to meta data has a unique `seq` bound.
/// Any conditioned or non-conditioned write operation can be done through the corresponding MatchSeq.
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
pub enum MatchSeq {
    /// Any value is acceptable, i.e. does not check seq at all.
    Any,

    /// To match an exact value of seq.
    /// E.g., CAS updates the exact version of some value,
    /// and put-if-absent adds a value only when seq is 0.
    Exact(u64),

    /// To match a seq that is greater-or-equal some value.
    /// E.g., GE(1) perform an update on any existent value.
    GE(u64),
}

impl From<Option<u64>> for MatchSeq {
    fn from(s: Option<u64>) -> Self {
        match s {
            None => MatchSeq::Any,
            Some(s) => MatchSeq::Exact(s),
        }
    }
}

impl Display for MatchSeq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MatchSeq::Any => {
                write!(f, "is any value")
            }
            MatchSeq::Exact(s) => {
                write!(f, "== {}", s)
            }
            MatchSeq::GE(s) => {
                write!(f, ">= {}", s)
            }
        }
    }
}

pub trait MatchSeqExt<T> {
    /// Match against a some value containing seq by checking if the seq satisfies the condition.
    fn match_seq(&self, sv: T) -> Result<(), ConflictSeq>;
}

impl<U> MatchSeqExt<&Option<SeqV<U>>> for MatchSeq {
    fn match_seq(&self, sv: &Option<SeqV<U>>) -> Result<(), ConflictSeq> {
        let seq = sv.as_ref().map_or(0, |sv| sv.seq);
        self.match_seq(seq)
    }
}

impl<U> MatchSeqExt<&SeqV<U>> for MatchSeq {
    fn match_seq(&self, sv: &SeqV<U>) -> Result<(), ConflictSeq> {
        let seq = sv.seq;
        self.match_seq(seq)
    }
}

impl MatchSeqExt<u64> for MatchSeq {
    fn match_seq(&self, seq: u64) -> Result<(), ConflictSeq> {
        match self {
            MatchSeq::Any => Ok(()),
            MatchSeq::Exact(s) if seq == *s => Ok(()),
            MatchSeq::GE(s) if seq >= *s => Ok(()),
            _ => Err(ConflictSeq::NotMatch {
                want: *self,
                got: seq,
            }),
        }
    }
}
