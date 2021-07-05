// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

use crate::SeqError;
use crate::SeqValue;

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

impl MatchSeq {
    /// Match against a SeqValue by checking if the seq in SeqValue satisfies the condition.
    /// On success it returns the value extracted from SeqValue.
    /// Otherwise it returns an Err() with the unmatched seq in it.
    pub fn match_seq_value<T>(&self, sv: SeqValue<T>) -> Result<T, SeqError> {
        self.match_seq(sv.0)?;
        Ok(sv.1)
    }

    pub fn match_seq(&self, seq: u64) -> Result<(), SeqError> {
        match self {
            MatchSeq::Any => Ok(()),
            MatchSeq::Exact(s) => {
                if seq == *s {
                    Ok(())
                } else {
                    Err(SeqError::NotMatch {
                        want: *self,
                        got: seq,
                    })
                }
            }
            MatchSeq::GE(s) => {
                if seq >= *s {
                    Ok(())
                } else {
                    Err(SeqError::NotMatch {
                        want: *self,
                        got: seq,
                    })
                }
            }
        }
    }
}
