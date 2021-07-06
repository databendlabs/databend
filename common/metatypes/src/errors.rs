// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::MatchSeq;

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum SeqError {
    #[error("seq not match, expect: {got} {want}")]
    NotMatch { want: MatchSeq, got: u64 },
}
