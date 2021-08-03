// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use serde::Deserialize;
use serde::Serialize;

use crate::MatchSeq;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ConflictSeq {
    NotMatch { want: MatchSeq, got: u64 },
}
