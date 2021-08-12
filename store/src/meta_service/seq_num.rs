// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use serde::Deserialize;
use serde::Serialize;

use crate::meta_service::SledSerde;

/// Sequence number that is used in SledTree
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct SeqNum(pub(crate) u64);

impl std::ops::Add<u64> for SeqNum {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        SeqNum(self.0 + rhs)
    }
}

impl fmt::Display for SeqNum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<SeqNum> for u64 {
    fn from(sn: SeqNum) -> Self {
        sn.0
    }
}

impl SledSerde for SeqNum {}
