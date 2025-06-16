// Copyright Qdrant
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

use std::cmp::Ordering;

use ordered_float::OrderedFloat;
use zerocopy::FromBytes;
use zerocopy::Immutable;
use zerocopy::IntoBytes;
use zerocopy::KnownLayout;

/// Type of vector matching score
pub type ScoreType = f32;
/// Type of point index inside a segment
pub type PointOffsetType = u32;

#[derive(Copy, Clone, PartialEq, Debug, Default, FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct ScoredPointOffset {
    pub idx: PointOffsetType,
    pub score: ScoreType,
}

impl Eq for ScoredPointOffset {}

impl Ord for ScoredPointOffset {
    fn cmp(&self, other: &Self) -> Ordering {
        OrderedFloat(self.score).cmp(&OrderedFloat(other.score))
    }
}

impl PartialOrd for ScoredPointOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
