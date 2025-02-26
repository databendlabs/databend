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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_expression::local_block_meta_serde;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;

use crate::column_oriented_segment::AbstractSegment;
use crate::column_oriented_segment::ColumnOrientedSegment;
use crate::SegmentLocation;

pub struct PrunedCompactSegmentMeta {
    pub segments: (SegmentLocation, Arc<CompactSegmentInfo>),
}

impl Debug for PrunedCompactSegmentMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrunedSegmentMeta").finish()
    }
}

local_block_meta_serde!(PrunedCompactSegmentMeta);

#[typetag::serde(name = "pruned_segment_meta")]
impl BlockMetaInfo for PrunedCompactSegmentMeta {}

pub struct PrunedColumnOrientedSegmentMeta {
    pub segments: (SegmentLocation, Arc<ColumnOrientedSegment>),
}

impl Debug for PrunedColumnOrientedSegmentMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrunedColumnOrientedSegmentMeta").finish()
    }
}

local_block_meta_serde!(PrunedColumnOrientedSegmentMeta);

#[typetag::serde(name = "pruned_column_oriented_segment_meta")]
impl BlockMetaInfo for PrunedColumnOrientedSegmentMeta {}

pub trait PrunedSegmentMeta: Send + Sync + 'static {
    type Segment: AbstractSegment;
    fn create(segments: (SegmentLocation, Arc<Self::Segment>)) -> BlockMetaInfoPtr;
}

impl PrunedSegmentMeta for PrunedCompactSegmentMeta {
    type Segment = CompactSegmentInfo;
    fn create(segments: (SegmentLocation, Arc<CompactSegmentInfo>)) -> BlockMetaInfoPtr {
        Box::new(PrunedCompactSegmentMeta { segments })
    }
}

impl PrunedSegmentMeta for PrunedColumnOrientedSegmentMeta {
    type Segment = ColumnOrientedSegment;
    fn create(segments: (SegmentLocation, Arc<ColumnOrientedSegment>)) -> BlockMetaInfoPtr {
        Box::new(PrunedColumnOrientedSegmentMeta { segments })
    }
}
