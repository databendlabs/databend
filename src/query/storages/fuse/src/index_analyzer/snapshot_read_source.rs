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

use std::any::Any;
use std::iter::Zip;
use std::ops::Range;
use std::sync::Arc;
use std::vec::IntoIter;

use common_catalog::plan::StealablePartitions;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_core::Pipeline;
use common_pipeline_sources::SyncSource;
use common_pipeline_sources::SyncSourcer;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::CompactSegmentInfo;
use storages_common_table_meta::meta::Location;

use crate::fuse_lazy_part::FuseLazyPartInfo;
use crate::index_analyzer::segment_location_meta::SegmentLocationMetaInfo;
use crate::index_analyzer::segment_transform_read::SegmentReadTransform;
use crate::io::MetaReaders;
use crate::pruning::PruningContext;
use crate::pruning::SegmentLocation;

pub struct SnapshotReadSource {
    ctx: Arc<dyn TableContext>,
    snapshot_loc: Option<String>,
}

impl SnapshotReadSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        snapshot_loc: Option<String>,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx.clone(), output, SnapshotReadSource {
            ctx,
            snapshot_loc,
        })
    }
}

impl SyncSource for SnapshotReadSource {
    const NAME: &'static str = "LazyPartsSource";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        match self.ctx.get_partition() {
            None => Ok(None),
            Some(part) => match part.as_any().downcast_ref::<FuseLazyPartInfo>() {
                None => Ok(None),
                Some(info) => Ok(Some(DataBlock::empty_with_meta(
                    SegmentLocationMetaInfo::create(SegmentLocation {
                        segment_idx: info.segment_index,
                        location: info.segment_location.clone(),
                        snapshot_loc: self.snapshot_loc.clone(),
                    }),
                ))),
            },
        }
    }
}
