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
use common_pipeline_core::processors::processor::{Event, ProcessorPtr};
use common_pipeline_core::processors::Processor;
use common_pipeline_sources::{SyncSource, SyncSourcer};
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::CompactSegmentInfo;
use storages_common_table_meta::meta::Location;
use crate::fuse_lazy_part::FuseLazyPartInfo;

use crate::index_analyzer::segment_location_meta::SegmentLocationMetaInfo;
use crate::io::MetaReaders;
use crate::pruning::PruningContext;
use crate::pruning::SegmentLocation;

pub struct LazyPartsSource {
    ctx: Arc<dyn TableContext>,
    snapshot_loc: Option<String>,
}

impl LazyPartsSource {
    pub fn create(ctx: Arc<dyn TableContext>, output: Arc<OutputPort>, snapshot_loc: String) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx.clone(), output, LazyPartsSource { ctx, snapshot_loc: Some(snapshot_loc) })
    }
}

impl SyncSource for LazyPartsSource {
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
                    })
                )))
            }
        }
    }
}

// #[async_trait::async_trait]
// impl Processor for LazyPartsSource {
//     fn name(&self) -> String {
//         String::from("SnapsReaderSource")
//     }
//
//     fn as_any(&mut self) -> &mut dyn Any {
//         self
//     }
//
//     fn event(&mut self) -> Result<Event> {
//         if !self.inited {
//             self.inited = true;
//             return Ok(Event::Async);
//         }
//
//         if self.output.is_finished() {
//             return Ok(Event::Finished);
//         }
//
//         if !self.output.can_push() {
//             return Ok(Event::NeedConsume);
//         }
//
//         if let Some(into_iter) = self.output_data.as_mut() {
//             // TODO: maybe batch with segment_location
//             if let Some((location, segment_idx)) = into_iter.next() {
//                 let segment_location = SegmentLocation {
//                     segment_idx,
//                     location,
//                     // TODO: maybe remove snapshot_loc
//                     snapshot_loc: Some(self.snapshot_location.0.clone()),
//                 };
//
//                 self.output.push_data(Ok(DataBlock::empty_with_meta(
//                     SegmentLocationMetaInfo::create(segment_location),
//                 )));
//                 return Ok(Event::NeedConsume);
//             }
//         }
//
//         self.output.finish();
//         Ok(Event::Finished)
//     }
//
//     #[async_backtrace::framed]
//     async fn async_process(&mut self) -> Result<()> {
//         let reader = MetaReaders::table_snapshot_reader(self.get_operator());
//         let (loc, ver) = self.snapshot_location.clone();
//         let params = LoadParams {
//             location: loc,
//             len_hint: None,
//             ver,
//             put_cache: true,
//         };
//         let table_snapshot = reader.read(&params).await?;
//
//         self.output_data = Some(
//             table_snapshot
//                 .segments
//                 .drain(self.segments_range.clone())
//                 .collect::<Vec<_>>()
//                 .into_iter()
//                 .zip(self.segments_range.clone()),
//         );
//         Ok(())
//     }
// }
