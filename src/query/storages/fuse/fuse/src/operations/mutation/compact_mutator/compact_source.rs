// Copyright 2022 Datafuse Labs.
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
use std::sync::Arc;

use common_base::base::Progress;
use common_exception::Result;
use common_planner::PartInfoPtr;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::SegmentInfo;

use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::Event;
use crate::pipelines::processors::Processor;

enum State {
    ReadData(Option<PartInfoPtr>),
    CompactBlock(PartInfoPtr),
    GenerateSegment(Vec<Arc<BlockMeta>>),
    SerializedSegment {
        data: Vec<u8>,
        location: String,
        segment: Arc<SegmentInfo>,
    },
    PreCommitSegment {
        location: String,
        segment: Arc<SegmentInfo>,
    },
    Finished,
}

pub struct CompactSource {
    state: State,
    scan_progress: Arc<Progress>,
    output: Arc<OutputPort>,
}

#[async_trait::async_trait]
impl Processor for CompactSource {
    fn name(&self) -> String {
        "CompactSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        todo!()
    }

    fn process(&mut self) -> Result<()> {
        todo!()
    }

    async fn async_process(&mut self) -> Result<()> {
        todo!()
    }
}
