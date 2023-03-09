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

use common_catalog::plan::RuntimeFilterId;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;

pub struct TransformRuntimeFilterPrunner {
    is_finished: bool,
    ctx: Arc<dyn TableContext>,
    runtime_filter_ids: Vec<RuntimeFilterId>,
}

impl TransformRuntimeFilterPrunner {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        runtime_filter_ids: Vec<RuntimeFilterId>,
    ) -> Box<dyn Processor> {
        Box::new(Self {
            is_finished: false,
            ctx,
            runtime_filter_ids,
        })
    }
}

impl Processor for TransformRuntimeFilterPrunner {
    fn name(&self) -> String {
        "TransformRuntimeFilterPrunner".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if !self.is_finished {
            return Ok(Event::Sync);
        }
        Ok(Event::Finished)
    }

    fn process(&mut self) -> Result<()> {
        self.is_finished = true;
        Ok(())
    }
}
