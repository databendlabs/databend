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
use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::plan::{PushDownInfo, RuntimeFilterId};
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::Processor;

pub struct TransformRuntimeFilterPrunner {
    is_finished: bool,
    ctx: Arc<dyn TableContext>,
    table: Arc<dyn Table>,
    runtime_filter_ids: Vec<RuntimeFilterId>,
}

impl TransformRuntimeFilterPrunner {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        table: Arc<dyn Table>,
        runtime_filter_ids: Vec<RuntimeFilterId>,
    ) -> Box<dyn Processor> {
        Box::new(Self {
            is_finished: false,
            ctx,
            table,
            runtime_filter_ids,
        })
    }
}

#[async_trait::async_trait]
impl Processor for TransformRuntimeFilterPrunner {
    fn name(&self) -> String {
        "TransformRuntimeFilterPrunner".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if !self.is_finished {
            return Ok(Event::Async);
        }
        Ok(Event::Finished)
    }

    async fn async_process(&mut self) -> Result<()> {
        let mut filters = HashMap::new();
        let collector = self.ctx.get_runtime_filter_collector();
        loop {
            for id in self.runtime_filter_ids.iter() {
                if !filters.contains_key(id) {
                    if let Some(filter) = collector.get_filters()?.get(id) {
                        filters.insert(id, filter.clone());
                    }
                }
            }
            if filters.len() == self.runtime_filter_ids.len() {
                break
            }
        }
        let push_down = PushDownInfo {
            projection: None,
            // Test code
            filter: Some(filters.get(&self.runtime_filter_ids[0]).unwrap().clone()),
            prewhere: None,
            limit: None,
            order_by: vec![],
            runtime_filter_ids: None,
        };

        let (_, partitions) = self.table.read_partitions(self.ctx.clone(), Some(push_down)).await?;
        self.ctx.set_partitions(partitions)?;
        self.is_finished = true;
        Ok(())
    }
}
