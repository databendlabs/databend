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

use databend_common_exception::Result;
use databend_common_sql::executor::physical_plans::MaterializeCTERef;
use databend_common_storages_fuse::TableContext;

use crate::pipelines::processors::transforms::CTESource;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_cte_consumer(&mut self, cte: &MaterializeCTERef) -> Result<()> {
        let receiver = self.ctx.get_materialized_cte_receiver(&cte.cte_name);
        self.main_pipeline.add_source(
            |output_port| {
                CTESource::create(self.ctx.clone(), output_port.clone(), receiver.clone())
            },
            self.ctx.get_settings().get_max_threads()? as usize,
        )?;
        Ok(())
    }
}
