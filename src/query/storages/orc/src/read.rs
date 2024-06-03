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

use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::EmptySource;
use databend_common_storage::init_stage_operator;

use super::OrcTable;
use crate::source::ORCSource;

impl OrcTable {
    #[inline]
    pub(super) fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        if plan.parts.is_empty() {
            // no file match
            pipeline.add_source(EmptySource::create, 1)?;
            return Ok(());
        };

        // let pos_projection = if let Some(PushDownInfo {
        //     projection: Some(Projection::Columns(columns)),
        //     ..
        // }) = &plan.push_downs
        // {
        //     Some(columns.clone())
        // } else {
        //     None
        // };
        let settings = ctx.get_settings();
        ctx.set_partitions(plan.parts.clone())?;

        let max_threads = settings.get_max_threads()? as usize;
        let num_source = max_threads.min(plan.parts.len());
        let operator = init_stage_operator(&self.stage_table_info.stage_info)?;
        let data_schema: DataSchema = self.stage_table_info.schema.clone().into();
        let data_schema = Arc::new(data_schema);
        pipeline.add_source(
            |output| {
                ORCSource::try_create(output, ctx.clone(), operator.clone(), data_schema.clone())
            },
            num_source,
        )?;
        Ok(())
    }
}
