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
use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::EmptySource;
use databend_common_pipeline_sources::PrefetchAsyncSourcer;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_storage::init_stage_operator;

use crate::read::avro::block_builder_processor::BlockBuilderProcessor;
use crate::read::load_context::LoadContext;
use crate::read::whole_file_reader::WholeFileReader;

pub struct AvroReadPipelineBuilder<'a> {
    pub(crate) stage_table_info: &'a StageTableInfo,
    pub(crate) compact_threshold: BlockThresholds,
}

impl AvroReadPipelineBuilder<'_> {
    pub fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        internal_columns: Vec<InternalColumn>,
    ) -> Result<()> {
        if plan.parts.is_empty() {
            // no file match
            pipeline.add_source(EmptySource::create, 1)?;
            return Ok(());
        };

        let pos_projection = if let Some(PushDownInfo {
            projection: Some(Projection::Columns(columns)),
            ..
        }) = &plan.push_downs
        {
            Some(columns.clone())
        } else {
            None
        };
        let settings = ctx.get_settings();
        ctx.set_partitions(plan.parts.clone())?;

        let max_threads = settings.get_max_threads()? as usize;
        let num_sources = std::cmp::min(max_threads, plan.parts.len());

        let avro_format = match &self.stage_table_info.stage_info.file_format_params {
            FileFormatParams::Avro(p) => p.clone(),
            _ => {
                unreachable!("expect avro FileFormatParams")
            }
        };

        let operator = init_stage_operator(&self.stage_table_info.stage_info)?;

        let load_ctx = Arc::new(LoadContext::try_create(
            ctx.clone(),
            self.stage_table_info,
            pos_projection,
            self.compact_threshold,
            internal_columns,
        )?);
        pipeline.add_source(
            |output| {
                let reader = WholeFileReader::try_create(ctx.clone(), operator.clone())?;
                PrefetchAsyncSourcer::create(ctx.clone(), output, reader)
            },
            num_sources,
        )?;

        pipeline.try_resize(max_threads)?;

        pipeline.try_add_accumulating_transformer(|| {
            BlockBuilderProcessor::create(load_ctx.clone(), avro_format.clone())
        })?;

        Ok(())
    }
}
