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

use databend_common_ast::ast::ColumnMatchMode;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::sources::EmptySource;
use databend_common_pipeline::sources::PrefetchAsyncSourcer;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_storage::init_stage_operator;

use super::block_builder::BlockBuilder;
use crate::read::load_context::LoadContext;
use crate::read::whole_file_reader::WholeFileReader;

pub struct LokiReadPipelineBuilder<'a> {
    pub(crate) stage_table_info: &'a StageTableInfo,
    pub(crate) compact_threshold: BlockThresholds,
}

impl LokiReadPipelineBuilder<'_> {
    pub fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        internal_columns: Vec<InternalColumn>,
    ) -> Result<()> {
        if plan.parts.is_empty() {
            pipeline.add_source(EmptySource::create, 1)?;
            return Ok(());
        }
        let FileFormatParams::Loki(params) = &self.stage_table_info.stage_info.file_format_params
        else {
            return Err(ErrorCode::Internal("expected Loki file format"));
        };
        ctx.set_partitions(plan.parts.clone())?;
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let num_sources = max_threads.min(plan.parts.len());
        let operator = init_stage_operator(&self.stage_table_info.stage_info)?;
        let load_ctx = Arc::new(LoadContext::try_create_for_copy(
            ctx.clone(),
            self.stage_table_info,
            None,
            self.compact_threshold,
            internal_columns,
        )?);
        if load_ctx.schema_evolution {
            return Err(ErrorCode::BadArguments(
                "LOKI does not support SCHEMA_EVOLUTION",
            ));
        }
        pipeline.add_source(
            |output| {
                let reader = WholeFileReader::try_create(ctx.clone(), operator.clone())?;
                PrefetchAsyncSourcer::create(ctx.get_scan_progress(), output, reader)
            },
            num_sources,
        )?;
        let case_sensitive = self
            .stage_table_info
            .copy_into_table_options
            .column_match_mode
            == Some(ColumnMatchMode::CaseSensitive);
        pipeline.try_add_accumulating_transformer(|| {
            Ok(BlockBuilder::create(
                load_ctx.clone(),
                params.clone(),
                case_sensitive,
            ))
        })?;
        Ok(())
    }
}
