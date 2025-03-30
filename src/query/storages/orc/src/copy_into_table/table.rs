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

use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::EmptySource;
use databend_common_storage::init_stage_operator;
use databend_storages_common_stage::SingleFilePartition;

use crate::copy_into_table::meta::read_metas_in_parallel_for_copy;
use crate::copy_into_table::processors::decoder::StripeDecoderForCopy;
use crate::copy_into_table::processors::source::ORCSourceForCopy;
use crate::copy_into_table::projection::ProjectionFactory;
use crate::read_partition::read_partitions_simple;

pub struct OrcTableForCopy {}

impl OrcTableForCopy {
    #[async_backtrace::framed]
    pub async fn do_read_partitions(
        stage_table_info: &StageTableInfo,
        ctx: Arc<dyn TableContext>,
        _push_down: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let n = ctx.get_settings().get_max_threads()?;
        let fmt = match &stage_table_info.stage_info.file_format_params {
            FileFormatParams::Orc(fmt) => fmt,
            _ => unreachable!("do_read_partitions expect orc"),
        };
        let parts = read_partitions_simple(ctx, stage_table_info).await?;
        let projections = Arc::new(ProjectionFactory::try_create(
            stage_table_info.schema.clone(),
            stage_table_info.default_exprs.clone(),
            fmt.missing_field_as.clone(),
        )?);
        let op = init_stage_operator(&stage_table_info.stage_info)?;
        let mut files = vec![];
        for part in &parts.1.partitions {
            let file = SingleFilePartition::from_part(part)?.clone();
            files.push((file.path, file.size as u64))
        }
        read_metas_in_parallel_for_copy(&op, &files, n as usize, &projections).await?;
        Ok(parts)
    }

    pub fn do_read_data(
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        if plan.parts.is_empty() {
            // no file match
            pipeline.add_source(EmptySource::create, 1)?;
            return Ok(());
        };

        let stage_table_info =
            if let DataSourceInfo::StageSource(stage_table_info) = &plan.source_info {
                stage_table_info
            } else {
                return Err(ErrorCode::Internal(""));
            };

        let fmt = match &stage_table_info.stage_info.file_format_params {
            FileFormatParams::Orc(fmt) => fmt,
            _ => unreachable!("do_read_partitions expect orc"),
        };

        let settings = ctx.get_settings();
        ctx.set_partitions(plan.parts.clone())?;

        let max_threads = settings.get_max_threads()? as usize;
        let num_source = max_threads.min(plan.parts.len());
        let operator = init_stage_operator(&stage_table_info.stage_info)?;
        pipeline.add_source(
            |output| ORCSourceForCopy::try_create(output, ctx.clone(), operator.clone()),
            num_source,
        )?;
        pipeline.resize(max_threads, true)?;
        let projections = Arc::new(ProjectionFactory::try_create(
            stage_table_info.schema.clone(),
            stage_table_info.default_exprs.clone(),
            fmt.missing_field_as.clone(),
        )?);
        let output_data_schema = Arc::new(DataSchema::from(stage_table_info.schema()));
        pipeline.add_transform(|input, output| {
            let transformer = StripeDecoderForCopy::try_create(
                input,
                output,
                ctx.clone(),
                projections.clone(),
                output_data_schema.clone(),
            )?;
            Ok(ProcessorPtr::create(Box::new(transformer)))
        })?;
        Ok(())
    }
}
