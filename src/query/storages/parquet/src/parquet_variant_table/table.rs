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
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_pipeline::core::Pipeline;
use databend_common_storage::init_stage_operator;

use crate::parquet_part::collect_parts;
use crate::parquet_variant_table::source::ParquetVariantSource;

pub struct ParquetVariantTable {}

impl ParquetVariantTable {
    #[async_backtrace::framed]
    pub async fn do_read_partitions(
        stage_table_info: &StageTableInfo,
        ctx: Arc<dyn TableContext>,
    ) -> Result<(PartStatistics, Partitions)> {
        let operator = init_stage_operator(&stage_table_info.stage_info)?;
        let thread_num = ctx.get_settings().get_max_threads()? as usize;
        let files = stage_table_info
            .files_info
            .list(&operator, thread_num, None)
            .await?
            .into_iter()
            .filter(|f| f.size > 0)
            .map(|f| (f.path.clone(), f.size, f.dedup_key()))
            .collect::<Vec<_>>();
        collect_parts(ctx, files, 1.0, 1, 1)
    }

    pub fn do_read_data(
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        let settings = ctx.get_settings();
        let max_threads = settings.get_max_threads()? as usize;
        let stage_table_info =
            if let DataSourceInfo::StageSource(stage_table_info) = &plan.source_info {
                stage_table_info
            } else {
                return Err(ErrorCode::Internal(
                    "bug: ParquetVariantTable::read_data must be called with StageSource",
                ));
            };
        let internal_columns = plan
            .internal_columns
            .as_ref()
            .map(|m| {
                m.values()
                    .map(|i| i.column_type.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let operator = Arc::new(init_stage_operator(&stage_table_info.stage_info)?);

        let use_logic_type = if let FileFormatParams::Parquet(parquet) =
            &stage_table_info.stage_info.file_format_params
        {
            parquet.use_logic_type
        } else {
            return Err(ErrorCode::Internal(
                "bug: ParquetVariantTable::read_data must be called with Parquet FileFormatParams",
            ));
        };

        ctx.set_partitions(plan.parts.clone())?;

        pipeline.add_source(
            |output| {
                ParquetVariantSource::try_create(
                    ctx.clone(),
                    output,
                    internal_columns.clone(),
                    operator.clone(),
                    use_logic_type,
                )
            },
            max_threads,
        )?;
        Ok(())
    }
}
