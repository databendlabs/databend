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
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::sources::EmptySource;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_storage::init_stage_operator;
use jiff::tz::TimeZone;

use super::OrcTable;
use crate::processors::decoder::StripeDecoder;
use crate::processors::source::ORCSource;
use crate::processors::variant_decoder::StripeDecoderForVariantTable;

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

        let settings = ctx.get_settings();
        ctx.set_partitions(plan.parts.clone())?;

        let max_threads = settings.get_max_threads()? as usize;
        let num_source = max_threads.min(plan.parts.len());
        let operator = init_stage_operator(&self.stage_table_info.stage_info)?;
        // No Projection
        let projection =
            PushDownInfo::projection_of_push_downs(&self.stage_table_info.schema, None);
        let data_schema: DataSchema = self.stage_table_info.schema.clone().into();
        let data_schema = Arc::new(data_schema);

        let internal_columns = plan
            .internal_columns
            .as_ref()
            .map(|m| {
                m.values()
                    .map(|i| i.column_type.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if let Some((arrow_schema, schema_from)) = &self.schema {
            pipeline.add_source(
                |output| {
                    ORCSource::try_create_with_schema(
                        output,
                        ctx.clone(),
                        Arc::new(operator.clone()),
                        arrow_schema.clone(),
                        Some(schema_from.clone()),
                        projection.clone(),
                    )
                },
                num_source,
            )?;
            pipeline.try_resize(max_threads)?;
            pipeline.add_accumulating_transformer(|| {
                StripeDecoder::new(
                    ctx.clone(),
                    data_schema.clone(),
                    arrow_schema.clone(),
                    internal_columns.clone(),
                )
            });
        } else {
            pipeline.add_source(
                |output| {
                    ORCSource::try_create(output, ctx.clone(), Arc::new(operator.clone()), None)
                },
                num_source,
            )?;
            pipeline.try_resize(max_threads)?;
            let settings = ctx.get_settings();
            let tz_string = settings.get_timezone()?;
            let tz = TimeZone::get(&tz_string).map_err(|e| {
                ErrorCode::InvalidTimezone(format!("Timezone validation failed: {}", e))
            })?;
            pipeline.add_accumulating_transformer(|| {
                StripeDecoderForVariantTable::new(ctx.clone(), tz.clone(), internal_columns.clone())
            });
        }
        Ok(())
    }
}
