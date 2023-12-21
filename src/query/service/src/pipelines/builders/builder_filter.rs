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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_function;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::ProcessorProfileWrapper;
use databend_common_sql::executor::physical_plans::Filter;

use crate::pipelines::processors::transforms::TransformFilter;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_filter(&mut self, filter: &Filter) -> Result<()> {
        self.build_pipeline(&filter.input)?;

        let predicate = filter
            .predicates
            .iter()
            .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS))
            .try_reduce(|lhs, rhs| {
                check_function(None, "and_filters", &[], &[lhs, rhs], &BUILTIN_FUNCTIONS)
            })
            .transpose()
            .unwrap_or_else(|| {
                Err(ErrorCode::Internal(
                    "Invalid empty predicate list".to_string(),
                ))
            })?;

        self.main_pipeline.add_transform(|input, output| {
            let transform = TransformFilter::create(
                input,
                output,
                predicate.clone(),
                filter.projections.clone(),
                self.func_ctx.clone(),
            );

            if self.enable_profiling {
                Ok(ProcessorPtr::create(ProcessorProfileWrapper::create(
                    transform,
                    filter.plan_id,
                    self.proc_profs.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(transform))
            }
        })?;

        Ok(())
    }
}
