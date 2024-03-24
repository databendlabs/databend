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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::filter::build_select_expr;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::DynTransformBuilder;
use databend_common_pipeline_transforms::processors::TransformDummy;
use databend_common_sql::executor::physical_plans::Filter;

use crate::pipelines::processors::transforms::TransformFilter;
use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::PipelineBuilder;
impl PipelineBuilder {
    pub(crate) fn filter_transform_builder(
        &self,
        predicates: &[RemoteExpr],
        projections: HashSet<usize>,
    ) -> Result<impl Fn(Arc<InputPort>, Arc<OutputPort>) -> Result<ProcessorPtr>> {
        let predicate = predicates
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
        assert_eq!(predicate.data_type(), &DataType::Boolean);

        let max_block_size = self.settings.get_max_block_size()? as usize;
        let (select_expr, has_or) = build_select_expr(&predicate).into();
        let fun_ctx = self.func_ctx.clone();
        Ok(move |input, output| {
            Ok(ProcessorPtr::create(TransformFilter::create(
                input,
                output,
                select_expr.clone(),
                has_or,
                projections.clone(),
                fun_ctx.clone(),
                max_block_size,
            )))
        })
    }

    pub(crate) fn dummy_transform_builder(
        &self,
    ) -> Result<impl Fn(Arc<InputPort>, Arc<OutputPort>) -> Result<ProcessorPtr>> {
        Ok(|input, output| Ok(TransformDummy::create(input, output)))
    }
}
