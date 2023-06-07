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

use common_exception::Result;
use common_expression::DataSchemaRef;
use common_expression::SortColumnDescription;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_profile::ProfSpanSetRef;

use super::transform_multi_sort_merge::try_add_multi_sort_merge;
use super::transform_sort_merge::try_create_transform_sort_merge;
use super::transform_sort_merge_limit::try_create_transform_sort_merge_limit;
use super::TransformSortPartial;
use crate::processors::ProfileWrapper;

pub fn build_full_sort_pipeline(
    pipeline: &mut Pipeline,
    input_schema: DataSchemaRef,
    sort_desc: Vec<SortColumnDescription>,
    limit: Option<usize>,
    block_size: usize,
    prof_info: Option<(u32, ProfSpanSetRef)>,
    after_exchange: bool,
) -> Result<()> {
    // Partial sort
    if limit.is_none() || !after_exchange {
        // If the sort plan is after an exchange plan, the blocks are already partially sorted on other nodes.
        pipeline.add_transform(|input, output| {
            let transform =
                TransformSortPartial::try_create(input, output, limit, sort_desc.clone())?;
            if let Some((plan_id, prof)) = &prof_info {
                Ok(ProcessorPtr::create(ProfileWrapper::create(
                    transform,
                    *plan_id,
                    prof.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(transform))
            }
        })?;
    }

    // Merge sort
    pipeline.add_transform(|input, output| {
        let transform = match limit {
            Some(limit) if limit <= block_size => try_create_transform_sort_merge_limit(
                input,
                output,
                input_schema.clone(),
                sort_desc.clone(),
                limit,
            )?,
            _ => try_create_transform_sort_merge(
                input,
                output,
                input_schema.clone(),
                block_size,
                limit,
                sort_desc.clone(),
            )?,
        };

        if let Some((plan_id, prof)) = &prof_info {
            Ok(ProcessorPtr::create(ProfileWrapper::create(
                transform,
                *plan_id,
                prof.clone(),
            )))
        } else {
            Ok(ProcessorPtr::create(transform))
        }
    })?;

    // Multi-pipelines merge sort
    try_add_multi_sort_merge(pipeline, input_schema, block_size, limit, sort_desc)
}
