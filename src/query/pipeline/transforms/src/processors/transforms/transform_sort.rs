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
use common_pipeline_core::processors::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_profile::SharedProcessorProfiles;

use super::transform_multi_sort_merge::try_add_multi_sort_merge;
use super::transform_sort_merge::try_create_transform_sort_merge;
use super::transform_sort_merge_limit::try_create_transform_sort_merge_limit;
use super::TransformSortPartial;
use crate::processors::profile_wrapper::ProcessorProfileWrapper;

#[allow(clippy::too_many_arguments)]
pub fn build_full_sort_pipeline(
    pipeline: &mut Pipeline,
    input_schema: DataSchemaRef,
    sort_desc: Vec<SortColumnDescription>,
    limit: Option<usize>,
    partial_block_size: usize,
    final_block_size: usize,
    prof_info: Option<(u32, SharedProcessorProfiles)>,
    after_exchange: bool,
) -> Result<()> {
    // Partial sort
    if limit.is_none() || !after_exchange {
        // If the sort plan is after an exchange plan, the blocks are already partially sorted on other nodes.
        pipeline.add_transform(|input, output| {
            let transform =
                TransformSortPartial::try_create(input, output, limit, sort_desc.clone())?;
            if let Some((plan_id, prof)) = &prof_info {
                Ok(ProcessorPtr::create(ProcessorProfileWrapper::create(
                    transform,
                    *plan_id,
                    prof.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(transform))
            }
        })?;
    }

    build_merge_sort_pipeline(
        pipeline,
        input_schema,
        sort_desc,
        limit,
        partial_block_size,
        final_block_size,
        prof_info,
    )
}

pub fn build_merge_sort_pipeline(
    pipeline: &mut Pipeline,
    input_schema: DataSchemaRef,
    sort_desc: Vec<SortColumnDescription>,
    limit: Option<usize>,
    partial_block_size: usize,
    final_block_size: usize,
    prof_info: Option<(u32, SharedProcessorProfiles)>,
) -> Result<()> {
    // Merge sort
    let need_multi_merge = pipeline.output_len() > 1;
    pipeline.add_transform(|input, output| {
        let transform = match limit {
            Some(limit) => try_create_transform_sort_merge_limit(
                input,
                output,
                input_schema.clone(),
                sort_desc.clone(),
                partial_block_size,
                limit,
                need_multi_merge,
            )?,
            _ => try_create_transform_sort_merge(
                input,
                output,
                input_schema.clone(),
                partial_block_size,
                sort_desc.clone(),
                need_multi_merge,
            )?,
        };

        if let Some((plan_id, prof)) = &prof_info {
            Ok(ProcessorPtr::create(ProcessorProfileWrapper::create(
                transform,
                *plan_id,
                prof.clone(),
            )))
        } else {
            Ok(ProcessorPtr::create(transform))
        }
    })?;

    if need_multi_merge {
        // Multi-pipelines merge sort
        try_add_multi_sort_merge(pipeline, input_schema, final_block_size, limit, sort_desc)?;
    }

    Ok(())
}
