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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::filter::build_select_expr;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::DynTransformBuilder;
use databend_common_sql::executor::physical_plans::ChunkAppendData;
use databend_common_sql::executor::physical_plans::ChunkCastSchema;
use databend_common_sql::executor::physical_plans::ChunkCommitInsert;
use databend_common_sql::executor::physical_plans::ChunkFillAndReorder;
use databend_common_sql::executor::physical_plans::ChunkFilter;
use databend_common_sql::executor::physical_plans::ChunkMerge;
use databend_common_sql::executor::physical_plans::ChunkProject;
use databend_common_sql::executor::physical_plans::Duplicate;
use databend_common_sql::executor::physical_plans::Filter;
use databend_common_sql::executor::physical_plans::Shuffle;

use crate::pipelines::processors::transforms::TransformFilter;
use crate::pipelines::PipelineBuilder;
impl PipelineBuilder {
    pub(crate) fn build_duplicate(&mut self, plan: &Duplicate) -> Result<()> {
        self.build_pipeline(&plan.input)?;
        self.main_pipeline.duplicate(false, plan.n)?;
        Ok(())
    }

    pub(crate) fn build_shuffle(&mut self, plan: &Shuffle) -> Result<()> {
        self.build_pipeline(&plan.input)?;
        self.main_pipeline
            .reorder_inputs(plan.strategy.shuffle(self.main_pipeline.output_len()));
        Ok(())
    }

    pub(crate) fn build_chunk_filter(&mut self, plan: &ChunkFilter) -> Result<()> {
        self.build_pipeline(&plan.input)?;
        if plan.predicates.iter().all(|x| x.is_none()) {
            return Ok(());
        }
        let mut f: Vec<DynTransformBuilder> = Vec::with_capacity(plan.predicates.len());
        for predicate in plan.predicates.iter() {
            if let Some(predicate) = predicate {
                f.push(Box::new(self.filter_transform_builder(
                    &vec![predicate.clone()],
                    HashSet::default(),
                )?));
            } else {
                f.push(Box::new(self.dummy_transform_builder()?));
            }
        }
        self.main_pipeline.add_transform_by_chunk(f)?;
        Ok(())
    }

    pub(crate) fn build_chunk_project(&mut self, plan: &ChunkProject) -> Result<()> {
        Ok(())
    }

    pub(crate) fn build_chunk_cast_schema(&mut self, plan: &ChunkCastSchema) -> Result<()> {
        Ok(())
    }

    pub(crate) fn build_chunk_fill_and_reorder(&mut self, plan: &ChunkFillAndReorder) -> Result<()> {
        Ok(())
    }

    pub(crate) fn build_chunk_merge(&mut self, plan: &ChunkMerge) -> Result<()> {
        Ok(())
    }

    pub(crate) fn build_chunk_append_data(&mut self, plan: &ChunkAppendData) -> Result<()> {
        Ok(())
    }

    pub(crate) fn build_chunk_commit_insert(&mut self, plan: &ChunkCommitInsert) -> Result<()> {
        Ok(())
    }
}
