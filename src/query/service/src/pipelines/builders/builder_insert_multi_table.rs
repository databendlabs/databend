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

use std::collections::HashMap;
use std::collections::HashSet;

use databend_common_catalog::catalog::CatalogManager;
use databend_common_exception::Result;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::DynTransformBuilder;
use databend_common_pipeline_sinks::AsyncSinker;
use databend_common_sql::executor::physical_plans::ChunkAppendData;
use databend_common_sql::executor::physical_plans::ChunkCastSchema;
use databend_common_sql::executor::physical_plans::ChunkCommitInsert;
use databend_common_sql::executor::physical_plans::ChunkFillAndReorder;
use databend_common_sql::executor::physical_plans::ChunkFilter;
use databend_common_sql::executor::physical_plans::ChunkMerge;
use databend_common_sql::executor::physical_plans::ChunkProject;
use databend_common_sql::executor::physical_plans::Duplicate;
use databend_common_sql::executor::physical_plans::Shuffle;
use databend_common_storages_fuse::operations::CommitMultiTableInsert;

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
                    &[predicate.clone()],
                    HashSet::default(),
                )?));
            } else {
                f.push(Box::new(self.dummy_transform_builder()?));
            }
        }
        self.main_pipeline.add_transform_by_chunk(f)?;
        Ok(())
    }

    pub(crate) fn build_chunk_project(&mut self, _plan: &ChunkProject) -> Result<()> {
        Ok(())
    }

    pub(crate) fn build_chunk_cast_schema(&mut self, _plan: &ChunkCastSchema) -> Result<()> {
        Ok(())
    }

    pub(crate) fn build_chunk_fill_and_reorder(
        &mut self,
        _plan: &ChunkFillAndReorder,
    ) -> Result<()> {
        Ok(())
    }

    pub(crate) fn build_chunk_merge(&mut self, plan: &ChunkMerge) -> Result<()> {
        self.build_pipeline(&plan.input)?;
        self.main_pipeline.chunk_merge(plan.chunk_num)?;
        Ok(())
    }

    pub(crate) fn build_chunk_append_data(&mut self, plan: &ChunkAppendData) -> Result<()> {
        self.build_pipeline(&plan.input)?;
        let mut compact_builders: Vec<DynTransformBuilder> =
            Vec::with_capacity(plan.target_tables.len());
        let mut serialize_block_builders: Vec<DynTransformBuilder> =
            Vec::with_capacity(plan.target_tables.len());
        for append_data in plan.target_tables.iter() {
            let table = self.ctx.build_table_by_table_info(
                &append_data.target_catalog_info,
                &append_data.target_table_info,
                None,
            )?;
            let block_thresholds = table.get_block_thresholds();
            compact_builders.push(Box::new(
                self.block_compact_transform_builder(block_thresholds)?,
            ));
            serialize_block_builders.push(Box::new(self.serialize_block_transform_builder(table)?));
        }
        self.main_pipeline
            .add_transform_by_chunk(compact_builders)?;
        self.main_pipeline
            .add_transform_by_chunk(serialize_block_builders)?;
        Ok(())
    }

    pub(crate) fn build_chunk_commit_insert(&mut self, plan: &ChunkCommitInsert) -> Result<()> {
        let ChunkCommitInsert {
            plan_id: _,
            input,
            update_stream_meta,
            overwrite,
            deduplicated_label,
            targets,
        } = plan;
        self.build_pipeline(input)?;
        let mut serialize_segment_builders: Vec<DynTransformBuilder> =
            Vec::with_capacity(targets.len());
        let mut mutation_aggregator_builders: Vec<DynTransformBuilder> =
            Vec::with_capacity(targets.len());
        let mut tables = HashMap::new();
        for target in targets {
            let table = self.ctx.build_table_by_table_info(
                &target.target_catalog_info,
                &target.target_table_info,
                None,
            )?;
            let block_thresholds = table.get_block_thresholds();
            serialize_segment_builders.push(Box::new(
                self.serialize_segment_transform_builder(table.clone(), block_thresholds)?,
            ));
            mutation_aggregator_builders.push(Box::new(
                self.mutation_aggregator_transform_builder(table.clone())?,
            ));
            tables.insert(table.get_id(), table);
        }
        self.main_pipeline
            .add_transform_by_chunk(serialize_segment_builders)?;
        self.main_pipeline
            .add_transform_by_chunk(mutation_aggregator_builders)?;
        self.main_pipeline.try_resize(1)?;
        let catalog = CatalogManager::instance().build_catalog(&targets[0].target_catalog_info)?;
        self.main_pipeline.add_sink(|input| {
            Ok(ProcessorPtr::create(AsyncSinker::create(
                input,
                self.ctx.clone(),
                CommitMultiTableInsert::create(
                    tables.clone(),
                    self.ctx.clone(),
                    *overwrite,
                    update_stream_meta.clone(),
                    deduplicated_label.clone(),
                    catalog.clone(),
                ),
            )))
        })?;
        Ok(())
    }
}
