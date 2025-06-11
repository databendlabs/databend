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

use databend_common_base::base::tokio::sync::Barrier;
use databend_common_exception::Result;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sinks::Sinker;
use databend_common_sql::executor::physical_plans::HashJoin;
use databend_common_sql::executor::physical_plans::RangeJoin;
use databend_common_sql::executor::PhysicalPlan;

use crate::pipelines::processors::transforms::range_join::RangeJoinState;
use crate::pipelines::processors::transforms::range_join::TransformRangeJoinLeft;
use crate::pipelines::processors::transforms::range_join::TransformRangeJoinRight;
use crate::pipelines::processors::transforms::HashJoinBuildState;
use crate::pipelines::processors::transforms::HashJoinProbeState;
use crate::pipelines::processors::transforms::TransformHashJoinBuild;
use crate::pipelines::processors::transforms::TransformHashJoinProbe;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::HashJoinState;
use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;

impl PipelineBuilder {
    // Create a new pipeline builder with the same context as the current builder
    fn create_sub_pipeline_builder(&self) -> PipelineBuilder {
        let sub_context = QueryContext::create_from(self.ctx.as_ref());
        let mut sub_builder = PipelineBuilder::create(
            self.func_ctx.clone(),
            self.settings.clone(),
            sub_context,
            self.main_pipeline.get_scopes(),
        );
        sub_builder.hash_join_states = self.hash_join_states.clone();
        sub_builder
    }

    pub(crate) fn build_hash_join(&mut self, join: &HashJoin) -> Result<()> {
        // Get optimization flags for merge-into operations
        let (enable_optimization, is_distributed) = self.merge_into_get_optimization_flag(join);

        // Create the join state with optimization flags
        let state = self.build_hash_join_state(join, is_distributed, enable_optimization)?;
        if let Some((build_cache_index, _)) = join.build_side_cache_info {
            self.hash_join_states
                .insert(build_cache_index, state.clone());
        }

        // Build both phases of the Hash Join
        self.build_hash_join_build_side(&join.build, join, state.clone())?;
        self.build_hash_join_probe_side(join, state)?;

        // In the case of spilling, we need to share state among multiple threads
        // Quickly fetch all data from this round to quickly start the next round
        self.main_pipeline
            .resize(self.main_pipeline.output_len(), true)
    }

    // Create the Hash Join state
    fn build_hash_join_state(
        &mut self,
        join: &HashJoin,
        merge_into_is_distributed: bool,
        enable_merge_into_optimization: bool,
    ) -> Result<Arc<HashJoinState>> {
        HashJoinState::try_create(
            self.ctx.clone(),
            join.build.output_schema()?,
            &join.build_projections,
            HashJoinDesc::create(join)?,
            &join.probe_to_build,
            merge_into_is_distributed,
            enable_merge_into_optimization,
            join.build_side_cache_info.clone(),
        )
    }

    // Build the build-side pipeline for Hash Join
    fn build_hash_join_build_side(
        &mut self,
        build: &PhysicalPlan,
        hash_join_plan: &HashJoin,
        join_state: Arc<HashJoinState>,
    ) -> Result<()> {
        let build_side_builder = self.create_sub_pipeline_builder();
        let mut build_res = build_side_builder.finalize(build)?;

        assert!(build_res.main_pipeline.is_pulling_pipeline()?);
        let output_len = build_res.main_pipeline.output_len();
        let build_state = HashJoinBuildState::try_create(
            self.ctx.clone(),
            self.func_ctx.clone(),
            &hash_join_plan.build_keys,
            &hash_join_plan.build_projections,
            join_state.clone(),
            output_len,
            hash_join_plan.broadcast_id,
        )?;
        build_state.add_runtime_filter_ready();

        let create_sink_processor = |input| {
            Ok(ProcessorPtr::create(TransformHashJoinBuild::try_create(
                input,
                build_state.clone(),
            )?))
        };
        // For distributed merge-into when source as build side
        if hash_join_plan.need_hold_hash_table {
            self.join_state = Some(build_state.clone())
        }
        build_res.main_pipeline.add_sink(create_sink_processor)?;

        self.pipelines.push(build_res.main_pipeline.finalize());
        self.pipelines.extend(build_res.sources_pipelines);
        Ok(())
    }

    // Build the probe-side pipeline for Hash Join
    fn build_hash_join_probe_side(
        &mut self,
        join: &HashJoin,
        state: Arc<HashJoinState>,
    ) -> Result<()> {
        self.build_pipeline(&join.probe)?;

        let max_block_size = self.settings.get_max_block_size()? as usize;
        let barrier = Barrier::new(self.main_pipeline.output_len());
        let probe_state = Arc::new(HashJoinProbeState::create(
            self.ctx.clone(),
            self.func_ctx.clone(),
            state.clone(),
            &join.probe_projections,
            &join.probe_keys,
            join.probe.output_schema()?,
            &join.join_type,
            self.main_pipeline.output_len(),
            barrier,
        )?);

        self.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(TransformHashJoinProbe::create(
                input,
                output,
                join.projections.clone(),
                probe_state.clone(),
                max_block_size,
                self.func_ctx.clone(),
                &join.join_type,
                !join.non_equi_conditions.is_empty(),
            )?))
        })?;

        // For merge-into operations that need to hold the hash table
        if join.need_hold_hash_table {
            // Extract projected fields from probe schema
            let mut projected_fields = vec![];
            for (i, field) in probe_state.probe_schema.fields().iter().enumerate() {
                if probe_state.probe_projections.contains(&i) {
                    projected_fields.push(field.clone());
                }
            }
            self.merge_into_probe_data_fields = Some(projected_fields);
        }

        Ok(())
    }

    pub(crate) fn build_range_join(&mut self, range_join: &RangeJoin) -> Result<()> {
        let state = Arc::new(RangeJoinState::new(self.ctx.clone(), range_join));
        self.build_range_join_right_side(range_join, state.clone())?;
        self.build_range_join_left_side(range_join, state)?;
        Ok(())
    }

    // Build the left-side pipeline for Range Join
    fn build_range_join_left_side(
        &mut self,
        range_join: &RangeJoin,
        state: Arc<RangeJoinState>,
    ) -> Result<()> {
        self.build_pipeline(&range_join.left)?;
        let max_threads = self.settings.get_max_threads()? as usize;
        self.main_pipeline.try_resize(max_threads)?;
        self.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(TransformRangeJoinLeft::create(
                input,
                output,
                state.clone(),
            )))
        })?;
        Ok(())
    }

    // Build the right-side pipeline for Range Join
    fn build_range_join_right_side(
        &mut self,
        range_join: &RangeJoin,
        state: Arc<RangeJoinState>,
    ) -> Result<()> {
        let right_side_builder = self.create_sub_pipeline_builder();

        let mut right_res = right_side_builder.finalize(&range_join.right)?;
        right_res.main_pipeline.add_sink(|input| {
            Ok(ProcessorPtr::create(
                Sinker::<TransformRangeJoinRight>::create(
                    input,
                    TransformRangeJoinRight::create(state.clone()),
                ),
            ))
        })?;
        self.pipelines.push(right_res.main_pipeline.finalize());
        self.pipelines.extend(right_res.sources_pipelines);
        Ok(())
    }
}
