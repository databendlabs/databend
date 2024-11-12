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
use std::sync::Arc;

use databend_common_base::base::tokio::sync::Barrier;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sinks::Sinker;
use databend_common_sql::executor::physical_plans::HashJoin;
use databend_common_sql::executor::physical_plans::MaterializedCte;
use databend_common_sql::executor::physical_plans::RangeJoin;
use databend_common_sql::executor::physical_plans::RuntimeFilterSink;
use databend_common_sql::executor::physical_plans::RuntimeFilterSource;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::ColumnBinding;
use databend_common_sql::IndexType;

use crate::pipelines::processors::transforms::range_join::RangeJoinState;
use crate::pipelines::processors::transforms::range_join::TransformRangeJoinLeft;
use crate::pipelines::processors::transforms::range_join::TransformRangeJoinRight;
use crate::pipelines::processors::transforms::HashJoinBuildState;
use crate::pipelines::processors::transforms::HashJoinProbeState;
use crate::pipelines::processors::transforms::MaterializedCteSink;
use crate::pipelines::processors::transforms::MaterializedCteState;
use crate::pipelines::processors::transforms::TransformHashJoinBuild;
use crate::pipelines::processors::transforms::TransformHashJoinProbe;
use crate::pipelines::processors::transforms::TransformRuntimeFilterSink;
use crate::pipelines::processors::transforms::TransformRuntimeFilterSource;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::HashJoinState;
use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;

impl PipelineBuilder {
    pub(crate) fn build_range_join(&mut self, range_join: &RangeJoin) -> Result<()> {
        let state = Arc::new(RangeJoinState::new(self.ctx.clone(), range_join));
        self.expand_right_side_pipeline(range_join, state.clone())?;
        self.build_left_side(range_join, state)?;
        Ok(())
    }

    fn build_left_side(
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

    fn expand_right_side_pipeline(
        &mut self,
        range_join: &RangeJoin,
        state: Arc<RangeJoinState>,
    ) -> Result<()> {
        let right_side_context = QueryContext::create_from(self.ctx.clone());
        let mut right_side_builder = PipelineBuilder::create(
            self.func_ctx.clone(),
            self.settings.clone(),
            right_side_context,
            self.main_pipeline.get_scopes(),
        );
        right_side_builder.cte_state = self.cte_state.clone();
        right_side_builder.cte_scan_offsets = self.cte_scan_offsets.clone();
        right_side_builder.hash_join_states = self.hash_join_states.clone();
        right_side_builder.runtime_filter_columns = self.runtime_filter_columns.clone();

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

    pub(crate) fn build_join(&mut self, join: &HashJoin) -> Result<()> {
        // for merge into target table as build side.
        let (enable_merge_into_optimization, merge_into_is_distributed) =
            self.merge_into_get_optimization_flag(join);

        let state = self.build_join_state(
            join,
            merge_into_is_distributed,
            enable_merge_into_optimization,
        )?;
        if let Some((build_cache_index, _)) = join.build_side_cache_info {
            self.hash_join_states
                .insert(build_cache_index, state.clone());
        }
        if let Some(runtime_filter) = &join.runtime_filter {
            self.expand_runtime_filter_pipeline(runtime_filter, state.clone())?;
        }
        self.expand_build_side_pipeline(&join.build, join, state.clone())?;
        self.build_join_probe(join, state)?;

        // In the case of spilling, we need to share state among multiple threads. Quickly fetch all data from this round to quickly start the next round.
        self.main_pipeline
            .resize(self.main_pipeline.output_len(), true)
    }

    fn build_join_state(
        &mut self,
        join: &HashJoin,
        merge_into_is_distributed: bool,
        enable_merge_into_optimization: bool,
    ) -> Result<Arc<HashJoinState>> {
        let hash_join_state = HashJoinState::create(
            self.ctx.clone(),
            join.build.output_schema()?,
            &join.build_projections,
            HashJoinDesc::create(join)?,
            &join.probe_to_build,
            merge_into_is_distributed,
            enable_merge_into_optimization,
            join.build_side_cache_info.clone(),
            join.runtime_filter_source_fields.clone(),
        )?;
        self.ctx.set_hash_join_probe_statistics(
            join.hash_join_id,
            hash_join_state.probe_statistics.clone(),
        );
        Ok(hash_join_state)
    }

    fn expand_build_side_pipeline(
        &mut self,
        build: &PhysicalPlan,
        hash_join_plan: &HashJoin,
        join_state: Arc<HashJoinState>,
    ) -> Result<()> {
        let build_side_context = QueryContext::create_from(self.ctx.clone());
        let mut build_side_builder = PipelineBuilder::create(
            self.func_ctx.clone(),
            self.settings.clone(),
            build_side_context,
            self.main_pipeline.get_scopes(),
        );
        build_side_builder.cte_state = self.cte_state.clone();
        build_side_builder.cte_scan_offsets = self.cte_scan_offsets.clone();
        build_side_builder.hash_join_states = self.hash_join_states.clone();
        build_side_builder.runtime_filter_columns = self.runtime_filter_columns.clone();
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
        )?;

        let create_sink_processor = |input| {
            Ok(ProcessorPtr::create(TransformHashJoinBuild::try_create(
                input,
                build_state.clone(),
            )?))
        };
        build_res.main_pipeline.add_sink(create_sink_processor)?;

        self.pipelines.push(build_res.main_pipeline.finalize());
        self.pipelines.extend(build_res.sources_pipelines);
        Ok(())
    }

    fn expand_runtime_filter_pipeline(
        &mut self,
        runtime_filter: &PhysicalPlan,
        hash_join_state: Arc<HashJoinState>,
    ) -> Result<()> {
        let context = QueryContext::create_from(self.ctx.clone());
        let mut builder = PipelineBuilder::create(
            self.func_ctx.clone(),
            self.settings.clone(),
            context,
            self.main_pipeline.get_scopes(),
        );
        builder.runtime_filter_hash_join_state = Some(hash_join_state);
        let build_res = builder.finalize(runtime_filter)?;

        self.pipelines.push(build_res.main_pipeline.finalize());
        self.pipelines.extend(build_res.sources_pipelines);
        Ok(())
    }

    fn build_join_probe(&mut self, join: &HashJoin, state: Arc<HashJoinState>) -> Result<()> {
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

        if join.need_hold_hash_table {
            let mut projected_probe_fields = vec![];
            for (i, field) in probe_state.probe_schema.fields().iter().enumerate() {
                if probe_state.probe_projections.contains(&i) {
                    projected_probe_fields.push(field.clone());
                }
            }
            self.merge_into_probe_data_fields = Some(projected_probe_fields);
        }

        Ok(())
    }

    pub(crate) fn build_materialized_cte(
        &mut self,
        materialized_cte: &MaterializedCte,
    ) -> Result<()> {
        self.cte_scan_offsets.insert(
            materialized_cte.cte_idx,
            materialized_cte.cte_scan_offset.clone(),
        );
        self.expand_materialized_side_pipeline(
            &materialized_cte.right,
            materialized_cte.cte_idx,
            &materialized_cte.materialized_output_columns,
        )?;
        self.build_pipeline(&materialized_cte.left)
    }

    fn expand_materialized_side_pipeline(
        &mut self,
        materialized_side: &PhysicalPlan,
        cte_idx: IndexType,
        materialized_output_columns: &[ColumnBinding],
    ) -> Result<()> {
        let materialized_side_ctx = QueryContext::create_from(self.ctx.clone());
        let state = Arc::new(MaterializedCteState::new(self.ctx.clone()));
        self.cte_state.insert(cte_idx, state.clone());
        let mut materialized_side_builder = PipelineBuilder::create(
            self.func_ctx.clone(),
            self.settings.clone(),
            materialized_side_ctx,
            self.main_pipeline.get_scopes(),
        );
        materialized_side_builder.cte_state = self.cte_state.clone();
        materialized_side_builder.cte_scan_offsets = self.cte_scan_offsets.clone();
        materialized_side_builder.hash_join_states = self.hash_join_states.clone();
        materialized_side_builder.runtime_filter_columns = self.runtime_filter_columns.clone();
        let mut materialized_side_pipeline =
            materialized_side_builder.finalize(materialized_side)?;
        assert!(
            materialized_side_pipeline
                .main_pipeline
                .is_pulling_pipeline()?
        );

        PipelineBuilder::build_result_projection(
            &self.func_ctx,
            materialized_side.output_schema()?,
            materialized_output_columns,
            &mut materialized_side_pipeline.main_pipeline,
            false,
        )?;

        materialized_side_pipeline.main_pipeline.add_sink(|input| {
            let transform = Sinker::<MaterializedCteSink>::create(
                input,
                MaterializedCteSink::create(self.ctx.clone(), cte_idx, state.clone())?,
            );
            Ok(ProcessorPtr::create(transform))
        })?;
        self.pipelines
            .push(materialized_side_pipeline.main_pipeline.finalize());
        self.pipelines
            .extend(materialized_side_pipeline.sources_pipelines);
        Ok(())
    }

    pub(crate) fn build_runtime_filter_source(
        &mut self,
        _runtime_filter_source: &RuntimeFilterSource,
    ) -> Result<()> {
        let node_id = self.ctx.get_cluster().local_id.clone();
        let hash_join_state = self.runtime_filter_hash_join_state.clone().unwrap();
        self.main_pipeline.add_source(
            |output| {
                TransformRuntimeFilterSource::create(
                    output,
                    node_id.clone(),
                    hash_join_state.clone(),
                )
            },
            1,
        )
    }

    pub(crate) fn build_runtime_filter_sink(
        &mut self,
        runtime_filter_sink: &RuntimeFilterSink,
    ) -> Result<()> {
        self.build_pipeline(&runtime_filter_sink.input)?;
        self.main_pipeline.try_resize(1)?;

        let local_id = self.ctx.get_cluster().local_id.clone();
        let num_cluster_nodes = self.ctx.get_cluster().nodes.len();
        let mut is_collected = self
            .ctx
            .get_cluster()
            .nodes
            .iter()
            .map(|node| (node.id.clone(), false))
            .collect::<HashMap<_, _>>();
        is_collected.insert(local_id, true);

        let hash_join_state = self.runtime_filter_hash_join_state.clone().unwrap();
        let create_sink_processor = |input| {
            Ok(TransformRuntimeFilterSink::create(
                input,
                hash_join_state.clone(),
                num_cluster_nodes,
                is_collected.clone(),
            )?)
        };

        self.main_pipeline.add_sink(create_sink_processor)
    }
}
