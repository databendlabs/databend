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

use std::any::Any;

use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::ColumnSet;
use databend_common_sql::optimizer::ir::PExpr;
use databend_common_sql::optimizer::ir::StatContext;
use databend_common_sql::plans::PhysicalRelOperator;

use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalCteRuntimeFilter;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::PhysicalRuntimeFilter;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::SequenceFormatter;
use crate::physical_plans::runtime_filter::CteFilterPlanner;
use crate::physical_plans::runtime_filter::PlannedCteFilter;
use crate::physical_plans::runtime_filter::cast_producer_targets;
use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;

/// This is a binary operator that executes its children in order (left to right), and returns the results of the right child
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Sequence {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
    pub left: PhysicalPlan,
    pub right: PhysicalPlan,
    /// Runtime filters on the scans of the CTE producer in `left`, built by
    /// joins in `right`.
    #[serde(default)]
    pub cte_runtime_filters: Vec<PhysicalCteRuntimeFilter>,
    pub meta: PhysicalPlanMeta,
}

#[typetag::serde]
impl IPhysicalPlan for Sequence {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    #[recursive::recursive]
    fn output_schema(&self) -> Result<DataSchemaRef> {
        self.right.output_schema()
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.left).chain(std::iter::once(&self.right)))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.left).chain(std::iter::once(&mut self.right)))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 2);
        let right = children.pop().unwrap();
        let left = children.pop().unwrap();
        PhysicalPlan::new(Sequence {
            plan_id: self.plan_id,
            stat_info: self.stat_info.clone(),
            left,
            right,
            cte_runtime_filters: self.cte_runtime_filters.clone(),
            meta: self.meta.clone(),
        })
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(SequenceFormatter::create(self))
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        // Register before any join in `right` can publish a source filter.
        for filter in &self.cte_runtime_filters {
            builder.ctx.register_cte_runtime_filter(filter)?;
        }

        // init builder for cte pipeline
        let sub_context = QueryContext::create_from(builder.ctx.as_ref());
        let sub_builder = PipelineBuilder::create(
            builder.func_ctx.clone(),
            builder.settings.clone(),
            sub_context,
        );

        // build cte pipeline
        let build_res = sub_builder.finalize(&self.left)?;

        // add cte pipeline to pipelines
        builder.pipelines.push(build_res.main_pipeline);
        builder.pipelines.extend(build_res.sources_pipelines);

        // build main pipeline
        self.right.build_pipeline(builder)
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_sequence(
        &mut self,
        s_expr: &PExpr,
        _sequence: &databend_common_sql::plans::Sequence,
        stat_info: PlanStatsInfo,
        required: ColumnSet,
    ) -> Result<PhysicalPlan> {
        let planned = self.plan_cte_runtime_filters(s_expr)?;
        let left_side = self.build(s_expr.child(0)?, Default::default()).await?;
        let right_side = self.build(s_expr.child(1)?, required).await?;
        let cte_runtime_filters = self.finish_cte_runtime_filters(planned)?;
        Ok(PhysicalPlan::new(Sequence {
            plan_id: 0,
            stat_info: Some(stat_info),
            left: left_side,
            right: right_side,
            cte_runtime_filters,
            meta: PhysicalPlanMeta::new("Sequence"),
        }))
    }

    /// Plans the producer filters of the CTE defined by `sequence`, and asks
    /// the joins that build their keys to create a filter for them.
    fn plan_cte_runtime_filters(&mut self, sequence: &PExpr) -> Result<Vec<PlannedCteFilter>> {
        let settings = self.ctx.get_settings();
        if !settings.get_enable_join_runtime_filter()?
            || !settings.get_enable_materialized_cte_runtime_filter()?
        {
            return Ok(vec![]);
        }
        let PhysicalRelOperator::MaterializedCTE(cte) = sequence.child(0)?.plan() else {
            return Ok(vec![]);
        };
        let stat_ctx = StatContext::new(self.func_ctx.clone(), &settings);
        let plan = CteFilterPlanner {
            metadata: &self.metadata,
            func_ctx: &self.func_ctx,
            stat_ctx: &stat_ctx,
            producers: &self.cte_producers,
            producer_waits: &self.cte_producer_waits,
        }
        .plan(sequence)?;

        self.cte_producer_waits
            .insert(cte.cte_name.clone(), plan.producer_waits);
        for source in plan.filters.iter().flat_map(|filter| &filter.sources) {
            let conditions = self.cte_filter_requests.entry(source.join).or_default();
            if !conditions.contains(&source.condition) {
                conditions.push(source.condition);
            }
        }
        Ok(plan.filters)
    }

    /// Turns planned filters into physical filters. A filter is dropped if a
    /// source join did not build its filter, e.g. because it was planned as a
    /// range join, or if the sources do not share one key type.
    fn finish_cte_runtime_filters(
        &mut self,
        planned: Vec<PlannedCteFilter>,
    ) -> Result<Vec<PhysicalCteRuntimeFilter>> {
        let mut filters = Vec::with_capacity(planned.len());
        for planned in planned {
            let Some(sources) = planned
                .sources
                .iter()
                .map(|source| self.cte_filter_sources.get(source))
                .collect::<Option<Vec<_>>>()
            else {
                continue;
            };
            // The union concatenates the sources' keys, so they need one type.
            let key_type = sources[0]
                .build_key
                .as_expr(&BUILTIN_FUNCTIONS)
                .data_type()
                .clone();
            if sources.iter().any(|source| {
                source
                    .build_key
                    .as_expr(&BUILTIN_FUNCTIONS)
                    .data_type()
                    .remove_nullable()
                    != key_type.remove_nullable()
            }) {
                continue;
            }
            let Some(probe_targets) =
                cast_producer_targets(&planned.probe_targets, &key_type, &self.func_ctx)?
            else {
                continue;
            };

            let mut source_filters = Vec::with_capacity(sources.len());
            for source in &sources {
                if !source_filters.contains(&source.id) {
                    source_filters.push(source.id);
                }
            }
            let filter = PhysicalRuntimeFilter {
                id: self.metadata.write().next_runtime_filter_id(),
                build_key: sources[0].build_key.clone(),
                probe_targets,
                build_table_rows: sources.iter().map(|source| source.build_table_rows).sum(),
                probe_table_rows: None,
                enable_bloom_runtime_filter: sources
                    .iter()
                    .all(|source| source.enable_bloom_runtime_filter),
                enable_inlist_runtime_filter: sources
                    .iter()
                    .all(|source| source.enable_inlist_runtime_filter),
                enable_min_max_runtime_filter: sources
                    .iter()
                    .all(|source| source.enable_min_max_runtime_filter),
            };
            filters.push(PhysicalCteRuntimeFilter {
                cte_name: planned.cte_name,
                filter,
                source_filters,
            });
        }
        Ok(filters)
    }
}
