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
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::AsyncSinker;
use databend_common_sql::ColumnSet;
use databend_common_sql::ScalarExpr;
use databend_common_sql::Symbol;
use databend_common_sql::TypeCheck;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::Join;
use databend_common_sql::plans::SpatialJoinCandidate;

use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::TableScan;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanCast;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::processors::transforms::SpatialBuildSide;
use crate::pipelines::processors::transforms::SpatialIndexJoinState;
use crate::pipelines::processors::transforms::TransformSpatialIndexJoinBuild;
use crate::pipelines::processors::transforms::TransformSpatialIndexJoinProbe;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PhysicalSpatialIndexJoin {
    pub meta: PhysicalPlanMeta,
    pub probe: PhysicalPlan,
    pub build: PhysicalPlan,
    pub build_side: SpatialBuildSide,
    pub build_geometry: RemoteExpr,
    pub probe_geometry: RemoteExpr,
    pub predicates: Vec<RemoteExpr>,
    pub output_projection: Vec<usize>,
    pub output_schema: DataSchemaRef,
}

#[typetag::serde]
impl IPhysicalPlan for PhysicalSpatialIndexJoin {
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
        Ok(self.output_schema.clone())
    }

    fn children(&self) -> Box<dyn Iterator<Item = &'_ PhysicalPlan> + '_> {
        Box::new(std::iter::once(&self.probe).chain(std::iter::once(&self.build)))
    }

    fn children_mut(&mut self) -> Box<dyn Iterator<Item = &'_ mut PhysicalPlan> + '_> {
        Box::new(std::iter::once(&mut self.probe).chain(std::iter::once(&mut self.build)))
    }

    fn get_desc(&self) -> Result<String> {
        let predicates = self
            .predicates
            .iter()
            .map(|predicate| predicate.as_expr(&BUILTIN_FUNCTIONS).sql_display())
            .collect::<Vec<_>>()
            .join(" AND ");
        Ok(format!(
            "build side: {:?}, predicates: [{}]",
            self.build_side, predicates
        ))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 2);
        let build = children.pop().unwrap();
        let probe = children.pop().unwrap();
        PhysicalPlan::new(PhysicalSpatialIndexJoin {
            meta: self.meta.clone(),
            probe,
            build,
            build_side: self.build_side,
            build_geometry: self.build_geometry.clone(),
            probe_geometry: self.probe_geometry.clone(),
            predicates: self.predicates.clone(),
            output_projection: self.output_projection.clone(),
            output_schema: self.output_schema.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let max_block_size = builder.settings.get_max_block_size()? as usize;
        let state = SpatialIndexJoinState::create(
            builder.func_ctx.clone(),
            self.build_geometry.clone(),
            self.probe_geometry.clone(),
            self.predicates.clone(),
            self.output_projection.clone(),
            self.build_side,
            max_block_size,
        );

        self.build_side_pipeline(state.clone(), builder)?;
        self.probe.build_pipeline(builder)?;
        builder.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(
                TransformSpatialIndexJoinProbe::create(input, output, state.clone()),
            ))
        })
    }
}

impl PhysicalSpatialIndexJoin {
    fn build_side_pipeline(
        &self,
        state: std::sync::Arc<SpatialIndexJoinState>,
        builder: &mut PipelineBuilder,
    ) -> Result<()> {
        let build_builder = builder.create_sub_pipeline_builder();
        let mut build_res = build_builder.finalize(&self.build)?;

        build_res.main_pipeline.resize(1, true)?;
        build_res.main_pipeline.add_sink(|input| {
            Ok(ProcessorPtr::create(AsyncSinker::create(
                input,
                TransformSpatialIndexJoinBuild::create(state.clone()),
            )))
        })?;

        builder
            .pipelines
            .push(build_res.main_pipeline.finalize(None));
        builder.pipelines.extend(build_res.sources_pipelines);
        Ok(())
    }
}

impl PhysicalPlanBuilder {
    /// Whether the join's *shape* is one the spatial index path supports. This
    /// only inspects the join/candidate structure; row/byte-size admission is
    /// handled separately in `try_build_spatial_index_join`.
    fn is_spatial_join_eligible(&self, join: &Join, candidate: &SpatialJoinCandidate) -> bool {
        // The spatial index join is node-local, takes no equi key, and cannot
        // serve the single-to-inner rewrite.
        if !self.ctx.get_cluster().is_empty()
            || join.single_to_inner.is_some()
            || !join.equi_conditions.is_empty()
        {
            return false;
        }
        // CacheScan on the build side depends on state registered by the hash join path.
        if join.build_side_cache_info.is_some() {
            return false;
        }
        // Residual predicates can short-circuit throwing spatial predicates in
        // the fallback path. Keep those joins out of the spatial path until it
        // can preserve that evaluation order.
        if !candidate.residual_predicates.is_empty() {
            return false;
        }
        true
    }

    pub async fn try_build_spatial_index_join(
        &mut self,
        join: &Join,
        candidate: SpatialJoinCandidate,
        s_expr: &SExpr,
        required: ColumnSet,
        left_required: ColumnSet,
        right_required: ColumnSet,
    ) -> Result<Option<PhysicalPlan>> {
        if !self.is_spatial_join_eligible(join, &candidate) {
            return Ok(None);
        }

        let left_cardinality = RelExpr::with_s_expr(s_expr.left_child())
            .derive_cardinality()?
            .cardinality;
        let right_cardinality = RelExpr::with_s_expr(s_expr.right_child())
            .derive_cardinality()?
            .cardinality;

        let (build_side, build_rows) = if left_cardinality <= right_cardinality {
            (SpatialBuildSide::Left, left_cardinality)
        } else {
            (SpatialBuildSide::Right, right_cardinality)
        };

        if build_rows > self.ctx.get_settings().get_spatial_join_max_build_rows()? as f64 {
            return Ok(None);
        }

        // Byte admission needs the column-pruned `read_bytes`, which only exists
        // after the scans are built. We build first and reject afterwards (the
        // caller then falls back and rebuilds the same sub-plans, which is safe):
        // the only pre-build estimate is the whole-table `data_size`, which would
        // wrongly reject build sides that read just the geometry column.
        let (left_input, right_input) = self
            .build_join_sides(s_expr, left_required, right_required)
            .await?;

        let build_bytes = match build_side {
            SpatialBuildSide::Left => collect_scan_bytes(&left_input),
            SpatialBuildSide::Right => collect_scan_bytes(&right_input),
        };
        if !self.spatial_build_bytes_allowed(build_bytes)? {
            return Ok(None);
        }

        let left_schema = left_input.output_schema()?;
        let right_schema = right_input.output_schema()?;
        let left_user_columns = left_schema.num_fields();
        let right_user_columns = right_schema.num_fields();
        let merged_schema = merged_user_schema(
            left_schema.clone(),
            right_schema.clone(),
            left_user_columns,
            right_user_columns,
        );
        let predicates = spatial_join_predicates(&candidate)
            .into_iter()
            .map(|predicate| remote_expr_for_schema(&predicate, &merged_schema, &self.func_ctx))
            .collect::<Result<Vec<_>>>()?;
        let mut required = required;
        {
            let metadata = self.metadata.read();
            required.extend(metadata.get_retained_column());
        }
        let (output_projection, output_schema) =
            spatial_output_projection_and_schema(&merged_schema, &required)?;

        let left_geometry =
            remote_expr_for_schema(&candidate.left_geometry, &left_schema, &self.func_ctx)?;
        let right_geometry =
            remote_expr_for_schema(&candidate.right_geometry, &right_schema, &self.func_ctx)?;

        let (build, probe, build_geometry, probe_geometry) = match build_side {
            SpatialBuildSide::Left => (left_input, right_input, left_geometry, right_geometry),
            SpatialBuildSide::Right => (right_input, left_input, right_geometry, left_geometry),
        };

        Ok(Some(PhysicalPlan::new(PhysicalSpatialIndexJoin {
            meta: PhysicalPlanMeta::new("SpatialIndexJoin"),
            probe,
            build,
            build_side,
            build_geometry,
            probe_geometry,
            predicates,
            output_projection,
            output_schema,
        })))
    }

    fn spatial_build_bytes_allowed(&self, build_bytes: Option<u64>) -> Result<bool> {
        let Some(bytes) = build_bytes else {
            return Ok(true);
        };

        let settings = self.ctx.get_settings();
        let max_build_bytes = settings.get_spatial_join_max_build_bytes()?;
        let max_indexed_build_bytes = settings.get_spatial_join_max_indexed_build_bytes()?;
        let overhead = settings.get_spatial_join_index_overhead_factor()?;

        Ok(bytes <= max_build_bytes && bytes.saturating_mul(overhead) <= max_indexed_build_bytes)
    }
}

fn remote_expr_for_schema(
    scalar: &ScalarExpr,
    schema: &DataSchemaRef,
    func_ctx: &FunctionContext,
) -> Result<RemoteExpr> {
    let expr = scalar
        .type_check(schema.as_ref())?
        .project_column_ref(|index| schema.index_of(&index.to_string()))?;
    let (expr, _) = ConstantFolder::fold(&expr, func_ctx, &BUILTIN_FUNCTIONS);
    Ok(expr.as_remote_expr())
}

fn merged_user_schema(
    left_schema: DataSchemaRef,
    right_schema: DataSchemaRef,
    left_user_columns: usize,
    right_user_columns: usize,
) -> DataSchemaRef {
    let mut fields = Vec::with_capacity(left_user_columns + right_user_columns);
    fields.extend(left_schema.fields().iter().take(left_user_columns).cloned());
    fields.extend(
        right_schema
            .fields()
            .iter()
            .take(right_user_columns)
            .cloned(),
    );
    DataSchemaRefExt::create(fields)
}

fn spatial_join_predicates(candidate: &SpatialJoinCandidate) -> Vec<ScalarExpr> {
    std::iter::once(candidate.predicate.clone())
        .chain(candidate.residual_predicates.iter().cloned())
        .collect()
}

fn spatial_output_projection_and_schema(
    merged_schema: &DataSchemaRef,
    required: &ColumnSet,
) -> Result<(Vec<usize>, DataSchemaRef)> {
    let mut projection = Vec::new();
    let mut fields = Vec::new();
    for (index, field) in merged_schema.fields().iter().enumerate() {
        let Ok(symbol) = field.name().parse::<usize>() else {
            continue;
        };
        if required.contains(&Symbol::new(symbol)) {
            projection.push(index);
            fields.push(field.clone());
        }
    }
    Ok((projection, DataSchemaRefExt::create(fields)))
}

/// Sums `read_bytes` across all `TableScan`s in `plan`, or `None` if it has none.
fn collect_scan_bytes(plan: &PhysicalPlan) -> Option<u64> {
    let mut bytes = 0;
    let mut found = false;
    collect_scan_bytes_inner(plan, &mut found, &mut bytes);
    found.then_some(bytes)
}

fn collect_scan_bytes_inner(plan: &PhysicalPlan, found: &mut bool, bytes: &mut u64) {
    if let Some(scan) = TableScan::from_physical_plan(plan) {
        *found = true;
        *bytes = bytes.saturating_add(scan.source.statistics.read_bytes as u64);
    }

    for child in plan.children() {
        collect_scan_bytes_inner(child, found, bytes);
    }
}
