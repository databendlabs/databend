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
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_cast;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::DataType;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::Join;
use databend_common_sql::plans::JoinType;
use databend_common_sql::ColumnEntry;
use databend_common_sql::ColumnSet;
use databend_common_sql::IndexType;
use databend_common_sql::ScalarExpr;
use databend_common_sql::TypeCheck;
use tokio::sync::Barrier;

use super::physical_join_filter::PhysicalRuntimeFilters;
use super::JoinRuntimeFilter;
use super::PhysicalPlanCast;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::HashJoinFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::physical_plans::Exchange;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::pipelines::processors::transforms::HashJoinFactory;
use crate::pipelines::processors::transforms::HashJoinProbeState;
use crate::pipelines::processors::transforms::RuntimeFiltersDesc;
use crate::pipelines::processors::transforms::TransformHashJoin;
use crate::pipelines::processors::transforms::TransformHashJoinBuild;
use crate::pipelines::processors::transforms::TransformHashJoinProbe;
use crate::pipelines::processors::HashJoinBuildState;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::HashJoinState;
use crate::pipelines::HashJoinStateRef;
use crate::pipelines::PipelineBuilder;

// Type aliases to simplify complex return types
type JoinConditionsResult = (
    Vec<RemoteExpr>,
    Vec<RemoteExpr>,
    Vec<bool>,
    Vec<Option<(RemoteExpr<String>, usize, usize)>>,
    Vec<((usize, bool), usize)>,
);

type ProjectionsResult = (
    ColumnSet,
    ColumnSet,
    Option<(usize, HashMap<IndexType, usize>)>,
);

type MergedFieldsResult = (
    Vec<DataField>,
    Vec<DataField>,
    Vec<DataField>,
    Vec<(usize, (bool, bool))>,
);

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HashJoin {
    pub meta: PhysicalPlanMeta,
    // After building the probe key and build key, we apply probe_projections to probe_datablock
    // and build_projections to build_datablock, which can help us reduce memory usage and calls
    // of expensive functions (take_compacted_indices and gather), after processing other_conditions,
    // we will use projections for final column elimination.
    pub projections: ColumnSet,
    pub probe_projections: ColumnSet,
    pub build_projections: ColumnSet,

    pub build: PhysicalPlan,
    pub probe: PhysicalPlan,
    pub build_keys: Vec<RemoteExpr>,
    pub probe_keys: Vec<RemoteExpr>,
    pub is_null_equal: Vec<bool>,
    pub non_equi_conditions: Vec<RemoteExpr>,
    pub join_type: JoinType,
    pub marker_index: Option<IndexType>,
    pub from_correlated_subquery: bool,
    // Use the column of probe side to construct build side column.
    // (probe index, (is probe column nullable, is build column nullable))
    pub probe_to_build: Vec<(usize, (bool, bool))>,
    pub output_schema: DataSchemaRef,
    // if we execute distributed merge into, we need to hold the
    // hash table to get not match data from source.
    pub need_hold_hash_table: bool,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,

    // When left/right single join converted to inner join, record the original join type
    // and do some special processing during runtime.
    pub single_to_inner: Option<JoinType>,

    // Hash join build side cache information for ExpressionScan, which includes the cache index and
    // a HashMap for mapping the column indexes to the BlockEntry indexes in DataBlock.
    pub build_side_cache_info: Option<(usize, HashMap<IndexType, usize>)>,

    pub runtime_filter: PhysicalRuntimeFilters,
    pub broadcast_id: Option<u32>,
}

#[typetag::serde]
impl IPhysicalPlan for HashJoin {
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

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.probe).chain(std::iter::once(&self.build)))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.probe).chain(std::iter::once(&mut self.build)))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(HashJoinFormatter::create(self))
    }

    fn get_desc(&self) -> Result<String> {
        let mut conditions = self
            .build_keys
            .iter()
            .zip(self.probe_keys.iter())
            .map(|(l, r)| {
                format!(
                    "({} = {})",
                    l.as_expr(&BUILTIN_FUNCTIONS).sql_display(),
                    r.as_expr(&BUILTIN_FUNCTIONS).sql_display()
                )
            })
            .collect::<Vec<_>>();

        conditions.extend(
            self.non_equi_conditions
                .iter()
                .map(|x| x.as_expr(&BUILTIN_FUNCTIONS).sql_display()),
        );

        Ok(conditions.join(" AND "))
    }

    fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        let mut labels = HashMap::with_capacity(4);
        labels.insert(String::from("Join Type"), vec![self.join_type.to_string()]);

        if !self.build_keys.is_empty() {
            labels.insert(
                String::from("Join Build Side Keys"),
                self.build_keys
                    .iter()
                    .map(|x| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                    .collect(),
            );
        }

        if !self.probe_keys.is_empty() {
            labels.insert(
                String::from("Join Probe Side Keys"),
                self.probe_keys
                    .iter()
                    .map(|x| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                    .collect(),
            );
        }

        if !self.non_equi_conditions.is_empty() {
            labels.insert(
                String::from("Join Conditions"),
                self.non_equi_conditions
                    .iter()
                    .map(|x| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                    .collect(),
            );
        }

        Ok(labels)
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 2);
        let probe_child = children.remove(0);
        let build_child = children.remove(0);

        PhysicalPlan::new(HashJoin {
            build: build_child,
            probe: probe_child,

            meta: self.meta.clone(),
            projections: self.projections.clone(),
            probe_projections: self.probe_projections.clone(),
            build_projections: self.build_projections.clone(),

            build_keys: self.build_keys.clone(),
            probe_keys: self.probe_keys.clone(),
            is_null_equal: self.is_null_equal.clone(),
            non_equi_conditions: self.non_equi_conditions.clone(),
            join_type: self.join_type.clone(),
            marker_index: self.marker_index,
            from_correlated_subquery: self.from_correlated_subquery,
            probe_to_build: self.probe_to_build.clone(),
            output_schema: self.output_schema.clone(),
            need_hold_hash_table: self.need_hold_hash_table,
            stat_info: self.stat_info.clone(),
            single_to_inner: self.single_to_inner.clone(),
            build_side_cache_info: self.build_side_cache_info.clone(),
            runtime_filter: self.runtime_filter.clone(),
            broadcast_id: self.broadcast_id,
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let desc = Arc::new(HashJoinDesc::create(self)?);
        let experimental_new_join = builder.settings.get_enable_experimental_new_join()?;
        let (enable_optimization, _) = builder.merge_into_get_optimization_flag(self);

        if desc.single_to_inner.is_none()
            && (self.join_type == JoinType::Inner || self.join_type == JoinType::Left)
            && experimental_new_join
            && !enable_optimization
        {
            return self.build_new_join_pipeline(builder, desc);
        }

        // Create the join state with optimization flags
        let state = self.build_state(builder)?;

        if let Some((build_cache_index, _)) = self.build_side_cache_info {
            builder.hash_join_states.insert(
                build_cache_index,
                HashJoinStateRef::OldHashJoinState(state.clone()),
            );
        }

        // Build both phases of the Hash Join
        self.build_right(builder, state.clone())?;
        self.build_left(builder, state.clone())?;

        // In the case of spilling, we need to share state among multiple threads
        // Quickly fetch all data from this round to quickly start the next round
        builder
            .main_pipeline
            .resize(builder.main_pipeline.output_len(), true)
    }
}

impl HashJoin {
    fn build_state(&self, builder: &mut PipelineBuilder) -> Result<Arc<HashJoinState>> {
        let (enable_optimization, is_distributed) = builder.merge_into_get_optimization_flag(self);
        HashJoinState::try_create(
            builder.ctx.clone(),
            self.build.output_schema()?,
            &self.build_projections,
            HashJoinDesc::create(self)?,
            &self.probe_to_build,
            is_distributed,
            enable_optimization,
            self.build_side_cache_info.clone(),
        )
    }

    fn build_left(&self, builder: &mut PipelineBuilder, state: Arc<HashJoinState>) -> Result<()> {
        self.probe.build_pipeline(builder)?;

        let max_block_size = builder.settings.get_max_block_size()? as usize;
        let barrier = Barrier::new(builder.main_pipeline.output_len());
        let probe_state = Arc::new(HashJoinProbeState::create(
            builder.ctx.clone(),
            builder.func_ctx.clone(),
            state.clone(),
            &self.probe_projections,
            &self.build_projections,
            &self.probe_keys,
            self.probe.output_schema()?,
            &self.join_type,
            builder.main_pipeline.output_len(),
            barrier,
        )?);

        builder.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(TransformHashJoinProbe::create(
                input,
                output,
                self.projections.clone(),
                probe_state.clone(),
                max_block_size,
                builder.func_ctx.clone(),
                &self.join_type,
                !self.non_equi_conditions.is_empty(),
            )?))
        })?;

        // For merge-into operations that need to hold the hash table
        if self.need_hold_hash_table {
            // Extract projected fields from probe schema
            let mut projected_fields = vec![];
            for (i, field) in probe_state.probe_schema.fields().iter().enumerate() {
                if probe_state.probe_projections.contains(&i) {
                    projected_fields.push(field.clone());
                }
            }
            builder.merge_into_probe_data_fields = Some(projected_fields);
        }

        Ok(())
    }

    fn build_right(&self, builder: &mut PipelineBuilder, state: Arc<HashJoinState>) -> Result<()> {
        let right_builder = builder.create_sub_pipeline_builder();
        let mut build_res = right_builder.finalize(&self.build)?;

        assert!(build_res.main_pipeline.is_pulling_pipeline()?);
        let output_len = build_res.main_pipeline.output_len();
        let build_state = HashJoinBuildState::try_create(
            builder.ctx.clone(),
            builder.func_ctx.clone(),
            &self.build_keys,
            &self.build_projections,
            state.clone(),
            output_len,
            self.broadcast_id,
        )?;
        build_state.add_runtime_filter_ready();

        let create_sink_processor = |input| {
            Ok(ProcessorPtr::create(TransformHashJoinBuild::try_create(
                input,
                build_state.clone(),
            )?))
        };

        // For distributed merge-into when source as build side
        if self.need_hold_hash_table {
            builder.join_state = Some(build_state.clone())
        }

        build_res.main_pipeline.add_sink(create_sink_processor)?;

        builder
            .pipelines
            .push(build_res.main_pipeline.finalize(None));
        builder.pipelines.extend(build_res.sources_pipelines);
        Ok(())
    }

    fn build_new_join_pipeline(
        &self,
        builder: &mut PipelineBuilder,
        desc: Arc<HashJoinDesc>,
    ) -> Result<()> {
        let factory = self.join_factory(builder, desc)?;

        // We must build the runtime filter before constructing the child nodes,
        // as we will inject some runtime filter information into the context for the child nodes to use.
        let rf_desc = RuntimeFiltersDesc::create(&builder.ctx, self)?;

        // After common subexpression elimination is completed, we can delete this type of code.
        {
            let state = factory.create_basic_state(0)?;

            if let Some((build_cache_index, _)) = self.build_side_cache_info {
                builder.hash_join_states.insert(
                    build_cache_index,
                    HashJoinStateRef::NewHashJoinState(state.clone()),
                );
            }
        }

        self.build.build_pipeline(builder)?;
        let mut build_sinks = builder.main_pipeline.take_sinks();

        self.probe.build_pipeline(builder)?;

        // Aligning hash join build and probe parallelism
        let output_len = std::cmp::max(build_sinks.len(), builder.main_pipeline.output_len());
        builder.main_pipeline.resize(output_len, false)?;

        let probe_sinks = builder.main_pipeline.take_sinks();

        if output_len != build_sinks.len() {
            builder.main_pipeline.extend_sinks(build_sinks);
            builder.main_pipeline.resize(output_len, false)?;
            build_sinks = builder.main_pipeline.take_sinks();
        }

        debug_assert_eq!(build_sinks.len(), probe_sinks.len());

        let stage_sync_barrier = Arc::new(Barrier::new(output_len));
        let mut join_sinks = Vec::with_capacity(output_len * 2);
        let mut join_pipe_items = Vec::with_capacity(output_len);
        for (build_sink, probe_sink) in build_sinks.into_iter().zip(probe_sinks.into_iter()) {
            join_sinks.push(build_sink);
            join_sinks.push(probe_sink);

            let build_input = InputPort::create();
            let probe_input = InputPort::create();
            let joined_output = OutputPort::create();

            let hash_join = TransformHashJoin::create(
                build_input.clone(),
                probe_input.clone(),
                joined_output.clone(),
                factory.create_hash_join(0)?,
                stage_sync_barrier.clone(),
                self.projections.clone(),
                rf_desc.clone(),
            );

            join_pipe_items.push(PipeItem::create(
                hash_join,
                vec![build_input, probe_input],
                vec![joined_output],
            ))
        }

        builder.main_pipeline.extend_sinks(join_sinks);
        let join_pipe = Pipe::create(output_len * 2, output_len, join_pipe_items);
        builder.main_pipeline.add_pipe(join_pipe);

        // In the case of spilling, we need to share state among multiple threads
        // Quickly fetch all data from this round to quickly start the next round
        builder
            .main_pipeline
            .resize(builder.main_pipeline.output_len(), true)
    }

    fn join_factory(
        &self,
        ctx: &PipelineBuilder,
        desc: Arc<HashJoinDesc>,
    ) -> Result<Arc<HashJoinFactory>> {
        let hash_key_types = self
            .build_keys
            .iter()
            .zip(&desc.is_null_equal)
            .map(|(expr, is_null_equal)| {
                let expr = expr.as_expr(&BUILTIN_FUNCTIONS);
                if *is_null_equal {
                    expr.data_type().clone()
                } else {
                    expr.data_type().remove_nullable()
                }
            })
            .collect::<Vec<_>>();

        Ok(HashJoinFactory::create(
            self.join_type.clone(),
            ctx.ctx.clone(),
            ctx.func_ctx.clone(),
            DataBlock::choose_hash_method_with_types(&hash_key_types)?,
            desc,
        ))
    }
}

impl PhysicalPlanBuilder {
    /// Builds the physical plans for both sides of the join
    pub async fn build_join_sides(
        &mut self,
        s_expr: &SExpr,
        left_required: ColumnSet,
        right_required: ColumnSet,
    ) -> Result<(PhysicalPlan, PhysicalPlan)> {
        let probe_side = self.build(s_expr.child(0)?, left_required).await?;
        let build_side = self.build(s_expr.child(1)?, right_required).await?;

        Ok((probe_side, build_side))
    }

    /// Prepare column projections with retained columns
    fn prepare_column_projections(
        &self,
        required: &mut ColumnSet,
        others_required: &mut ColumnSet,
    ) -> (Vec<IndexType>, Vec<IndexType>) {
        let retained_columns = self.metadata.read().get_retained_column().clone();
        *required = required.union(&retained_columns).cloned().collect();
        let column_projections = required.clone().into_iter().collect::<Vec<_>>();

        *others_required = others_required.union(&retained_columns).cloned().collect();
        let pre_column_projections = others_required.clone().into_iter().collect::<Vec<_>>();

        (column_projections, pre_column_projections)
    }

    /// Prepares the schema for the build side of the join based on join type
    ///
    /// For LEFT, LEFT_SINGLE, and FULL joins, all columns from the build side
    /// need to be wrapped as nullable types since these join types may produce
    /// unmatched rows that require NULL values for build side columns.
    ///
    /// # Returns
    /// * `Result<DataSchemaRef>` - The prepared schema for the build side
    pub fn prepare_build_schema(
        &self,
        join_type: &JoinType,
        build_side: &PhysicalPlan,
    ) -> Result<DataSchemaRef> {
        match join_type {
            JoinType::Left
            | JoinType::LeftAny
            | JoinType::LeftSingle
            | JoinType::LeftAsof
            | JoinType::Full => {
                let build_schema = build_side.output_schema()?;
                // Wrap nullable type for columns in build side
                let build_schema = DataSchemaRefExt::create(
                    build_schema
                        .fields()
                        .iter()
                        .map(|field| {
                            DataField::new(field.name(), field.data_type().wrap_nullable())
                        })
                        .collect::<Vec<_>>(),
                );
                Ok(build_schema)
            }
            _ => build_side.output_schema(),
        }
    }

    /// Prepares the schema for the probe side of the join based on join type
    ///
    /// For RIGHT, RIGHT_SINGLE, and FULL joins, all columns from the probe side
    /// need to be wrapped as nullable types since these join types may produce
    /// unmatched rows that require NULL values for probe side columns.
    ///
    /// # Arguments
    /// * `join_type` - The type of join operation
    /// * `probe_side` - The physical plan for the probe side
    ///
    /// # Returns
    /// * `Result<DataSchemaRef>` - The prepared schema for the probe side
    pub fn prepare_probe_schema(
        &self,
        join_type: &JoinType,
        probe_side: &PhysicalPlan,
    ) -> Result<DataSchemaRef> {
        match join_type {
            JoinType::Right | JoinType::RightSingle | JoinType::RightAsof | JoinType::Full => {
                let probe_schema = probe_side.output_schema()?;
                // Wrap nullable type for columns in probe side
                let probe_schema = DataSchemaRefExt::create(
                    probe_schema
                        .fields()
                        .iter()
                        .map(|field| {
                            DataField::new(field.name(), field.data_type().wrap_nullable())
                        })
                        .collect::<Vec<_>>(),
                );
                Ok(probe_schema)
            }
            _ => probe_side.output_schema(),
        }
    }

    /// Checks if the build side is a broadcast join and unifies exchange key types
    ///
    /// # Arguments
    /// * `probe_side` - The probe side physical plan
    /// * `build_side` - The build side physical plan
    fn unify_keys(
        &self,
        probe_side: &mut PhysicalPlan,
        build_side: &mut PhysicalPlan,
    ) -> Result<()> {
        // Unify the data types of the left and right exchange keys
        let Some(probe_exchange) = Exchange::from_mut_physical_plan(probe_side) else {
            return Ok(());
        };

        let Some(build_exchange) = Exchange::from_mut_physical_plan(build_side) else {
            return Ok(());
        };

        let cast_rules = &BUILTIN_FUNCTIONS.get_auto_cast_rules("eq");
        for (probe_key, build_key) in probe_exchange
            .keys
            .iter_mut()
            .zip(build_exchange.keys.iter_mut())
        {
            let probe_expr = probe_key.as_expr(&BUILTIN_FUNCTIONS);
            let build_expr = build_key.as_expr(&BUILTIN_FUNCTIONS);
            let common_ty = common_super_type(
                probe_expr.data_type().clone(),
                build_expr.data_type().clone(),
                cast_rules,
            )
            .ok_or_else(|| {
                ErrorCode::IllegalDataType(format!(
                    "Cannot find common type for probe key {:?} and build key {:?}",
                    &probe_expr, &build_expr
                ))
            })?;
            *probe_key = check_cast(
                probe_expr.span(),
                false,
                probe_expr,
                &common_ty,
                &BUILTIN_FUNCTIONS,
            )?
            .as_remote_expr();
            *build_key = check_cast(
                build_expr.span(),
                false,
                build_expr,
                &common_ty,
                &BUILTIN_FUNCTIONS,
            )?
            .as_remote_expr();
        }

        Ok(())
    }

    /// Prepares runtime filter expression for join conditions
    ///
    /// # Arguments
    /// * `left_condition` - The left side condition
    ///
    /// # Returns
    /// * `Result<Option<(databend_common_expression::Expr<String>, usize, usize)>>` - Runtime filter expression, scan ID, and table index
    fn prepare_runtime_filter_expr(
        &self,
        left_condition: &ScalarExpr,
    ) -> Result<Option<(databend_common_expression::Expr<String>, usize, usize)>> {
        // Runtime filter only supports columns in base tables
        if left_condition.used_columns().iter().all(|idx| {
            matches!(
                self.metadata.read().column(*idx),
                ColumnEntry::BaseTableColumn(_)
            )
        }) {
            if let Some(column_idx) = left_condition.used_columns().iter().next() {
                // Safe to unwrap because we have checked the column is a base table column
                let table_index = self
                    .metadata
                    .read()
                    .column(*column_idx)
                    .table_index()
                    .unwrap();
                let scan_id = self
                    .metadata
                    .read()
                    .base_column_scan_id(*column_idx)
                    .unwrap();

                return Ok(Some((
                    left_condition
                        .as_raw_expr()
                        .type_check(&*self.metadata.read())?
                        .project_column_ref(|col| Ok(col.column_name.clone()))?,
                    scan_id,
                    table_index,
                )));
            }
        }

        Ok(None)
    }

    /// Handles inner join column optimization
    ///
    /// # Arguments
    /// * `left_condition` - Left join condition
    /// * `right_condition` - Right join condition
    /// * `probe_schema` - Probe schema
    /// * `build_schema` - Build schema
    /// * `column_projections` - Column projections
    /// * `probe_to_build_index` - Probe to build index mapping
    /// * `pre_column_projections` - Pre-column projections
    ///
    /// # Returns
    /// * `Result<()>` - Success or error
    fn handle_inner_join_column_optimization(
        &self,
        left_condition: &ScalarExpr,
        right_condition: &ScalarExpr,
        probe_schema: &DataSchemaRef,
        build_schema: &DataSchemaRef,
        column_projections: &[IndexType],
        probe_to_build_index: &mut Vec<((usize, bool), usize)>,
        pre_column_projections: &mut Vec<IndexType>,
    ) -> Result<()> {
        if let (ScalarExpr::BoundColumnRef(left), ScalarExpr::BoundColumnRef(right)) =
            (left_condition, right_condition)
        {
            if column_projections.contains(&right.column.index) {
                if let (Ok(probe_index), Ok(build_index)) = (
                    probe_schema.index_of(&left.column.index.to_string()),
                    build_schema.index_of(&right.column.index.to_string()),
                ) {
                    if probe_schema
                        .field(probe_index)
                        .data_type()
                        .remove_nullable()
                        == build_schema
                            .field(build_index)
                            .data_type()
                            .remove_nullable()
                    {
                        probe_to_build_index.push(((probe_index, false), build_index));
                        if !pre_column_projections.contains(&left.column.index) {
                            pre_column_projections.push(left.column.index);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Processes join equi-conditions
    ///
    /// # Arguments
    /// * `join` - Join operation
    /// * `probe_schema` - Probe schema
    /// * `build_schema` - Build schema
    /// * `column_projections` - Column projections
    /// * `pre_column_projections` - Pre-column projections
    ///
    /// # Returns
    /// * Tuple containing processed join conditions and related data
    fn process_equi_conditions(
        &self,
        join: &Join,
        probe_schema: &DataSchemaRef,
        build_schema: &DataSchemaRef,
        column_projections: &[IndexType],
        pre_column_projections: &mut Vec<IndexType>,
    ) -> Result<JoinConditionsResult> {
        let mut left_join_conditions = Vec::new();
        let mut right_join_conditions = Vec::new();
        let mut is_null_equal = Vec::new();
        let mut left_join_conditions_rt = Vec::new();
        let mut probe_to_build_index = Vec::new();

        let cast_rules = &BUILTIN_FUNCTIONS.get_auto_cast_rules("eq");
        for condition in join.equi_conditions.iter() {
            let left_condition = &condition.left;
            let right_condition = &condition.right;

            // Type check expressions
            let right_expr = right_condition
                .type_check(build_schema.as_ref())?
                .project_column_ref(|index| build_schema.index_of(&index.to_string()))?;
            let left_expr = left_condition
                .type_check(probe_schema.as_ref())?
                .project_column_ref(|index| probe_schema.index_of(&index.to_string()))?;

            // Prepare runtime filter expression
            let left_expr_for_runtime_filter = self.prepare_runtime_filter_expr(left_condition)?;

            // Handle inner join column optimization
            if matches!(join.join_type, JoinType::Inner | JoinType::InnerAny) {
                self.handle_inner_join_column_optimization(
                    left_condition,
                    right_condition,
                    probe_schema,
                    build_schema,
                    column_projections,
                    &mut probe_to_build_index,
                    pre_column_projections,
                )?;
            }

            // Unify the data types of the left and right expressions
            let left_type = left_expr.data_type();
            let right_type = right_expr.data_type();
            let common_ty = common_super_type(left_type.clone(), right_type.clone(), cast_rules)
                .ok_or_else(|| {
                    ErrorCode::IllegalDataType(format!(
                        "Cannot find common type for {:?} and {:?}",
                        left_type, right_type
                    ))
                })?;

            let left_expr = check_cast(
                left_expr.span(),
                false,
                left_expr,
                &common_ty,
                &BUILTIN_FUNCTIONS,
            )?;

            let right_expr = check_cast(
                right_expr.span(),
                false,
                right_expr,
                &common_ty,
                &BUILTIN_FUNCTIONS,
            )?;

            // Process runtime filter expressions
            let left_expr_for_runtime_filter = left_expr_for_runtime_filter
                .map(|(expr, scan_id, table_index)| {
                    check_cast(expr.span(), false, expr, &common_ty, &BUILTIN_FUNCTIONS)
                        .map(|casted_expr| (casted_expr, scan_id, table_index))
                })
                .transpose()?;

            // Fold constants
            let (left_expr, _) =
                ConstantFolder::fold(&left_expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
            let (right_expr, _) =
                ConstantFolder::fold(&right_expr, &self.func_ctx, &BUILTIN_FUNCTIONS);

            let left_expr_for_runtime_filter =
                left_expr_for_runtime_filter.map(|(expr, scan_id, table_index)| {
                    (
                        ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS).0,
                        scan_id,
                        table_index,
                    )
                });

            // Add to result collections
            left_join_conditions.push(left_expr.as_remote_expr());
            right_join_conditions.push(right_expr.as_remote_expr());
            is_null_equal.push(condition.is_null_equal);
            left_join_conditions_rt.push(
                left_expr_for_runtime_filter.map(|(expr, scan_id, table_index)| {
                    (expr.as_remote_expr(), scan_id, table_index)
                }),
            );
        }

        Ok((
            left_join_conditions,
            right_join_conditions,
            is_null_equal,
            left_join_conditions_rt,
            probe_to_build_index,
        ))
    }

    /// Prepares cache columns and projections
    ///
    /// # Arguments
    /// * `join` - Join operation
    /// * `probe_schema` - Probe schema
    /// * `build_schema` - Build schema
    /// * `pre_column_projections` - Pre-column projections
    /// * `probe_to_build_index` - Probe to build index mapping
    ///
    /// # Returns
    /// * Tuple containing projections and cache info
    fn prepare_projections_and_cache(
        &self,
        join: &Join,
        probe_schema: &DataSchemaRef,
        build_schema: &DataSchemaRef,
        pre_column_projections: &[IndexType],
        probe_to_build_index: &mut Vec<((usize, bool), usize)>,
    ) -> Result<ProjectionsResult> {
        // Handle cache columns
        let mut cache_column_map = HashMap::new();
        let cached_column = if let Some(cache_info) = &join.build_side_cache_info {
            cache_info.columns.clone().into_iter().collect()
        } else {
            HashSet::new()
        };

        // Prepare projections
        let mut probe_projections = ColumnSet::new();
        let mut build_projections = ColumnSet::new();

        for column in pre_column_projections.iter() {
            if let Some((index, _)) = probe_schema.column_with_name(&column.to_string()) {
                probe_projections.insert(index);
            }
            if let Some((index, _)) = build_schema.column_with_name(&column.to_string()) {
                if cached_column.contains(column) {
                    cache_column_map.insert(*column, index);
                }
                build_projections.insert(index);
            }
        }

        // Prepare cache info
        let build_side_cache_info = if let Some(cache_info) = &join.build_side_cache_info {
            probe_to_build_index.clear();
            Some((cache_info.cache_idx, cache_column_map))
        } else {
            None
        };

        Ok((probe_projections, build_projections, build_side_cache_info))
    }

    /// Creates merged fields and handles field mapping
    ///
    /// # Arguments
    /// * `probe_schema` - Probe schema
    /// * `build_schema` - Build schema
    /// * `probe_projections` - Probe projections
    /// * `build_projections` - Build projections
    /// * `probe_to_build_index` - Probe to build index mapping
    ///
    /// # Returns
    /// * Tuple containing merged fields, probe fields, build fields, and probe to build mapping
    fn create_merged_fields(
        &self,
        probe_schema: &DataSchemaRef,
        build_schema: &DataSchemaRef,
        probe_projections: &ColumnSet,
        build_projections: &mut ColumnSet,
        probe_to_build_index: &mut [((usize, bool), usize)],
    ) -> Result<MergedFieldsResult> {
        let mut merged_fields =
            Vec::with_capacity(probe_projections.len() + build_projections.len());
        let mut probe_fields = Vec::with_capacity(probe_projections.len());
        let mut build_fields = Vec::with_capacity(build_projections.len());
        let mut probe_to_build = Vec::new();
        let mut tail_fields = Vec::new();

        // Process probe fields
        for (i, field) in probe_schema.fields().iter().enumerate() {
            if probe_projections.contains(&i) {
                for ((probe_index, updated), _) in probe_to_build_index.iter_mut() {
                    if probe_index == &i && !*updated {
                        *probe_index = probe_fields.len();
                        *updated = true;
                    }
                }
                probe_fields.push(field.clone());
                merged_fields.push(field.clone());
            }
        }

        // Process build fields
        for (i, field) in build_schema.fields().iter().enumerate() {
            if build_projections.contains(&i) {
                let mut is_tail = false;
                for ((probe_index, _), build_index) in probe_to_build_index.iter() {
                    if build_index == &i {
                        tail_fields.push(field.clone());
                        probe_to_build.push((
                            *probe_index,
                            (
                                probe_fields[*probe_index].data_type().is_nullable(),
                                field.data_type().is_nullable(),
                            ),
                        ));
                        build_projections.remove(&i);
                        is_tail = true;
                    }
                }
                if !is_tail {
                    build_fields.push(field.clone());
                    merged_fields.push(field.clone());
                }
            }
        }

        // Add tail fields
        build_fields.extend(tail_fields.clone());
        merged_fields.extend(tail_fields);

        Ok((merged_fields, probe_fields, build_fields, probe_to_build))
    }

    /// Creates output schema based on join type
    ///
    /// # Arguments
    /// * `join` - Join operation
    /// * `probe_fields` - Probe fields
    /// * `build_fields` - Build fields
    /// * `column_projections` - Column projections
    ///
    /// # Returns
    /// * Tuple containing merged fields, output schema, and projections
    fn create_output_schema(
        &self,
        join: &Join,
        probe_fields: Vec<DataField>,
        build_fields: Vec<DataField>,
        column_projections: &[IndexType],
    ) -> Result<(Vec<DataField>, DataSchemaRef, ColumnSet)> {
        // Create merged fields based on join type
        let merged_fields = match join.join_type {
            JoinType::Cross
            | JoinType::Inner
            | JoinType::InnerAny
            | JoinType::Left
            | JoinType::LeftAny
            | JoinType::LeftSingle
            | JoinType::Right
            | JoinType::RightAny
            | JoinType::RightSingle
            | JoinType::Full => {
                let mut result = probe_fields.clone();
                result.extend(build_fields);
                result
            }
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightSemi | JoinType::RightAnti => {
                let (result_fields, dropped_fields) = if join.join_type == JoinType::LeftSemi
                    || join.join_type == JoinType::LeftAnti
                {
                    (probe_fields, build_fields)
                } else {
                    (build_fields, probe_fields)
                };

                // Check for invalid column access in ANTI or SEMI joins
                for field in dropped_fields.iter() {
                    if result_fields.iter().all(|x| x.name() != field.name())
                        && let Ok(index) = field.name().parse::<usize>()
                        && column_projections.contains(&index)
                    {
                        let metadata = self.metadata.read();
                        let unexpected_column = metadata.column(index);
                        let unexpected_column_info =
                            if let Some(table_index) = unexpected_column.table_index() {
                                format!(
                                    "{:?}.{:?}",
                                    metadata.table(table_index).name(),
                                    unexpected_column.name()
                                )
                            } else {
                                unexpected_column.name().to_string()
                            };
                        return Err(ErrorCode::SemanticError(format!(
                            "cannot access the {} in ANTI or SEMI join",
                            unexpected_column_info
                        )));
                    }
                }
                result_fields
            }
            JoinType::LeftMark => {
                let name = if let Some(idx) = join.marker_index {
                    idx.to_string()
                } else {
                    "marker".to_string()
                };
                let mut result = build_fields;
                result.push(DataField::new(
                    name.as_str(),
                    DataType::Nullable(Box::new(DataType::Boolean)),
                ));
                result
            }
            JoinType::RightMark => {
                let name = if let Some(idx) = join.marker_index {
                    idx.to_string()
                } else {
                    "marker".to_string()
                };
                let mut result = probe_fields;
                result.push(DataField::new(
                    name.as_str(),
                    DataType::Nullable(Box::new(DataType::Boolean)),
                ));
                result
            }
            JoinType::Asof | JoinType::LeftAsof | JoinType::RightAsof => unreachable!(
                "Invalid join type {} during building physical hash join.",
                join.join_type
            ),
        };

        // Create projections and output schema
        let mut projections = ColumnSet::new();
        let projected_schema = DataSchemaRefExt::create(merged_fields.clone());

        for column in column_projections.iter() {
            if let Some((index, _)) = projected_schema.column_with_name(&column.to_string()) {
                projections.insert(index);
            }
        }

        let mut output_fields = Vec::with_capacity(column_projections.len());
        for (i, field) in merged_fields.iter().enumerate() {
            if projections.contains(&i) {
                output_fields.push(field.clone());
            }
        }

        let output_schema = DataSchemaRefExt::create(output_fields);

        Ok((merged_fields, output_schema, projections))
    }

    /// Processes non-equi conditions
    ///
    /// # Arguments
    /// * `join` - Join operation
    /// * `merged_schema` - Merged schema
    ///
    /// # Returns
    /// * `Result<Vec<RemoteExpr>>` - Processed non-equi conditions
    fn process_non_equi_conditions(
        &self,
        join: &Join,
        merged_schema: &DataSchemaRef,
    ) -> Result<Vec<RemoteExpr>> {
        join.non_equi_conditions
            .iter()
            .map(|scalar| {
                let expr = scalar
                    .type_check(merged_schema.as_ref())?
                    .project_column_ref(|index| merged_schema.index_of(&index.to_string()))?;
                let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                Ok(expr.as_remote_expr())
            })
            .collect::<Result<_>>()
    }

    /// Creates a HashJoin physical plan
    ///
    /// # Arguments
    /// * `join` - Join operation
    /// * `probe_side` - Probe side physical plan
    /// * `build_side` - Build side physical plan
    /// * `is_broadcast` - Whether this is a broadcast join
    /// * `projections` - Column projections
    /// * `probe_projections` - Probe side projections
    /// * `build_projections` - Build side projections
    /// * `left_join_conditions` - Left join conditions
    /// * `right_join_conditions` - Right join conditions
    /// * `is_null_equal` - Null equality flags
    /// * `non_equi_conditions` - Non-equi conditions
    /// * `probe_to_build` - Probe to build mapping
    /// * `output_schema` - Output schema
    /// * `build_side_cache_info` - Build side cache info
    /// * `runtime_filter` - Runtime filter
    /// * `stat_info` - Statistics info
    ///
    /// # Returns
    /// * `Result<PhysicalPlan>` - The HashJoin physical plan
    #[allow(clippy::too_many_arguments)]
    fn create_hash_join(
        &self,
        s_expr: &SExpr,
        join: &Join,
        probe_side: PhysicalPlan,
        build_side: PhysicalPlan,
        projections: ColumnSet,
        probe_projections: ColumnSet,
        build_projections: ColumnSet,
        left_join_conditions: Vec<RemoteExpr>,
        right_join_conditions: Vec<RemoteExpr>,
        is_null_equal: Vec<bool>,
        non_equi_conditions: Vec<RemoteExpr>,
        probe_to_build: Vec<(usize, (bool, bool))>,
        output_schema: DataSchemaRef,
        build_side_cache_info: Option<(usize, HashMap<IndexType, usize>)>,
        runtime_filter: PhysicalRuntimeFilters,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        let build_side_data_distribution = s_expr.build_side_child().get_data_distribution()?;
        let broadcast_id = if build_side_data_distribution
            .as_ref()
            .is_some_and(|e| matches!(e, databend_common_sql::plans::Exchange::Hash(_)))
        {
            Some(self.ctx.get_next_broadcast_id())
        } else {
            None
        };
        Ok(PhysicalPlan::new(HashJoin {
            projections,
            build_projections,
            probe_projections,
            build: build_side,
            probe: probe_side,
            join_type: join.join_type.clone(),
            build_keys: right_join_conditions,
            probe_keys: left_join_conditions,
            is_null_equal,
            non_equi_conditions,
            marker_index: join.marker_index,
            meta: PhysicalPlanMeta::new("HashJoin"),
            from_correlated_subquery: join.from_correlated_subquery,
            probe_to_build,
            output_schema,
            need_hold_hash_table: join.need_hold_hash_table,
            stat_info: Some(stat_info),
            single_to_inner: join.single_to_inner.clone(),
            build_side_cache_info,
            runtime_filter,
            broadcast_id,
        }))
    }

    pub async fn build_hash_join(
        &mut self,
        join: &Join,
        s_expr: &SExpr,
        mut required: ColumnSet,
        mut others_required: ColumnSet,
        left_required: ColumnSet,
        right_required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // Step 1: Build probe and build sides
        let (mut probe_side, mut build_side) = self
            .build_join_sides(s_expr, left_required, right_required)
            .await?;

        // Step 2: Prepare column projections
        let (column_projections, mut pre_column_projections) =
            self.prepare_column_projections(&mut required, &mut others_required);

        // Step 3: unify exchange keys
        self.unify_keys(&mut probe_side, &mut build_side)?;

        // Step 4: Prepare schemas for both sides
        let build_schema = self.prepare_build_schema(&join.join_type, &build_side)?;
        let probe_schema = self.prepare_probe_schema(&join.join_type, &probe_side)?;

        // Step 5: Process join conditions
        let (
            left_join_conditions,
            right_join_conditions,
            is_null_equal,
            left_join_conditions_rt,
            mut probe_to_build_index,
        ) = self.process_equi_conditions(
            join,
            &probe_schema,
            &build_schema,
            &column_projections,
            &mut pre_column_projections,
        )?;

        // Step 6: Prepare projections and cache info
        let (probe_projections, mut build_projections, build_side_cache_info) = self
            .prepare_projections_and_cache(
                join,
                &probe_schema,
                &build_schema,
                &pre_column_projections,
                &mut probe_to_build_index,
            )?;

        // Step 7: Create merged fields
        let (merged_fields, probe_fields, build_fields, probe_to_build) = self
            .create_merged_fields(
                &probe_schema,
                &build_schema,
                &probe_projections,
                &mut build_projections,
                &mut probe_to_build_index,
            )?;

        // Step 8: Create merged schema for non-equi conditions
        let merged_schema = DataSchemaRefExt::create(merged_fields);

        // Step 9: Create output schema
        let (_merged_fields_unused, output_schema, projections) =
            self.create_output_schema(join, probe_fields, build_fields, &column_projections)?;

        // Step 10: Process non-equi conditions
        let non_equi_conditions = self.process_non_equi_conditions(join, &merged_schema)?;

        // Step 11: Build runtime filter
        let runtime_filter = self
            .build_runtime_filter(
                join,
                s_expr,
                &right_join_conditions,
                left_join_conditions_rt,
            )
            .await?;

        // Step 12: Create and return the HashJoin
        self.create_hash_join(
            s_expr,
            join,
            probe_side,
            build_side,
            projections,
            probe_projections,
            build_projections,
            left_join_conditions,
            right_join_conditions,
            is_null_equal,
            non_equi_conditions,
            probe_to_build,
            output_schema,
            build_side_cache_info,
            runtime_filter,
            stat_info,
        )
    }

    async fn build_runtime_filter(
        &self,
        join: &Join,
        s_expr: &SExpr,
        build_keys: &[RemoteExpr],
        probe_keys: Vec<Option<(RemoteExpr<String>, usize, usize)>>,
    ) -> Result<PhysicalRuntimeFilters> {
        JoinRuntimeFilter::build_runtime_filter(
            self.ctx.clone(),
            &self.metadata,
            join,
            s_expr,
            build_keys,
            probe_keys,
        )
        .await
    }
}
