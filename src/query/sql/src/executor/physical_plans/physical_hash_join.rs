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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_cast;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::DataType;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::Expr;
use databend_common_expression::RemoteExpr;
use databend_common_expression::ROW_NUMBER_COL_NAME;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::physical_plans::Exchange;
use crate::executor::physical_plans::FragmentKind;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::ColumnSet;
use crate::optimizer::SExpr;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::ColumnEntry;
use crate::IndexType;
use crate::ScalarExpr;
use crate::TypeCheck;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HashJoin {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    // After building the probe key and build key, we apply probe_projections to probe_datablock
    // and build_projections to build_datablock, which can help us reduce memory usage and calls
    // of expensive functions (take_compacted_indices and gather), after processing other_conditions,
    // we will use projections for final column elimination.
    pub projections: ColumnSet,
    pub probe_projections: ColumnSet,
    pub build_projections: ColumnSet,

    pub hash_join_id: usize,
    pub build: Box<PhysicalPlan>,
    pub probe: Box<PhysicalPlan>,
    pub runtime_filter: Option<Box<PhysicalPlan>>,
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

    // probe keys for runtime filter, and record the index of table that used in probe keys.
    pub runtime_filter_exprs: Vec<Option<(RemoteExpr<String>, IndexType)>>,
    pub runtime_filter_source_fields: Vec<DataField>,
    // If support runtime filter.
    pub support_runtime_filter: bool,
    // Under cluster, mark if the join is broadcast join.
    pub broadcast: bool,
    // When left/right single join converted to inner join, record the original join type
    // and do some special processing during runtime.
    pub single_to_inner: Option<JoinType>,

    // Hash join build side cache information for ExpressionScan, which includes the cache index and
    // a HashMap for mapping the column indexes to the BlockEntry indexes in DataBlock.
    pub build_side_cache_info: Option<(usize, HashMap<IndexType, usize>)>,
}

impl HashJoin {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.output_schema.clone())
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_hash_join(
        &mut self,
        join: &Join,
        s_expr: &SExpr,
        mut required: ColumnSet,
        mut others_required: ColumnSet,
        left_required: ColumnSet,
        right_required: ColumnSet,
        plan_stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        let hash_join_id = self.next_hash_join_id();
        let runtime_filter_exprs = self.runtime_filter_exprs(join)?;
        self.runtime_filter_columns
            .insert(hash_join_id, runtime_filter_columns(&runtime_filter_exprs));

        let mut probe_side = Box::new(self.build(s_expr.child(0)?, left_required).await?);
        let mut build_side = Box::new(self.build(s_expr.child(1)?, right_required).await?);

        let retained_columns = self.metadata.read().get_retained_column().clone();
        required = required.union(&retained_columns).cloned().collect();
        let column_projections = required.clone().into_iter().collect::<Vec<_>>();

        others_required = others_required.union(&retained_columns).cloned().collect();
        let mut pre_column_projections = others_required.clone().into_iter().collect::<Vec<_>>();

        // Check whether it is a distributed hash join.
        let mut is_broadcast_join = false;
        if let PhysicalPlan::Exchange(Exchange {
            kind: FragmentKind::Expansive,
            ..
        }) = build_side.as_ref()
        {
            // Broadcast join.
            is_broadcast_join = true;
        }
        let mut is_shuffle_join = false;
        let conmon_data_types = if let (
            PhysicalPlan::Exchange(Exchange {
                keys: probe_keys, ..
            }),
            PhysicalPlan::Exchange(Exchange {
                keys: build_keys, ..
            }),
        ) = (probe_side.as_mut(), build_side.as_mut())
        {
            // Shuffle join, unify the data types of the left and right exchange keys.
            is_shuffle_join = true;
            let conmon_data_types = self.unify_keys_data_type(probe_keys, build_keys)?;
            Some(conmon_data_types)
        } else {
            None
        };

        // The output schema of build and probe side.
        let (build_schema, probe_schema) =
            self.build_and_probe_output_schema(join, &build_side, &probe_side)?;

        let mut probe_keys = Vec::new();
        let mut build_keys = Vec::new();
        let mut is_null_equal = Vec::new();
        let mut probe_to_build_index = Vec::new();
        let mut runtime_filter_source_fields = Vec::new();
        for (index, condition) in join.equi_conditions.iter().enumerate() {
            let build_condition = &condition.right;
            let probe_condition = &condition.left;
            let build_expr = build_condition
                .type_check(build_schema.as_ref())?
                .project_column_ref(|index| build_schema.index_of(&index.to_string()).unwrap());
            let probe_expr = probe_condition
                .type_check(probe_schema.as_ref())?
                .project_column_ref(|index| probe_schema.index_of(&index.to_string()).unwrap());

            if let Some((_, build_index)) =
                self.support_runtime_filter(probe_condition, build_condition)?
            {
                let build_index = build_schema.index_of(&build_index.to_string())?;
                let build_field = build_schema.field(build_index);
                if let Some(conmon_data_types) = &conmon_data_types {
                    let data_type = conmon_data_types[index].clone();
                    runtime_filter_source_fields
                        .push(DataField::new(build_field.name(), data_type));
                } else {
                    runtime_filter_source_fields.push(build_field.clone());
                }
            }

            if join.join_type == JoinType::Inner {
                if let (ScalarExpr::BoundColumnRef(left), ScalarExpr::BoundColumnRef(right)) =
                    (probe_condition, build_condition)
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
            }

            // Unify the data types of the probe and right expressions.
            let probe_type = probe_expr.data_type();
            let build_type = build_expr.data_type();
            let common_ty = common_super_type(
                probe_type.clone(),
                build_type.clone(),
                &BUILTIN_FUNCTIONS.default_cast_rules,
            )
            .ok_or_else(|| {
                ErrorCode::IllegalDataType(format!(
                    "Cannot find common type for {:?} and {:?}",
                    probe_type, build_type
                ))
            })?;
            let probe_expr = check_cast(
                probe_expr.span(),
                false,
                probe_expr,
                &common_ty,
                &BUILTIN_FUNCTIONS,
            )?;
            let build_expr = check_cast(
                build_expr.span(),
                false,
                build_expr,
                &common_ty,
                &BUILTIN_FUNCTIONS,
            )?;

            let (probe_expr, _) =
                ConstantFolder::fold(&probe_expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
            let (build_expr, _) =
                ConstantFolder::fold(&build_expr, &self.func_ctx, &BUILTIN_FUNCTIONS);

            probe_keys.push(probe_expr.as_remote_expr());
            build_keys.push(build_expr.as_remote_expr());
            is_null_equal.push(condition.is_null_equal);
        }

        // For shuffle join, we need to build global runtime filter.
        let support_runtime_filter = supported_runtime_filter_join_type(&join.join_type);
        let runtime_filter_plan = if support_runtime_filter
            && !self.ctx.get_cluster().is_empty()
            && is_shuffle_join
            && !runtime_filter_source_fields.is_empty()
        {
            runtime_filter_source_fields.push(DataField::new("node_id", DataType::String));
            runtime_filter_source_fields.push(DataField::new("need_to_build", DataType::Boolean));
            Some(self.build_runtime_filter_plan(
                hash_join_id,
                DataSchemaRefExt::create(runtime_filter_source_fields.clone()),
            )?)
        } else {
            None
        };

        // Cache scan info.
        let mut cache_column_map = HashMap::new();
        let cached_column = if let Some(cache_info) = &join.build_side_cache_info {
            cache_info.columns.clone().into_iter().collect()
        } else {
            HashSet::new()
        };

        pre_column_projections.extend(cached_column.iter());
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

        let build_side_cache_info = if let Some(cache_info) = &join.build_side_cache_info {
            probe_to_build_index.clear();
            Some((cache_info.cache_idx, cache_column_map))
        } else {
            None
        };

        let mut merged_fields =
            Vec::with_capacity(probe_projections.len() + build_projections.len());
        let mut probe_fields = Vec::with_capacity(probe_projections.len());
        let mut build_fields = Vec::with_capacity(build_projections.len());
        let mut probe_to_build = Vec::new();
        let mut tail_fields = Vec::new();
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
        build_fields.extend(tail_fields.clone());
        merged_fields.extend(tail_fields);
        let merged_schema = DataSchemaRefExt::create(merged_fields);

        let merged_fields = match join.join_type {
            JoinType::Cross
            | JoinType::Inner
            | JoinType::Left
            | JoinType::LeftSingle
            | JoinType::Right
            | JoinType::RightSingle
            | JoinType::Full => {
                probe_fields.extend(build_fields);
                probe_fields
            }
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightSemi | JoinType::RightAnti => {
                let (result_fields, dropped_fields) = if join.join_type == JoinType::LeftSemi
                    || join.join_type == JoinType::LeftAnti
                {
                    (probe_fields, build_fields)
                } else {
                    (build_fields, probe_fields)
                };
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
                build_fields.push(DataField::new(
                    name.as_str(),
                    DataType::Nullable(Box::new(DataType::Boolean)),
                ));
                build_fields
            }
            JoinType::RightMark => {
                let name = if let Some(idx) = join.marker_index {
                    idx.to_string()
                } else {
                    "marker".to_string()
                };
                probe_fields.push(DataField::new(
                    name.as_str(),
                    DataType::Nullable(Box::new(DataType::Boolean)),
                ));
                probe_fields
            }
        };
        let mut projections = ColumnSet::new();
        let projected_schema = DataSchemaRefExt::create(merged_fields.clone());
        for column in column_projections.iter() {
            if let Some((index, _)) = projected_schema.column_with_name(&column.to_string()) {
                projections.insert(index);
            }
        }

        // for distributed merge into, there is a field called "_row_number", but
        // it's not an internal row_number, we need to add it here
        if let Some((index, _)) = projected_schema.column_with_name(ROW_NUMBER_COL_NAME) {
            projections.insert(index);
        }

        let mut output_fields = Vec::with_capacity(column_projections.len());
        for (i, field) in merged_fields.iter().enumerate() {
            if projections.contains(&i) {
                output_fields.push(field.clone());
            }
        }
        let output_schema = DataSchemaRefExt::create(output_fields);
        Ok(PhysicalPlan::HashJoin(HashJoin {
            plan_id: 0,
            projections,
            build_projections,
            probe_projections,
            hash_join_id,
            build: build_side,
            probe: probe_side,
            runtime_filter: runtime_filter_plan,
            join_type: join.join_type.clone(),
            build_keys,
            probe_keys,
            is_null_equal,
            runtime_filter_exprs,
            runtime_filter_source_fields,
            non_equi_conditions: join
                .non_equi_conditions
                .iter()
                .map(|scalar| {
                    let expr = scalar
                        .type_check(merged_schema.as_ref())?
                        .project_column_ref(|index| {
                            merged_schema.index_of(&index.to_string()).unwrap()
                        });
                    let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                    Ok(expr.as_remote_expr())
                })
                .collect::<Result<_>>()?,
            marker_index: join.marker_index,
            from_correlated_subquery: join.from_correlated_subquery,
            probe_to_build,
            output_schema,
            need_hold_hash_table: join.need_hold_hash_table,
            stat_info: Some(plan_stat_info),
            broadcast: is_broadcast_join,
            single_to_inner: join.single_to_inner.clone(),
            support_runtime_filter,
            build_side_cache_info,
        }))
    }

    fn unify_keys_data_type(
        &self,
        probe_keys: &mut [RemoteExpr],
        build_keys: &mut [RemoteExpr],
    ) -> Result<Vec<DataType>> {
        let mut common_data_types = Vec::with_capacity(probe_keys.len());
        for (probe_key, build_key) in probe_keys.iter_mut().zip(build_keys.iter_mut()) {
            let probe_expr = probe_key.as_expr(&BUILTIN_FUNCTIONS);
            let build_expr = build_key.as_expr(&BUILTIN_FUNCTIONS);
            let common_ty = common_super_type(
                probe_expr.data_type().clone(),
                build_expr.data_type().clone(),
                &BUILTIN_FUNCTIONS.default_cast_rules,
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
            common_data_types.push(common_ty);
        }

        Ok(common_data_types)
    }

    fn build_and_probe_output_schema(
        &self,
        join: &Join,
        build: &PhysicalPlan,
        probe: &PhysicalPlan,
    ) -> Result<(DataSchemaRef, DataSchemaRef)> {
        let build_schema = match join.join_type {
            JoinType::Left | JoinType::LeftSingle | JoinType::Full => {
                // Wrap nullable type for columns in build side.
                DataSchemaRefExt::create(
                    build
                        .output_schema()?
                        .fields()
                        .iter()
                        .map(|field| {
                            DataField::new(field.name(), field.data_type().wrap_nullable())
                        })
                        .collect::<Vec<_>>(),
                )
            }
            _ => build.output_schema()?,
        };

        let probe_schema = match join.join_type {
            JoinType::Right | JoinType::RightSingle | JoinType::Full => {
                // Wrap nullable type for columns in probe side.
                DataSchemaRefExt::create(
                    probe
                        .output_schema()?
                        .fields()
                        .iter()
                        .map(|field| {
                            DataField::new(field.name(), field.data_type().wrap_nullable())
                        })
                        .collect::<Vec<_>>(),
                )
            }
            _ => probe.output_schema()?,
        };

        Ok((build_schema, probe_schema))
    }

    fn build_runtime_filter_plan(
        &mut self,
        hash_join_id: usize,
        runtime_filter_source_output_schema: DataSchemaRef,
    ) -> Result<Box<PhysicalPlan>> {
        let runtime_filter_source =
            self.build_runtime_filter_source(hash_join_id, runtime_filter_source_output_schema)?;
        Ok(Box::new(self.build_runtime_filter_sink(
            hash_join_id,
            Box::new(PhysicalPlan::Exchange(Exchange {
                plan_id: 0,
                input: Box::new(runtime_filter_source),
                kind: FragmentKind::Expansive,
                keys: vec![],
                allow_adjust_parallelism: true,
                ignore_exchange: false,
            })),
        )?))
    }

    pub fn support_runtime_filter(
        &self,
        probe_condition: &ScalarExpr,
        build_condition: &ScalarExpr,
    ) -> Result<Option<(IndexType, IndexType)>> {
        // Runtime filter only support column in base table. It's possible to use a wrong derived column with
        // the same name as a base table column, so we need to check if the column is a base table column.
        if let (ScalarExpr::BoundColumnRef(probe_column), ScalarExpr::BoundColumnRef(build_column)) =
            (probe_condition, build_condition)
            && matches!(
                self.metadata.read().column(probe_column.column.index),
                ColumnEntry::BaseTableColumn(_)
            )
            && matches!(
                self.metadata.read().column(build_column.column.index),
                ColumnEntry::BaseTableColumn(_)
            )
            && supported_runtime_filter_data_type(&probe_condition.data_type()?)
        {
            Ok(Some((probe_column.column.index, build_column.column.index)))
        } else {
            Ok(None)
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn runtime_filter_exprs(
        &self,
        join: &Join,
    ) -> Result<Vec<Option<(RemoteExpr<String>, IndexType)>>> {
        let mut runtime_filter_exprs = Vec::with_capacity(join.equi_conditions.len());
        for condition in join.equi_conditions.iter() {
            let build_condition = &condition.right;
            let probe_condition = &condition.left;
            // Runtime filter only support column in base table. It's possible to use a wrong derived column with
            // the same name as a base table column, so we need to check if the column is a base table column.
            let runtime_filter_expr = if let Some((probe_index, _)) =
                self.support_runtime_filter(probe_condition, build_condition)?
            {
                // Safe to unwrap because we have checked the column is a base table column.
                let table_index = self
                    .metadata
                    .read()
                    .column(probe_index)
                    .table_index()
                    .unwrap();
                Some((
                    probe_condition
                        .as_raw_expr()
                        .type_check(&*self.metadata.read())?
                        .project_column_ref(|col| col.column_name.clone()),
                    table_index,
                ))
            } else {
                None
            };

            let probe_data_type = probe_condition.data_type()?;
            let build_data_type = build_condition.data_type()?;
            let common_data_type = common_super_type(
                probe_data_type.clone(),
                build_data_type.clone(),
                &BUILTIN_FUNCTIONS.default_cast_rules,
            )
            .ok_or_else(|| {
                ErrorCode::IllegalDataType(format!(
                    "Cannot find common type for {:?} and {:?}",
                    probe_data_type, build_data_type
                ))
            })?;
            let runtime_filter_expr = runtime_filter_expr
                .map(|(expr, idx)| {
                    check_cast(
                        expr.span(),
                        false,
                        expr,
                        &common_data_type,
                        &BUILTIN_FUNCTIONS,
                    )
                    .map(|casted_expr| (casted_expr, idx))
                })
                .transpose()?;

            let runtime_filter_expr = runtime_filter_expr.map(|(expr, table_index)| {
                (
                    ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS).0,
                    table_index,
                )
            });

            runtime_filter_exprs
                .push(runtime_filter_expr.map(|(expr, index)| (expr.as_remote_expr(), index)));
        }
        Ok(runtime_filter_exprs)
    }
}

pub fn runtime_filter_columns(
    runtime_filter_exprs: &[Option<(RemoteExpr<String>, IndexType)>],
) -> Vec<(usize, String)> {
    let runtime_filter_exprs: Vec<Option<(Expr<String>, IndexType)>> = runtime_filter_exprs
        .iter()
        .map(|runtime_filter_expr| {
            runtime_filter_expr
                .as_ref()
                .map(|(expr, table_index)| (expr.as_expr(&BUILTIN_FUNCTIONS), *table_index))
        })
        .collect();
    let mut columns = Vec::new();
    for (probe_key, table_index) in runtime_filter_exprs
        .iter()
        .filter_map(|runtime_filter_expr| {
            runtime_filter_expr
                .as_ref()
                .map(|(probe_key, table_index)| (probe_key, table_index))
        })
    {
        if let Some(column_name) = Expr::<String>::column_id(probe_key) {
            columns.push((*table_index, column_name));
        }
    }
    columns
}

pub fn supported_runtime_filter_data_type(data_type: &DataType) -> bool {
    let data_type = data_type.remove_nullable();
    data_type.is_numeric()
        || data_type.is_string()
        || data_type.is_date()
        || data_type.is_timestamp()
}

pub fn supported_runtime_filter_join_type(join_type: &JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner | JoinType::Right | JoinType::RightSemi | JoinType::LeftMark
    )
}
