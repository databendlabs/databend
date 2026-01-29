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

use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_sql::ColumnSet;
use databend_common_sql::MetadataRef;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::RelOperator;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::physical_plan::PhysicalPlan;

pub struct PhysicalPlanBuilder {
    pub metadata: MetadataRef,
    pub ctx: Arc<dyn TableContext>,
    pub func_ctx: FunctionContext,
    pub dry_run: bool,
    // DataMutation info, used to build MergeInto physical plan
    pub mutation_build_info: Option<MutationBuildInfo>,
    pub cte_required_columns: HashMap<String, ColumnSet>,
    pub is_cte_required_columns_collected: bool,
}

impl PhysicalPlanBuilder {
    pub fn new(metadata: MetadataRef, ctx: Arc<dyn TableContext>, dry_run: bool) -> Self {
        let func_ctx = ctx.get_function_context().unwrap();
        Self {
            metadata,
            ctx,
            func_ctx,
            dry_run,
            mutation_build_info: None,
            cte_required_columns: HashMap::new(),
            is_cte_required_columns_collected: false,
        }
    }

    pub fn build_plan_stat_info(&self, s_expr: &SExpr) -> Result<PlanStatsInfo> {
        let rel_expr = RelExpr::with_s_expr(s_expr);
        let stat_info = rel_expr.derive_cardinality()?;

        Ok(PlanStatsInfo {
            estimated_rows: stat_info.cardinality,
        })
    }

    pub async fn build(&mut self, s_expr: &SExpr, required: ColumnSet) -> Result<PhysicalPlan> {
        if !self.is_cte_required_columns_collected {
            self.collect_cte_required_columns(s_expr, required.clone())?;
            self.is_cte_required_columns_collected = true;
        }

        let mut plan = self.build_physical_plan(s_expr, required).await?;
        plan.adjust_plan_id(&mut 0);

        Ok(plan)
    }

    #[async_recursion::async_recursion(#[recursive::recursive])]
    pub async fn build_physical_plan(
        &mut self,
        s_expr: &SExpr,
        required: ColumnSet,
    ) -> Result<PhysicalPlan> {
        // Build stat info.
        let stat_info = self.build_plan_stat_info(s_expr)?;
        match s_expr.plan() {
            RelOperator::Scan(scan) => self.build_table_scan(scan, required, stat_info).await,
            RelOperator::DummyTableScan(dummy_scan) => {
                self.build_dummy_table_scan(dummy_scan).await
            }
            RelOperator::Join(join) => self.build_join(s_expr, join, required, stat_info).await,
            RelOperator::EvalScalar(eval_scalar) => {
                self.build_eval_scalar(s_expr, eval_scalar, required, stat_info)
                    .await
            }
            RelOperator::Filter(filter) => {
                self.build_filter(s_expr, filter, required, stat_info).await
            }
            RelOperator::SecureFilter(secure_filter) => {
                self.build_secure_filter(s_expr, secure_filter, required, stat_info)
                    .await
            }
            RelOperator::Aggregate(agg) => {
                self.build_aggregate(s_expr, agg, required, stat_info).await
            }
            RelOperator::Window(window) => {
                self.build_window(s_expr, window, required, stat_info).await
            }
            RelOperator::Sort(sort) => self.build_sort(s_expr, sort, required, stat_info).await,
            RelOperator::Limit(limit) => self.build_limit(s_expr, limit, required, stat_info).await,
            RelOperator::Exchange(exchange) => {
                self.build_exchange(s_expr, exchange, required).await
            }
            RelOperator::UnionAll(union_all) => {
                self.build_union_all(s_expr, union_all, required, stat_info)
                    .await
            }
            RelOperator::ProjectSet(project_set) => {
                self.build_project_set(s_expr, project_set, required, stat_info)
                    .await
            }
            RelOperator::ConstantTableScan(scan) => {
                self.build_constant_table_scan(scan, required).await
            }
            RelOperator::ExpressionScan(scan) => {
                self.build_expression_scan(s_expr, scan, required).await
            }
            RelOperator::CacheScan(scan) => self.build_cache_scan(scan, required).await,
            RelOperator::Udf(udf) => self.build_udf(s_expr, udf, required, stat_info).await,
            RelOperator::RecursiveCteScan(scan) => {
                self.build_recursive_cte_scan(scan, stat_info).await
            }
            RelOperator::AsyncFunction(async_func) => {
                self.build_async_func(s_expr, async_func, required, stat_info)
                    .await
            }
            RelOperator::Mutation(mutation) => {
                self.build_mutation(s_expr, mutation, required).await
            }
            RelOperator::MutationSource(mutation_source) => {
                self.build_mutation_source(mutation_source).await
            }
            RelOperator::CompactBlock(compact) => self.build_compact_block(compact).await,
            RelOperator::MaterializedCTE(materialized_cte) => {
                self.build_materialized_cte(s_expr, materialized_cte, stat_info)
                    .await
            }
            RelOperator::MaterializedCTERef(cte_consumer) => {
                self.build_cte_consumer(cte_consumer, stat_info).await
            }
            RelOperator::Sequence(sequence) => {
                self.build_sequence(s_expr, sequence, stat_info, required)
                    .await
            }
        }
    }

    pub fn set_mutation_build_info(&mut self, mutation_build_info: MutationBuildInfo) {
        self.mutation_build_info = Some(mutation_build_info);
    }

    pub fn set_metadata(&mut self, metadata: MetadataRef) {
        self.metadata = metadata;
    }

    pub(crate) fn derive_children_required_columns(
        &self,
        s_expr: &SExpr,
        parent_required: &ColumnSet,
    ) -> Result<Vec<ColumnSet>> {
        let arity = s_expr.arity();
        if arity == 0 {
            return Ok(vec![]);
        }

        let mut child_required: Vec<ColumnSet> =
            (0..arity).map(|_| parent_required.clone()).collect();

        match s_expr.plan() {
            RelOperator::EvalScalar(eval_scalar) => {
                let req = &mut child_required[0];
                for item in &eval_scalar.items {
                    if parent_required.contains(&item.index) {
                        for col in item.scalar.used_columns() {
                            req.insert(col);
                        }
                    }
                }
            }
            RelOperator::Filter(filter) => {
                let req = &mut child_required[0];
                for predicate in &filter.predicates {
                    req.extend(predicate.used_columns());
                }
            }
            RelOperator::SecureFilter(filter) => {
                let req = &mut child_required[0];
                for predicate in &filter.predicates {
                    req.extend(predicate.used_columns());
                }
            }
            RelOperator::Aggregate(agg) => {
                let req = &mut child_required[0];
                for item in &agg.group_items {
                    req.insert(item.index);
                    for col in item.scalar.used_columns() {
                        req.insert(col);
                    }
                }
                for item in &agg.aggregate_functions {
                    if parent_required.contains(&item.index) {
                        for col in item.scalar.used_columns() {
                            req.insert(col);
                        }
                    }
                }
            }
            RelOperator::Window(window) => {
                let req = &mut child_required[0];
                for item in &window.arguments {
                    req.extend(item.scalar.used_columns());
                    req.insert(item.index);
                }
                for item in &window.partition_by {
                    req.extend(item.scalar.used_columns());
                    req.insert(item.index);
                }
                for item in &window.order_by {
                    req.extend(item.order_by_item.scalar.used_columns());
                    req.insert(item.order_by_item.index);
                }
            }
            RelOperator::Sort(sort) => {
                let req = &mut child_required[0];
                for item in &sort.items {
                    req.insert(item.index);
                }
            }
            RelOperator::Join(join) => {
                let mut others_required = join
                    .non_equi_conditions
                    .iter()
                    .fold(parent_required.clone(), |acc, v| {
                        acc.union(&v.used_columns()).cloned().collect()
                    });
                if let Some(cache_info) = &join.build_side_cache_info {
                    for column in &cache_info.columns {
                        others_required.insert(*column);
                    }
                }

                let left_required: ColumnSet = join
                    .equi_conditions
                    .iter()
                    .fold(parent_required.clone(), |acc, v| {
                        acc.union(&v.left.used_columns()).cloned().collect()
                    })
                    .union(&others_required)
                    .cloned()
                    .collect();
                let right_required: ColumnSet = join
                    .equi_conditions
                    .iter()
                    .fold(parent_required.clone(), |acc, v| {
                        acc.union(&v.right.used_columns()).cloned().collect()
                    })
                    .union(&others_required)
                    .cloned()
                    .collect();

                child_required[0] = left_required.union(&others_required).cloned().collect();
                child_required[1] = right_required.union(&others_required).cloned().collect();
            }
            RelOperator::UnionAll(union_all) => {
                let (left_required, right_required) = if !union_all.cte_scan_names.is_empty() {
                    let left: ColumnSet = union_all
                        .left_outputs
                        .iter()
                        .map(|(index, _)| *index)
                        .collect();
                    let right: ColumnSet = union_all
                        .right_outputs
                        .iter()
                        .map(|(index, _)| *index)
                        .collect();

                    (left, right)
                } else {
                    let offset_indices: Vec<usize> = (0..union_all.left_outputs.len())
                        .filter(|index| parent_required.contains(&union_all.output_indexes[*index]))
                        .collect();

                    if offset_indices.is_empty() {
                        (
                            ColumnSet::from([union_all.left_outputs[0].0]),
                            ColumnSet::from([union_all.right_outputs[0].0]),
                        )
                    } else {
                        offset_indices.iter().fold(
                            (ColumnSet::default(), ColumnSet::default()),
                            |(mut left, mut right), &index| {
                                left.insert(union_all.left_outputs[index].0);
                                right.insert(union_all.right_outputs[index].0);
                                (left, right)
                            },
                        )
                    }
                };
                child_required[0] = left_required;
                child_required[1] = right_required;
            }
            RelOperator::Exchange(databend_common_sql::plans::Exchange::NodeToNodeHash(exprs)) => {
                let req = &mut child_required[0];
                for expr in exprs {
                    req.extend(expr.used_columns());
                }
            }
            RelOperator::ProjectSet(project_set) => {
                let req = &mut child_required[0];
                for item in &project_set.srfs {
                    if parent_required.contains(&item.index) {
                        for col in item.scalar.used_columns() {
                            req.insert(col);
                        }
                    }
                }
            }
            RelOperator::Udf(udf) => {
                let req = &mut child_required[0];
                for item in &udf.items {
                    if parent_required.contains(&item.index) {
                        for col in item.scalar.used_columns() {
                            req.insert(col);
                        }
                    }
                }
            }
            RelOperator::AsyncFunction(async_func) => {
                let req = &mut child_required[0];
                for item in &async_func.items {
                    if parent_required.contains(&item.index) {
                        for col in item.scalar.used_columns() {
                            req.insert(col);
                        }
                    }
                }
            }

            RelOperator::Scan(_)
            | RelOperator::DummyTableScan(_)
            | RelOperator::ConstantTableScan(_)
            | RelOperator::CacheScan(_)
            | RelOperator::RecursiveCteScan(_)
            | RelOperator::CompactBlock(_)
            | RelOperator::MutationSource(_)
            | RelOperator::MaterializedCTERef(_)
            | RelOperator::Sequence(_)
            | RelOperator::MaterializedCTE(_)
            | RelOperator::Mutation(_)
            | RelOperator::ExpressionScan(_)
            | RelOperator::Limit(_)
            | RelOperator::Exchange(_) => {}
        }

        Ok(child_required)
    }

    fn collect_cte_required_columns(&mut self, s_expr: &SExpr, required: ColumnSet) -> Result<()> {
        match s_expr.plan() {
            RelOperator::MaterializedCTERef(cte_ref) => {
                let mut required_mapped = ColumnSet::new();
                for col in required {
                    if let Some(mapped) = cte_ref.column_mapping.get(&col) {
                        required_mapped.insert(*mapped);
                    }
                }
                self.cte_required_columns
                    .entry(cte_ref.cte_name.clone())
                    .and_modify(|cols| {
                        *cols = cols.union(&required_mapped).cloned().collect();
                    })
                    .or_insert(required_mapped);
                Ok(())
            }
            RelOperator::Sequence(_) => {
                // The right child contains all consumers for the left CTE definition.
                self.collect_cte_required_columns(s_expr.child(1)?, required.clone())?;

                let left_child = s_expr.child(0)?;
                let RelOperator::MaterializedCTE(cte) = left_child.plan() else {
                    return Err(ErrorCode::Internal(
                        "Sequence left child is expected to be MaterializedCTE".to_string(),
                    ));
                };
                let cte_required = self
                    .cte_required_columns
                    .get(&cte.cte_name)
                    .ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "CTE required columns not found for CTE name: {}",
                            cte.cte_name
                        ))
                    })?
                    .clone();
                self.collect_cte_required_columns(left_child.child(0)?, cte_required)?;
                Ok(())
            }
            _ => {
                let child_required = self.derive_children_required_columns(s_expr, &required)?;
                for (idx, columns) in child_required.into_iter().enumerate() {
                    self.collect_cte_required_columns(s_expr.child(idx)?, columns)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Clone)]
pub struct MutationBuildInfo {
    pub table_info: TableInfo,
    pub table_snapshot: Option<Arc<TableSnapshot>>,
    pub update_stream_meta: Vec<UpdateStreamMetaReq>,
    pub partitions: Partitions,
    pub statistics: PartStatistics,
    pub table_meta_timestamps: TableMetaTimestamps,
}
