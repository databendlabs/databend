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

use databend_common_ast::ast::FormatTreeNode;
use databend_common_base::base::format_byte_size;
use databend_common_base::runtime::profile::get_statistics_desc;
use databend_common_catalog::plan::PartStatistics;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::PlanProfile;
use itertools::Itertools;

use super::physical_plans::AddStreamColumn;
use crate::binder::MutationType;
use crate::executor::explain::PlanStatsInfo;
use crate::executor::physical_plans::AggregateExpand;
use crate::executor::physical_plans::AggregateFinal;
use crate::executor::physical_plans::AggregateFunctionDesc;
use crate::executor::physical_plans::AggregatePartial;
use crate::executor::physical_plans::AsyncFunction;
use crate::executor::physical_plans::CacheScan;
use crate::executor::physical_plans::ColumnMutation;
use crate::executor::physical_plans::CommitSink;
use crate::executor::physical_plans::ConstantTableScan;
use crate::executor::physical_plans::CopyIntoLocation;
use crate::executor::physical_plans::CopyIntoTable;
use crate::executor::physical_plans::DistributedInsertSelect;
use crate::executor::physical_plans::EvalScalar;
use crate::executor::physical_plans::Exchange;
use crate::executor::physical_plans::ExchangeSink;
use crate::executor::physical_plans::ExchangeSource;
use crate::executor::physical_plans::ExpressionScan;
use crate::executor::physical_plans::Filter;
use crate::executor::physical_plans::FragmentKind;
use crate::executor::physical_plans::HashJoin;
use crate::executor::physical_plans::Limit;
use crate::executor::physical_plans::Mutation;
use crate::executor::physical_plans::MutationManipulate;
use crate::executor::physical_plans::MutationOrganize;
use crate::executor::physical_plans::MutationSource;
use crate::executor::physical_plans::MutationSplit;
use crate::executor::physical_plans::ProjectSet;
use crate::executor::physical_plans::RangeJoin;
use crate::executor::physical_plans::RangeJoinType;
use crate::executor::physical_plans::RowFetch;
use crate::executor::physical_plans::Sort;
use crate::executor::physical_plans::TableScan;
use crate::executor::physical_plans::Udf;
use crate::executor::physical_plans::UnionAll;
use crate::executor::physical_plans::Window;
use crate::executor::physical_plans::WindowFunction;
use crate::executor::physical_plans::WindowPartition;
use crate::executor::PhysicalPlan;
use crate::planner::Metadata;
use crate::planner::MetadataRef;
use crate::planner::DUMMY_TABLE_INDEX;
use crate::plans::CacheSource;

impl PhysicalPlan {
    pub fn format(
        &self,
        metadata: MetadataRef,
        profs: HashMap<u32, PlanProfile>,
    ) -> Result<FormatTreeNode<String>> {
        let metadata = metadata.read().clone();
        to_format_tree(self, &metadata, &profs)
    }

    #[recursive::recursive]
    pub fn format_join(&self, metadata: &MetadataRef) -> Result<FormatTreeNode<String>> {
        match self {
            PhysicalPlan::TableScan(plan) => {
                if plan.table_index == Some(DUMMY_TABLE_INDEX) {
                    return Ok(FormatTreeNode::with_children(
                        format!("Scan: dummy, rows: {}", plan.source.statistics.read_rows),
                        vec![],
                    ));
                }

                match plan.table_index {
                    None => Ok(FormatTreeNode::with_children(
                        format!(
                            "Scan: {}.{} (read rows: {})",
                            plan.source.source_info.catalog_name(),
                            plan.source.source_info.desc(),
                            plan.source.statistics.read_rows
                        ),
                        vec![],
                    )),
                    Some(table_index) => {
                        let table = metadata.read().table(table_index).clone();
                        let table_name =
                            format!("{}.{}.{}", table.catalog(), table.database(), table.name());

                        Ok(FormatTreeNode::with_children(
                            format!(
                                "Scan: {} (#{}) (read rows: {})",
                                table_name, table_index, plan.source.statistics.read_rows
                            ),
                            vec![],
                        ))
                    }
                }
            }
            PhysicalPlan::HashJoin(plan) => {
                let build_child = plan.build.format_join(metadata)?;
                let probe_child = plan.probe.format_join(metadata)?;

                let children = vec![
                    FormatTreeNode::with_children("Build".to_string(), vec![build_child]),
                    FormatTreeNode::with_children("Probe".to_string(), vec![probe_child]),
                ];

                let _estimated_rows = if let Some(info) = &plan.stat_info {
                    format!("{0:.2}", info.estimated_rows)
                } else {
                    String::from("None")
                };

                Ok(FormatTreeNode::with_children(
                    format!("HashJoin: {}", plan.join_type),
                    children,
                ))
            }
            PhysicalPlan::RangeJoin(plan) => {
                let left_child = plan.left.format_join(metadata)?;
                let right_child = plan.right.format_join(metadata)?;

                let children = vec![
                    FormatTreeNode::with_children("Left".to_string(), vec![left_child]),
                    FormatTreeNode::with_children("Right".to_string(), vec![right_child]),
                ];

                let _estimated_rows = if let Some(info) = &plan.stat_info {
                    format!("{0:.2}", info.estimated_rows)
                } else {
                    String::from("none")
                };

                Ok(FormatTreeNode::with_children(
                    format!("RangeJoin: {}", plan.join_type),
                    children,
                ))
            }
            PhysicalPlan::UnionAll(union_all) => {
                let left_child = union_all.left.format_join(metadata)?;
                let right_child = union_all.right.format_join(metadata)?;

                let children = vec![
                    FormatTreeNode::with_children("Left".to_string(), vec![left_child]),
                    FormatTreeNode::with_children("Right".to_string(), vec![right_child]),
                ];

                Ok(FormatTreeNode::with_children(
                    "UnionAll".to_string(),
                    children,
                ))
            }
            other => {
                let children = other
                    .children()
                    .map(|child| child.format_join(metadata))
                    .collect::<Result<Vec<FormatTreeNode<String>>>>()?;

                if children.len() == 1 {
                    Ok(children[0].clone())
                } else {
                    Ok(FormatTreeNode::with_children(
                        format!("{:?}", other),
                        children,
                    ))
                }
            }
        }
    }
}

// The method will only collect scan,filter and join nodes
// It's only used to debug cardinality estimator.
#[recursive::recursive]
pub fn format_partial_tree(
    plan: &PhysicalPlan,
    metadata: &MetadataRef,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    match plan {
        PhysicalPlan::TableScan(plan) => {
            if plan.table_index == Some(DUMMY_TABLE_INDEX) {
                return Ok(FormatTreeNode::new("DummyTableScan".to_string()));
            }
            let table_name = match plan.table_index {
                None => format!(
                    "{}.{}",
                    plan.source.source_info.catalog_name(),
                    plan.source.source_info.desc()
                ),
                Some(table_index) => {
                    let metadata = metadata.read().clone();
                    let table = metadata.table(table_index).clone();
                    format!("{}.{}.{}", table.catalog(), table.database(), table.name())
                }
            };
            let mut children = vec![FormatTreeNode::new(format!("table: {table_name}"))];
            if let Some(info) = &plan.stat_info {
                let items = plan_stats_info_to_format_tree(info);
                children.extend(items);
            }
            append_output_rows_info(&mut children, profs, plan.plan_id);

            Ok(FormatTreeNode::with_children(
                "TableScan".to_string(),
                children,
            ))
        }
        PhysicalPlan::Filter(plan) => {
            let filter = plan
                .predicates
                .iter()
                .map(|pred| pred.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                .join(", ");
            let mut children = vec![FormatTreeNode::new(format!("filters: [{filter}]"))];
            if let Some(info) = &plan.stat_info {
                let items = plan_stats_info_to_format_tree(info);
                children.extend(items);
            }
            append_output_rows_info(&mut children, profs, plan.plan_id);
            children.push(format_partial_tree(&plan.input, metadata, profs)?);
            Ok(FormatTreeNode::with_children(
                "Filter".to_string(),
                children,
            ))
        }
        PhysicalPlan::HashJoin(plan) => {
            let build_child = format_partial_tree(&plan.build, metadata, profs)?;
            let probe_child = format_partial_tree(&plan.probe, metadata, profs)?;
            let mut children = vec![];
            if let Some(info) = &plan.stat_info {
                let items = plan_stats_info_to_format_tree(info);
                children.extend(items);
            }
            append_output_rows_info(&mut children, profs, plan.plan_id);
            children.push(build_child);
            children.push(probe_child);

            Ok(FormatTreeNode::with_children(
                format!("HashJoin: {}", plan.join_type),
                children,
            ))
        }
        PhysicalPlan::RangeJoin(plan) => {
            let left_child = format_partial_tree(&plan.left, metadata, profs)?;
            let right_child = format_partial_tree(&plan.right, metadata, profs)?;
            let mut children = vec![];
            if let Some(info) = &plan.stat_info {
                let items = plan_stats_info_to_format_tree(info);
                children.extend(items);
            }
            append_output_rows_info(&mut children, profs, plan.plan_id);

            let children = vec![
                FormatTreeNode::with_children("Left".to_string(), vec![left_child]),
                FormatTreeNode::with_children("Right".to_string(), vec![right_child]),
            ];

            Ok(FormatTreeNode::with_children(
                format!("RangeJoin: {}", plan.join_type),
                children,
            ))
        }
        PhysicalPlan::UnionAll(union_all) => {
            let left_child = format_partial_tree(&union_all.left, metadata, profs)?;
            let right_child = format_partial_tree(&union_all.right, metadata, profs)?;
            let mut children = vec![];
            if let Some(info) = &union_all.stat_info {
                let items = plan_stats_info_to_format_tree(info);
                children.extend(items);
            }
            append_output_rows_info(&mut children, profs, union_all.plan_id);
            let children = vec![
                FormatTreeNode::with_children("Left".to_string(), vec![left_child]),
                FormatTreeNode::with_children("Right".to_string(), vec![right_child]),
            ];

            Ok(FormatTreeNode::with_children(
                "UnionAll".to_string(),
                children,
            ))
        }
        PhysicalPlan::MutationSource(plan) => {
            let metadata = metadata.read().clone();
            let table = metadata.table(plan.table_index).clone();
            let table_name = format!("{}.{}.{}", table.catalog(), table.database(), table.name());
            let mut children = vec![FormatTreeNode::new(format!("table: {table_name}"))];
            if let Some(filters) = &plan.filters {
                let filter = filters.filter.as_expr(&BUILTIN_FUNCTIONS).sql_display();
                children.push(FormatTreeNode::new(format!("filters: [{filter}]")));
            }
            append_output_rows_info(&mut children, profs, plan.plan_id);
            Ok(FormatTreeNode::with_children(
                "MutationSource".to_string(),
                children,
            ))
        }
        other => {
            let children = other
                .children()
                .map(|child| format_partial_tree(child, metadata, profs))
                .collect::<Result<Vec<FormatTreeNode<String>>>>()?;

            if children.len() == 1 {
                Ok(children[0].clone())
            } else {
                Ok(FormatTreeNode::with_children(
                    format!("{:?}", other),
                    children,
                ))
            }
        }
    }
}

#[recursive::recursive]
fn to_format_tree(
    plan: &PhysicalPlan,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    match plan {
        PhysicalPlan::TableScan(plan) => table_scan_to_format_tree(plan, metadata, profs),
        PhysicalPlan::Filter(plan) => filter_to_format_tree(plan, metadata, profs),
        PhysicalPlan::EvalScalar(plan) => eval_scalar_to_format_tree(plan, metadata, profs),
        PhysicalPlan::AggregateExpand(plan) => {
            aggregate_expand_to_format_tree(plan, metadata, profs)
        }
        PhysicalPlan::AggregatePartial(plan) => {
            aggregate_partial_to_format_tree(plan, metadata, profs)
        }
        PhysicalPlan::AggregateFinal(plan) => aggregate_final_to_format_tree(plan, metadata, profs),
        PhysicalPlan::Window(plan) => window_to_format_tree(plan, metadata, profs),
        PhysicalPlan::WindowPartition(plan) => {
            window_partition_to_format_tree(plan, metadata, profs)
        }
        PhysicalPlan::Sort(plan) => sort_to_format_tree(plan, metadata, profs),
        PhysicalPlan::Limit(plan) => limit_to_format_tree(plan, metadata, profs),
        PhysicalPlan::RowFetch(plan) => row_fetch_to_format_tree(plan, metadata, profs),
        PhysicalPlan::HashJoin(plan) => hash_join_to_format_tree(plan, metadata, profs),
        PhysicalPlan::Exchange(plan) => exchange_to_format_tree(plan, metadata, profs),
        PhysicalPlan::UnionAll(plan) => union_all_to_format_tree(plan, metadata, profs),
        PhysicalPlan::ExchangeSource(plan) => exchange_source_to_format_tree(plan, metadata),
        PhysicalPlan::ExchangeSink(plan) => exchange_sink_to_format_tree(plan, metadata, profs),
        PhysicalPlan::DistributedInsertSelect(plan) => {
            distributed_insert_to_format_tree(plan.as_ref(), metadata, profs)
        }
        PhysicalPlan::Recluster(_) => Ok(FormatTreeNode::new("Recluster".to_string())),
        PhysicalPlan::HilbertPartition(_) => {
            Ok(FormatTreeNode::new("HilbertPartition".to_string()))
        }
        PhysicalPlan::CompactSource(_) => Ok(FormatTreeNode::new("CompactSource".to_string())),
        PhysicalPlan::CommitSink(plan) => commit_sink_to_format_tree(plan, metadata, profs),
        PhysicalPlan::ProjectSet(plan) => project_set_to_format_tree(plan, metadata, profs),
        PhysicalPlan::Udf(plan) => udf_to_format_tree(plan, metadata, profs),
        PhysicalPlan::RangeJoin(plan) => range_join_to_format_tree(plan, metadata, profs),
        PhysicalPlan::CopyIntoTable(plan) => copy_into_table(plan),
        PhysicalPlan::CopyIntoLocation(plan) => copy_into_location(plan),
        PhysicalPlan::ReplaceAsyncSourcer(_) => {
            Ok(FormatTreeNode::new("ReplaceAsyncSourcer".to_string()))
        }
        PhysicalPlan::ReplaceDeduplicate(_) => {
            Ok(FormatTreeNode::new("ReplaceDeduplicate".to_string()))
        }
        PhysicalPlan::ReplaceInto(_) => Ok(FormatTreeNode::new("Replace".to_string())),
        PhysicalPlan::MutationSource(plan) => format_mutation_source(plan, metadata, profs),
        PhysicalPlan::ColumnMutation(plan) => format_column_mutation(plan, metadata, profs),
        PhysicalPlan::Mutation(plan) => format_merge_into(plan, metadata, profs),
        PhysicalPlan::MutationSplit(plan) => format_merge_into_split(plan, metadata, profs),
        PhysicalPlan::MutationManipulate(plan) => {
            format_merge_into_manipulate(plan, metadata, profs)
        }
        PhysicalPlan::MutationOrganize(plan) => format_merge_into_organize(plan, metadata, profs),
        PhysicalPlan::AddStreamColumn(plan) => format_add_stream_column(plan, metadata, profs),
        PhysicalPlan::RecursiveCteScan(_) => {
            Ok(FormatTreeNode::new("RecursiveCTEScan".to_string()))
        }
        PhysicalPlan::ConstantTableScan(plan) => constant_table_scan_to_format_tree(plan, metadata),
        PhysicalPlan::ExpressionScan(plan) => expression_scan_to_format_tree(plan, metadata, profs),
        PhysicalPlan::CacheScan(plan) => cache_scan_to_format_tree(plan, metadata),
        PhysicalPlan::Duplicate(plan) => {
            let mut children = Vec::new();
            children.push(FormatTreeNode::new(format!(
                "Duplicate data to {} branch",
                plan.n
            )));
            append_profile_info(&mut children, profs, plan.plan_id);
            children.push(to_format_tree(&plan.input, metadata, profs)?);
            Ok(FormatTreeNode::with_children(
                "Duplicate".to_string(),
                children,
            ))
        }
        PhysicalPlan::Shuffle(plan) => to_format_tree(&plan.input, metadata, profs), /* will be hided in explain */
        PhysicalPlan::ChunkFilter(plan) => {
            if plan.predicates.iter().all(|x| x.is_none()) {
                return to_format_tree(&plan.input, metadata, profs);
            }
            let mut children = Vec::new();
            for (i, predicate) in plan.predicates.iter().enumerate() {
                if let Some(predicate) = predicate {
                    children.push(FormatTreeNode::new(format!(
                        "branch {}: {}",
                        i,
                        predicate.as_expr(&BUILTIN_FUNCTIONS).sql_display()
                    )));
                } else {
                    children.push(FormatTreeNode::new(format!("branch {}: None", i)));
                }
            }
            append_profile_info(&mut children, profs, plan.plan_id);
            children.push(to_format_tree(&plan.input, metadata, profs)?);
            Ok(FormatTreeNode::with_children(
                "Filter".to_string(),
                children,
            ))
        }
        PhysicalPlan::ChunkEvalScalar(plan) => {
            let mut children = Vec::new();
            if plan.eval_scalars.iter().all(|x| x.is_none()) {
                return to_format_tree(&plan.input, metadata, profs);
            }
            for (i, eval_scalar) in plan.eval_scalars.iter().enumerate() {
                if let Some(eval_scalar) = eval_scalar {
                    children.push(FormatTreeNode::new(format!(
                        "branch {}: {}",
                        i,
                        eval_scalar
                            .remote_exprs
                            .iter()
                            .map(|x| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                            .join(", ")
                    )));
                } else {
                    children.push(FormatTreeNode::new(format!("branch {}: None", i)));
                }
            }
            append_profile_info(&mut children, profs, plan.plan_id);
            children.push(to_format_tree(&plan.input, metadata, profs)?);
            Ok(FormatTreeNode::with_children(
                "EvalScalar".to_string(),
                children,
            ))
        }
        PhysicalPlan::ChunkCastSchema(plan) => to_format_tree(&plan.input, metadata, profs), /* will be hided in explain */
        PhysicalPlan::ChunkFillAndReorder(plan) => to_format_tree(&plan.input, metadata, profs), /* will be hided in explain */
        PhysicalPlan::ChunkAppendData(plan) => {
            let mut children = Vec::new();
            append_profile_info(&mut children, profs, plan.plan_id);
            children.push(to_format_tree(&plan.input, metadata, profs)?);
            Ok(FormatTreeNode::with_children(
                "WriteData".to_string(),
                children,
            ))
        }
        PhysicalPlan::ChunkMerge(plan) => to_format_tree(&plan.input, metadata, profs), /* will be hided in explain */
        PhysicalPlan::ChunkCommitInsert(plan) => {
            let mut children = Vec::new();
            append_profile_info(&mut children, profs, plan.plan_id);
            children.push(to_format_tree(&plan.input, metadata, profs)?);
            Ok(FormatTreeNode::with_children(
                "Commit".to_string(),
                children,
            ))
        }
        PhysicalPlan::AsyncFunction(plan) => async_function_to_format_tree(plan, metadata, profs),
    }
}

/// Helper function to add profile info to the format tree.
fn append_profile_info(
    children: &mut Vec<FormatTreeNode<String>>,
    profs: &HashMap<u32, PlanProfile>,
    plan_id: u32,
) {
    if let Some(prof) = profs.get(&plan_id) {
        for (_, desc) in get_statistics_desc().iter() {
            if prof.statistics[desc.index] != 0 {
                children.push(FormatTreeNode::new(format!(
                    "{}: {}",
                    desc.display_name.to_lowercase(),
                    desc.human_format(prof.statistics[desc.index])
                )));
            }
        }
    }
}

fn append_output_rows_info(
    children: &mut Vec<FormatTreeNode<String>>,
    profs: &HashMap<u32, PlanProfile>,
    plan_id: u32,
) {
    if let Some(prof) = profs.get(&plan_id) {
        for (_, desc) in get_statistics_desc().iter() {
            if desc.display_name != "output rows" {
                continue;
            }
            if prof.statistics[desc.index] != 0 {
                children.push(FormatTreeNode::new(format!(
                    "{}: {}",
                    desc.display_name.to_lowercase(),
                    desc.human_format(prof.statistics[desc.index])
                )));
            }
            break;
        }
    }
}

fn format_mutation_source(
    plan: &MutationSource,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let table = metadata.table(plan.table_index);
    let table_name = format!("{}.{}.{}", table.catalog(), table.database(), table.name());
    let filters = plan
        .filters
        .as_ref()
        .map(|filters| filters.filter.as_expr(&BUILTIN_FUNCTIONS).sql_display())
        .unwrap_or_default();
    let mut children = vec![
        FormatTreeNode::new(format!("table: {table_name}")),
        FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(plan.output_schema()?, metadata, false)
        )),
        FormatTreeNode::new(format!("filters: [{filters}]")),
    ];

    let payload = match plan.input_type {
        MutationType::Update => "Update",
        MutationType::Delete => {
            if plan.truncate_table {
                "DeleteAll"
            } else {
                "Delete"
            }
        }
        MutationType::Merge => "Merge",
    };

    // Part stats.
    children.extend(part_stats_info_to_format_tree(&plan.statistics));
    append_profile_info(&mut children, profs, plan.plan_id);

    Ok(FormatTreeNode::with_children(
        format!("MutationSource({})", payload),
        children,
    ))
}

fn format_column_mutation(
    plan: &ColumnMutation,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    to_format_tree(&plan.input, metadata, profs)
}

fn format_merge_into(
    merge_into: &Mutation,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let table_entry = metadata.table(merge_into.target_table_index).clone();
    let target_table = vec![FormatTreeNode::new(format!(
        "target table: [catalog: {}] [database: {}] [table: {}]",
        table_entry.catalog(),
        table_entry.database(),
        table_entry.name()
    ))];
    let target_schema = table_entry.table().schema_with_stream();

    let merge_into_organize: &PhysicalPlan = &merge_into.input;
    let merge_into_manipulate: &PhysicalPlan =
        if let PhysicalPlan::MutationOrganize(plan) = merge_into_organize {
            &plan.input
        } else {
            return Err(ErrorCode::Internal(
                "Expect MutationOrganize after MergeIntoSerialize ".to_string(),
            ));
        };

    let children = if let PhysicalPlan::MutationManipulate(plan) = merge_into_manipulate {
        // Matched clauses.
        let mut matched_children = Vec::with_capacity(plan.matched.len());
        for evaluator in &plan.matched {
            let condition_format = evaluator.0.as_ref().map_or_else(
                || "condition: None".to_string(),
                |predicate| {
                    format!(
                        "condition: {}",
                        predicate.as_expr(&BUILTIN_FUNCTIONS).sql_display()
                    )
                },
            );
            if evaluator.1.is_none() {
                matched_children.push(FormatTreeNode::new(format!(
                    "matched delete: [{}]",
                    condition_format
                )));
            } else {
                let mut update_list = evaluator.1.as_ref().unwrap().clone();
                update_list.sort_by(|a, b| a.0.cmp(&b.0));
                let update_format = update_list
                    .iter()
                    .map(|(field_idx, expr)| {
                        format!(
                            "{} = {}",
                            target_schema.field(*field_idx).name(),
                            expr.as_expr(&BUILTIN_FUNCTIONS).sql_display()
                        )
                    })
                    .join(",");
                matched_children.push(FormatTreeNode::new(format!(
                    "matched update: [{}, update set {}]",
                    condition_format, update_format
                )));
            }
        }

        // UnMatched clauses.
        let mut unmatched_children = Vec::with_capacity(plan.unmatched.len());
        for evaluator in &plan.unmatched {
            let condition_format = evaluator.1.as_ref().map_or_else(
                || "condition: None".to_string(),
                |predicate| {
                    format!(
                        "condition: {}",
                        predicate.as_expr(&BUILTIN_FUNCTIONS).sql_display()
                    )
                },
            );
            let insert_schema_format = evaluator
                .0
                .fields
                .iter()
                .map(|field| field.name())
                .join(",");
            let values_format = evaluator
                .2
                .iter()
                .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                .join(",");
            let unmatched_format = format!(
                "insert into ({}) values({})",
                insert_schema_format, values_format
            );
            unmatched_children.push(FormatTreeNode::new(format!(
                "unmatched insert: [{}, {}]",
                condition_format, unmatched_format
            )));
        }

        [target_table, matched_children, unmatched_children, vec![
            to_format_tree(&plan.input, metadata, profs)?,
        ]]
        .concat()
    } else {
        return Err(ErrorCode::Internal(
            "Expect MutationManipulate after MutationOrganize ".to_string(),
        ));
    };
    Ok(FormatTreeNode::with_children(
        "DataMutation".to_string(),
        children,
    ))
}

fn format_merge_into_split(
    plan: &MutationSplit,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    to_format_tree(&plan.input, metadata, profs)
}

fn format_merge_into_manipulate(
    plan: &MutationManipulate,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    to_format_tree(&plan.input, metadata, profs)
}

fn format_merge_into_organize(
    plan: &MutationOrganize,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    to_format_tree(&plan.input, metadata, profs)
}

fn format_add_stream_column(
    plan: &AddStreamColumn,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    to_format_tree(&plan.input, metadata, profs)
}

fn copy_into_table(plan: &CopyIntoTable) -> Result<FormatTreeNode<String>> {
    Ok(FormatTreeNode::new(format!(
        "CopyIntoTable: {}",
        plan.table_info
    )))
}

fn copy_into_location(_: &CopyIntoLocation) -> Result<FormatTreeNode<String>> {
    Ok(FormatTreeNode::new("CopyIntoLocation".to_string()))
}

fn table_scan_to_format_tree(
    plan: &TableScan,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    if plan.table_index == Some(DUMMY_TABLE_INDEX) {
        return Ok(FormatTreeNode::new("DummyTableScan".to_string()));
    }

    let table_name = match plan.table_index {
        None => format!(
            "{}.{}",
            plan.source.source_info.catalog_name(),
            plan.source.source_info.desc()
        ),
        Some(table_index) => {
            let table = metadata.table(table_index).clone();
            format!("{}.{}.{}", table.catalog(), table.database(), table.name())
        }
    };
    let filters = plan
        .source
        .push_downs
        .as_ref()
        .and_then(|extras| {
            extras
                .filters
                .as_ref()
                .map(|filters| filters.filter.as_expr(&BUILTIN_FUNCTIONS).sql_display())
        })
        .unwrap_or_default();

    let limit = plan
        .source
        .push_downs
        .as_ref()
        .map_or("NONE".to_string(), |extras| {
            extras
                .limit
                .map_or("NONE".to_string(), |limit| limit.to_string())
        });

    let virtual_columns = plan.source.push_downs.as_ref().and_then(|extras| {
        extras.virtual_column.as_ref().map(|virtual_column| {
            let mut names = virtual_column
                .virtual_column_fields
                .iter()
                .filter(|c| c.is_created)
                .map(|c| c.name.clone())
                .collect::<Vec<_>>();
            names.sort();
            names.iter().join(", ")
        })
    });
    let not_created_virtual_columns = plan.source.push_downs.as_ref().and_then(|extras| {
        extras.virtual_column.as_ref().map(|virtual_column| {
            let mut names = virtual_column
                .virtual_column_fields
                .iter()
                .filter(|c| !c.is_created)
                .map(|c| c.name.clone())
                .collect::<Vec<_>>();
            names.sort();
            names.iter().join(", ")
        })
    });

    let agg_index = plan
        .source
        .push_downs
        .as_ref()
        .and_then(|extras| extras.agg_index.as_ref());

    let mut children = vec![
        FormatTreeNode::new(format!("table: {table_name}")),
        FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(plan.output_schema()?, metadata, false)
        )),
    ];

    // Part stats.
    children.extend(part_stats_info_to_format_tree(&plan.source.statistics));
    // Push downs.
    let push_downs = format!("push downs: [filters: [{filters}], limit: {limit}]");
    children.push(FormatTreeNode::new(push_downs));

    // Virtual columns.
    if let Some(virtual_columns) = virtual_columns {
        if !virtual_columns.is_empty() {
            let virtual_columns = format!("virtual columns: [{virtual_columns}]");
            children.push(FormatTreeNode::new(virtual_columns));
        }
    }
    if let Some(not_created_virtual_columns) = not_created_virtual_columns {
        if !not_created_virtual_columns.is_empty() {
            let not_created_virtual_columns =
                format!("not created virtual columns: [{not_created_virtual_columns}]");
            children.push(FormatTreeNode::new(not_created_virtual_columns));
        }
    }

    // Aggregating index
    if let Some(agg_index) = agg_index {
        let (_, agg_index_sql, _) = metadata
            .get_agg_indexes(&table_name)
            .unwrap()
            .iter()
            .find(|(index, _, _)| *index == agg_index.index_id)
            .unwrap();

        children.push(FormatTreeNode::new(format!(
            "aggregating index: [{agg_index_sql}]"
        )));

        let agg_sel = agg_index
            .selection
            .iter()
            .map(|(expr, _)| expr.as_expr(&BUILTIN_FUNCTIONS).sql_display())
            .join(", ");
        let agg_filter = agg_index
            .filter
            .as_ref()
            .map(|f| f.as_expr(&BUILTIN_FUNCTIONS).sql_display());
        let text = if let Some(f) = agg_filter {
            format!("rewritten query: [selection: [{agg_sel}], filter: {f}]")
        } else {
            format!("rewritten query: [selection: [{agg_sel}]]")
        };
        children.push(FormatTreeNode::new(text));
    }

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    append_profile_info(&mut children, profs, plan.plan_id);

    Ok(FormatTreeNode::with_children(
        "TableScan".to_string(),
        children,
    ))
}

fn constant_table_scan_to_format_tree(
    plan: &ConstantTableScan,
    metadata: &Metadata,
) -> Result<FormatTreeNode<String>> {
    if plan.num_rows == 0 {
        return Ok(FormatTreeNode::new(plan.name().to_string()));
    }

    let mut children = Vec::with_capacity(plan.values.len() + 1);
    children.push(FormatTreeNode::new(format!(
        "output columns: [{}]",
        format_output_columns(plan.output_schema()?, metadata, true)
    )));
    for (i, value) in plan.values.iter().enumerate() {
        let column = value.iter().map(|val| format!("{val}")).join(", ");
        children.push(FormatTreeNode::new(format!("column {}: [{}]", i, column)));
    }
    Ok(FormatTreeNode::with_children(
        plan.name().to_string(),
        children,
    ))
}

fn expression_scan_to_format_tree(
    plan: &ExpressionScan,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let mut children = Vec::with_capacity(plan.values.len() + 1);
    children.push(FormatTreeNode::new(format!(
        "output columns: [{}]",
        format_output_columns(plan.output_schema()?, metadata, true)
    )));
    for (i, value) in plan.values.iter().enumerate() {
        let column = value
            .iter()
            .map(|val| val.as_expr(&BUILTIN_FUNCTIONS).sql_display())
            .join(", ");
        children.push(FormatTreeNode::new(format!("column {}: [{}]", i, column)));
    }

    children.push(to_format_tree(&plan.input, metadata, profs)?);

    Ok(FormatTreeNode::with_children(
        "ExpressionScan".to_string(),
        children,
    ))
}

fn cache_scan_to_format_tree(
    plan: &CacheScan,
    metadata: &Metadata,
) -> Result<FormatTreeNode<String>> {
    let mut children = Vec::with_capacity(2);
    children.push(FormatTreeNode::new(format!(
        "output columns: [{}]",
        format_output_columns(plan.output_schema()?, metadata, true)
    )));

    match &plan.cache_source {
        CacheSource::HashJoinBuild((cache_index, column_indexes)) => {
            let mut column_indexes = column_indexes.clone();
            column_indexes.sort();
            children.push(FormatTreeNode::new(format!("cache index: {}", cache_index)));
            children.push(FormatTreeNode::new(format!(
                "column indexes: {:?}",
                column_indexes
            )));
        }
    }

    Ok(FormatTreeNode::with_children(
        "CacheScan".to_string(),
        children,
    ))
}

fn filter_to_format_tree(
    plan: &Filter,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let filter = plan
        .predicates
        .iter()
        .map(|pred| pred.as_expr(&BUILTIN_FUNCTIONS).sql_display())
        .join(", ");
    let mut children = vec![
        FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(plan.output_schema()?, metadata, true)
        )),
        FormatTreeNode::new(format!("filters: [{filter}]")),
    ];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    append_profile_info(&mut children, profs, plan.plan_id);

    children.push(to_format_tree(&plan.input, metadata, profs)?);

    Ok(FormatTreeNode::with_children(
        "Filter".to_string(),
        children,
    ))
}

fn eval_scalar_to_format_tree(
    plan: &EvalScalar,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    if plan.exprs.is_empty() {
        return to_format_tree(&plan.input, metadata, profs);
    }
    let scalars = plan
        .exprs
        .iter()
        .map(|(expr, _)| expr.as_expr(&BUILTIN_FUNCTIONS).sql_display())
        .collect::<Vec<_>>()
        .join(", ");
    let mut children = vec![
        FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(plan.output_schema()?, metadata, true)
        )),
        FormatTreeNode::new(format!("expressions: [{scalars}]")),
    ];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    append_profile_info(&mut children, profs, plan.plan_id);

    children.push(to_format_tree(&plan.input, metadata, profs)?);

    Ok(FormatTreeNode::with_children(
        "EvalScalar".to_string(),
        children,
    ))
}

fn async_function_to_format_tree(
    plan: &AsyncFunction,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let mut children = vec![FormatTreeNode::new(format!(
        "output columns: [{}]",
        format_output_columns(plan.output_schema()?, metadata, true)
    ))];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    append_profile_info(&mut children, profs, plan.plan_id);

    children.push(to_format_tree(&plan.input, metadata, profs)?);

    Ok(FormatTreeNode::with_children(
        "AsyncFunction".to_string(),
        children,
    ))
}

pub fn pretty_display_agg_desc(desc: &AggregateFunctionDesc, metadata: &Metadata) -> String {
    format!(
        "{}({})",
        desc.sig.name,
        desc.arg_indices
            .iter()
            .map(|&index| { metadata.column(index).name() })
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn aggregate_expand_to_format_tree(
    plan: &AggregateExpand,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let sets = plan
        .grouping_sets
        .sets
        .iter()
        .map(|set| {
            set.iter()
                .map(|&index| metadata.column(index).name())
                .collect::<Vec<_>>()
                .join(", ")
        })
        .map(|s| format!("({})", s))
        .collect::<Vec<_>>()
        .join(", ");

    let mut children = vec![
        FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(plan.output_schema()?, metadata, true)
        )),
        FormatTreeNode::new(format!("grouping sets: [{sets}]")),
    ];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    append_profile_info(&mut children, profs, plan.plan_id);

    children.push(to_format_tree(&plan.input, metadata, profs)?);

    Ok(FormatTreeNode::with_children(
        "AggregateExpand".to_string(),
        children,
    ))
}

fn aggregate_partial_to_format_tree(
    plan: &AggregatePartial,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let group_by = plan
        .group_by
        .iter()
        .map(|&index| metadata.column(index).name())
        .join(", ");
    let agg_funcs = plan
        .agg_funcs
        .iter()
        .map(|agg| pretty_display_agg_desc(agg, metadata))
        .collect::<Vec<_>>()
        .join(", ");

    let mut children = vec![
        FormatTreeNode::new(format!("group by: [{group_by}]")),
        FormatTreeNode::new(format!("aggregate functions: [{agg_funcs}]")),
    ];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    if let Some((_, r)) = &plan.rank_limit {
        children.push(FormatTreeNode::new(format!("rank limit: {r}")));
    }

    append_profile_info(&mut children, profs, plan.plan_id);

    children.push(to_format_tree(&plan.input, metadata, profs)?);

    Ok(FormatTreeNode::with_children(
        "AggregatePartial".to_string(),
        children,
    ))
}

fn aggregate_final_to_format_tree(
    plan: &AggregateFinal,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let group_by = plan
        .group_by
        .iter()
        .map(|&index| {
            let name = metadata.column(index).name();
            Ok(name)
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");

    let agg_funcs = plan
        .agg_funcs
        .iter()
        .map(|agg| pretty_display_agg_desc(agg, metadata))
        .collect::<Vec<_>>()
        .join(", ");

    let mut children = vec![
        FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(plan.output_schema()?, metadata, true)
        )),
        FormatTreeNode::new(format!("group by: [{group_by}]")),
        FormatTreeNode::new(format!("aggregate functions: [{agg_funcs}]")),
    ];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    append_profile_info(&mut children, profs, plan.plan_id);

    children.push(to_format_tree(&plan.input, metadata, profs)?);

    Ok(FormatTreeNode::with_children(
        "AggregateFinal".to_string(),
        children,
    ))
}

fn window_to_format_tree(
    plan: &Window,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let partition_by = plan
        .partition_by
        .iter()
        .map(|&index| {
            let name = metadata.column(index).name();
            Ok(name)
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");

    let order_by = plan
        .order_by
        .iter()
        .map(|v| v.display_name.clone())
        .collect::<Vec<_>>()
        .join(", ");

    let frame = plan.window_frame.to_string();

    let func = match &plan.func {
        WindowFunction::Aggregate(agg) => pretty_display_agg_desc(agg, metadata),
        func => format!("{}", func),
    };

    let mut children = vec![
        FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(plan.output_schema()?, metadata, true)
        )),
        FormatTreeNode::new(format!("aggregate function: [{func}]")),
        FormatTreeNode::new(format!("partition by: [{partition_by}]")),
        FormatTreeNode::new(format!("order by: [{order_by}]")),
        FormatTreeNode::new(format!("frame: [{frame}]")),
    ];

    if let Some(limit) = plan.limit {
        children.push(FormatTreeNode::new(format!("limit: [{limit}]")))
    }

    append_profile_info(&mut children, profs, plan.plan_id);

    children.push(to_format_tree(&plan.input, metadata, profs)?);

    Ok(FormatTreeNode::with_children(
        "Window".to_string(),
        children,
    ))
}

fn sort_to_format_tree(
    plan: &Sort,
    metadata: &Metadata,
    prof_span_set: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let sort_keys = plan
        .order_by
        .iter()
        .map(|sort_key| {
            Ok(format!(
                "{} {} {}",
                sort_key.display_name,
                if sort_key.asc { "ASC" } else { "DESC" },
                if sort_key.nulls_first {
                    "NULLS FIRST"
                } else {
                    "NULLS LAST"
                }
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");

    let mut children = vec![
        FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(plan.output_schema()?, metadata, true)
        )),
        FormatTreeNode::new(format!("sort keys: [{sort_keys}]")),
    ];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    append_profile_info(&mut children, prof_span_set, plan.plan_id);

    children.push(to_format_tree(&plan.input, metadata, prof_span_set)?);

    Ok(FormatTreeNode::with_children("Sort".to_string(), children))
}

fn window_partition_to_format_tree(
    plan: &WindowPartition,
    metadata: &Metadata,
    prof_span_set: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let partition_by = plan
        .partition_by
        .iter()
        .map(|&index| {
            let name = metadata.column(index).name();
            Ok(name)
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");

    let mut children = vec![
        FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(plan.output_schema()?, metadata, true)
        )),
        FormatTreeNode::new(format!("hash keys: [{partition_by}]")),
    ];

    if let Some(top_n) = &plan.top_n {
        children.push(FormatTreeNode::new(format!("top: {}", top_n.top)));
    }

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    append_profile_info(&mut children, prof_span_set, plan.plan_id);

    children.push(to_format_tree(&plan.input, metadata, prof_span_set)?);

    Ok(FormatTreeNode::with_children(
        "WindowPartition".to_string(),
        children,
    ))
}

fn limit_to_format_tree(
    plan: &Limit,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let mut children = vec![
        FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(plan.output_schema()?, metadata, true)
        )),
        FormatTreeNode::new(format!(
            "limit: {}",
            plan.limit
                .map_or("NONE".to_string(), |limit| limit.to_string())
        )),
        FormatTreeNode::new(format!("offset: {}", plan.offset)),
    ];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    append_profile_info(&mut children, profs, plan.plan_id);

    children.push(to_format_tree(&plan.input, metadata, profs)?);

    Ok(FormatTreeNode::with_children("Limit".to_string(), children))
}

fn row_fetch_to_format_tree(
    plan: &RowFetch,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let table_schema = plan.source.source_info.schema();
    let projected_schema = plan.cols_to_fetch.project_schema(&table_schema);
    let fields_to_fetch = projected_schema.fields();

    let mut children = vec![
        FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(plan.output_schema()?, metadata, true)
        )),
        FormatTreeNode::new(format!(
            "columns to fetch: [{}]",
            fields_to_fetch.iter().map(|f| f.name()).join(", ")
        )),
    ];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    append_profile_info(&mut children, profs, plan.plan_id);

    children.push(to_format_tree(&plan.input, metadata, profs)?);

    Ok(FormatTreeNode::with_children(
        "RowFetch".to_string(),
        children,
    ))
}

fn range_join_to_format_tree(
    plan: &RangeJoin,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let range_join_conditions = plan
        .conditions
        .iter()
        .map(|condition| {
            let left = condition
                .left_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .sql_display();
            let right = condition
                .right_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .sql_display();
            format!("{left} {:?} {right}", condition.operator)
        })
        .collect::<Vec<_>>()
        .join(", ");
    let other_conditions = plan
        .other_conditions
        .iter()
        .map(|filter| filter.as_expr(&BUILTIN_FUNCTIONS).sql_display())
        .collect::<Vec<_>>()
        .join(", ");

    let mut left_child = to_format_tree(&plan.left, metadata, profs)?;
    let mut right_child = to_format_tree(&plan.right, metadata, profs)?;

    left_child.payload = format!("{}(Left)", left_child.payload);
    right_child.payload = format!("{}(Right)", right_child.payload);

    let mut children = vec![
        FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(plan.output_schema()?, metadata, true)
        )),
        FormatTreeNode::new(format!("join type: {}", plan.join_type)),
        FormatTreeNode::new(format!("range join conditions: [{range_join_conditions}]")),
        FormatTreeNode::new(format!("other conditions: [{other_conditions}]")),
    ];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    append_profile_info(&mut children, profs, plan.plan_id);

    children.push(left_child);
    children.push(right_child);

    Ok(FormatTreeNode::with_children(
        match plan.range_join_type {
            RangeJoinType::IEJoin => "IEJoin".to_string(),
            RangeJoinType::Merge => "MergeJoin".to_string(),
        },
        children,
    ))
}

fn hash_join_to_format_tree(
    plan: &HashJoin,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let build_keys = plan
        .build_keys
        .iter()
        .map(|scalar| scalar.as_expr(&BUILTIN_FUNCTIONS).sql_display())
        .collect::<Vec<_>>()
        .join(", ");
    let probe_keys = plan
        .probe_keys
        .iter()
        .map(|scalar| scalar.as_expr(&BUILTIN_FUNCTIONS).sql_display())
        .collect::<Vec<_>>()
        .join(", ");
    let is_null_equal = plan.is_null_equal.iter().map(|b| format!("{b}")).join(", ");
    let filters = plan
        .non_equi_conditions
        .iter()
        .map(|filter| filter.as_expr(&BUILTIN_FUNCTIONS).sql_display())
        .collect::<Vec<_>>()
        .join(", ");

    let mut build_child = to_format_tree(&plan.build, metadata, profs)?;
    let mut probe_child = to_format_tree(&plan.probe, metadata, profs)?;

    build_child.payload = format!("{}(Build)", build_child.payload);
    probe_child.payload = format!("{}(Probe)", probe_child.payload);

    let mut children = vec![
        FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(plan.output_schema()?, metadata, true)
        )),
        FormatTreeNode::new(format!("join type: {}", plan.join_type)),
        FormatTreeNode::new(format!("build keys: [{build_keys}]")),
        FormatTreeNode::new(format!("probe keys: [{probe_keys}]")),
        FormatTreeNode::new(format!("keys is null equal: [{is_null_equal}]")),
        FormatTreeNode::new(format!("filters: [{filters}]")),
    ];

    if let Some((cache_index, column_map)) = &plan.build_side_cache_info {
        let mut column_indexes = column_map.keys().collect::<Vec<_>>();
        column_indexes.sort();
        children.push(FormatTreeNode::new(format!("cache index: {}", cache_index)));
        children.push(FormatTreeNode::new(format!(
            "cache columns: {:?}",
            column_indexes
        )));
    }

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    append_profile_info(&mut children, profs, plan.plan_id);

    children.push(build_child);
    children.push(probe_child);

    Ok(FormatTreeNode::with_children(
        "HashJoin".to_string(),
        children,
    ))
}

fn exchange_to_format_tree(
    plan: &Exchange,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    Ok(FormatTreeNode::with_children("Exchange".to_string(), vec![
        FormatTreeNode::new(format!(
            "output columns: [{}]",
            format_output_columns(plan.output_schema()?, metadata, true)
        )),
        FormatTreeNode::new(format!("exchange type: {}", match plan.kind {
            FragmentKind::Init => "Init-Partition".to_string(),
            FragmentKind::Normal => format!(
                "Hash({})",
                plan.keys
                    .iter()
                    .map(|key| { key.as_expr(&BUILTIN_FUNCTIONS).sql_display() })
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            FragmentKind::Expansive => "Broadcast".to_string(),
            FragmentKind::Merge => "Merge".to_string(),
        })),
        to_format_tree(&plan.input, metadata, profs)?,
    ]))
}

fn union_all_to_format_tree(
    plan: &UnionAll,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let mut children = vec![FormatTreeNode::new(format!(
        "output columns: [{}]",
        format_output_columns(plan.output_schema()?, metadata, true)
    ))];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    append_profile_info(&mut children, profs, plan.plan_id);

    children.extend(vec![
        to_format_tree(&plan.left, metadata, profs)?,
        to_format_tree(&plan.right, metadata, profs)?,
    ]);

    let root = if !plan.cte_scan_names.is_empty() {
        "UnionAll(recursive cte)".to_string()
    } else {
        "UnionAll".to_string()
    };

    Ok(FormatTreeNode::with_children(root, children))
}

fn part_stats_info_to_format_tree(info: &PartStatistics) -> Vec<FormatTreeNode<String>> {
    let read_size = format_byte_size(info.read_bytes);
    let mut items = vec![
        FormatTreeNode::new(format!("read rows: {}", info.read_rows)),
        FormatTreeNode::new(format!("read size: {}", read_size)),
        FormatTreeNode::new(format!("partitions total: {}", info.partitions_total)),
        FormatTreeNode::new(format!("partitions scanned: {}", info.partitions_scanned)),
    ];

    // format is like "pruning stats: [segments: <range pruning: x to y>, blocks: <range pruning: x to y>]"
    let mut blocks_pruning_description = String::new();

    // range pruning status.
    if info.pruning_stats.blocks_range_pruning_before > 0 {
        blocks_pruning_description += &format!(
            "range pruning: {} to {}",
            info.pruning_stats.blocks_range_pruning_before,
            info.pruning_stats.blocks_range_pruning_after
        );
    }

    // bloom pruning status.
    if info.pruning_stats.blocks_bloom_pruning_before > 0 {
        if !blocks_pruning_description.is_empty() {
            blocks_pruning_description += ", ";
        }
        blocks_pruning_description += &format!(
            "bloom pruning: {} to {}",
            info.pruning_stats.blocks_bloom_pruning_before,
            info.pruning_stats.blocks_bloom_pruning_after
        );
    }

    // inverted index pruning status.
    if info.pruning_stats.blocks_inverted_index_pruning_before > 0 {
        if !blocks_pruning_description.is_empty() {
            blocks_pruning_description += ", ";
        }
        blocks_pruning_description += &format!(
            "inverted pruning: {} to {}",
            info.pruning_stats.blocks_inverted_index_pruning_before,
            info.pruning_stats.blocks_inverted_index_pruning_after
        );
    }

    // Combine segment pruning and blocks pruning descriptions if any
    if info.pruning_stats.segments_range_pruning_before > 0
        || !blocks_pruning_description.is_empty()
    {
        let mut pruning_description = String::new();

        if info.pruning_stats.segments_range_pruning_before > 0 {
            pruning_description += &format!(
                "segments: <range pruning: {} to {}>",
                info.pruning_stats.segments_range_pruning_before,
                info.pruning_stats.segments_range_pruning_after
            );
        }

        if !blocks_pruning_description.is_empty() {
            if !pruning_description.is_empty() {
                pruning_description += ", ";
            }
            pruning_description += &format!("blocks: <{}>", blocks_pruning_description);
        }

        items.push(FormatTreeNode::new(format!(
            "pruning stats: [{}]",
            pruning_description
        )));
    }

    items
}

fn plan_stats_info_to_format_tree(info: &PlanStatsInfo) -> Vec<FormatTreeNode<String>> {
    vec![FormatTreeNode::new(format!(
        "estimated rows: {0:.2}",
        info.estimated_rows
    ))]
}

fn exchange_source_to_format_tree(
    plan: &ExchangeSource,
    metadata: &Metadata,
) -> Result<FormatTreeNode<String>> {
    let mut children = vec![FormatTreeNode::new(format!(
        "output columns: [{}]",
        format_output_columns(plan.output_schema()?, metadata, true)
    ))];

    children.push(FormatTreeNode::new(format!(
        "source fragment: [{}]",
        plan.source_fragment_id
    )));

    Ok(FormatTreeNode::with_children(
        "ExchangeSource".to_string(),
        children,
    ))
}

fn exchange_sink_to_format_tree(
    plan: &ExchangeSink,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let mut children = vec![FormatTreeNode::new(format!(
        "output columns: [{}]",
        format_output_columns(plan.output_schema()?, metadata, true)
    ))];

    children.push(FormatTreeNode::new(format!(
        "destination fragment: [{}]",
        plan.destination_fragment_id
    )));

    children.push(to_format_tree(&plan.input, metadata, profs)?);

    Ok(FormatTreeNode::with_children(
        "ExchangeSink".to_string(),
        children,
    ))
}

fn distributed_insert_to_format_tree(
    plan: &DistributedInsertSelect,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let children = vec![to_format_tree(&plan.input, metadata, profs)?];

    Ok(FormatTreeNode::with_children(
        "DistributedInsertSelect".to_string(),
        children,
    ))
}

fn commit_sink_to_format_tree(
    plan: &CommitSink,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let children = vec![to_format_tree(&plan.input, metadata, profs)?];
    Ok(FormatTreeNode::with_children(
        "CommitSink".to_string(),
        children,
    ))
}

fn project_set_to_format_tree(
    plan: &ProjectSet,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let mut children = vec![FormatTreeNode::new(format!(
        "output columns: [{}]",
        format_output_columns(plan.output_schema()?, metadata, true)
    ))];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    append_profile_info(&mut children, profs, plan.plan_id);

    children.extend(vec![FormatTreeNode::new(format!(
        "set returning functions: {}",
        plan.srf_exprs
            .iter()
            .map(|(expr, _)| expr.clone().as_expr(&BUILTIN_FUNCTIONS).sql_display())
            .collect::<Vec<_>>()
            .join(", ")
    ))]);

    children.extend(vec![to_format_tree(&plan.input, metadata, profs)?]);

    Ok(FormatTreeNode::with_children(
        "ProjectSet".to_string(),
        children,
    ))
}

fn udf_to_format_tree(
    plan: &Udf,
    metadata: &Metadata,
    profs: &HashMap<u32, PlanProfile>,
) -> Result<FormatTreeNode<String>> {
    let mut children = vec![FormatTreeNode::new(format!(
        "output columns: [{}]",
        format_output_columns(plan.output_schema()?, metadata, true)
    ))];

    if let Some(info) = &plan.stat_info {
        let items = plan_stats_info_to_format_tree(info);
        children.extend(items);
    }

    append_profile_info(&mut children, profs, plan.plan_id);

    children.extend(vec![FormatTreeNode::new(format!(
        "udf functions: {}",
        plan.udf_funcs
            .iter()
            .map(|func| {
                let arg_exprs = func.arg_exprs.join(", ");
                format!("{}({})", func.func_name, arg_exprs)
            })
            .collect::<Vec<_>>()
            .join(", ")
    ))]);

    children.extend(vec![to_format_tree(&plan.input, metadata, profs)?]);

    Ok(FormatTreeNode::with_children("Udf".to_string(), children))
}

fn format_output_columns(
    output_schema: DataSchemaRef,
    metadata: &Metadata,
    format_table: bool,
) -> String {
    output_schema
        .fields()
        .iter()
        .map(|field| match field.name().parse::<usize>() {
            Ok(column_index) => {
                if column_index == usize::MAX {
                    return String::from("dummy value");
                }
                let column_entry = metadata.column(column_index);
                match column_entry.table_index() {
                    Some(table_index) if format_table => match metadata
                        .table(table_index)
                        .alias_name()
                    {
                        Some(alias_name) => {
                            format!("{}.{} (#{})", alias_name, column_entry.name(), column_index)
                        }
                        None => format!(
                            "{}.{} (#{})",
                            metadata.table(table_index).name(),
                            column_entry.name(),
                            column_index,
                        ),
                    },
                    _ => format!("{} (#{})", column_entry.name(), column_index),
                }
            }
            _ => format!("#{}", field.name()),
        })
        .collect::<Vec<_>>()
        .join(", ")
}
