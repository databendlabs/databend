// Copyright 2022 Datafuse Labs.
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

use common_ast::ast::FormatTreeNode;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::StageKind;
use itertools::Itertools;

use super::AggregateFinal;
use super::AggregatePartial;
use super::EvalScalar;
use super::Exchange;
use super::Filter;
use super::HashJoin;
use super::Limit;
use super::PhysicalPlan;
use super::Project;
use super::Sort;
use super::TableScan;
use super::UnionAll;
use crate::sql::IndexType;
use crate::sql::MetadataRef;

impl PhysicalPlan {
    pub fn format(&self, metadata: MetadataRef) -> Result<String> {
        to_format_tree(self, &metadata)?.format_pretty()
    }
}

fn to_format_tree(plan: &PhysicalPlan, metadata: &MetadataRef) -> Result<FormatTreeNode<String>> {
    match plan {
        PhysicalPlan::TableScan(plan) => table_scan_to_format_tree(plan, metadata),
        PhysicalPlan::Filter(plan) => filter_to_format_tree(plan, metadata),
        PhysicalPlan::Project(plan) => project_to_format_tree(plan, metadata),
        PhysicalPlan::EvalScalar(plan) => eval_scalar_to_format_tree(plan, metadata),
        PhysicalPlan::AggregatePartial(plan) => aggregate_partial_to_format_tree(plan, metadata),
        PhysicalPlan::AggregateFinal(plan) => aggregate_final_to_format_tree(plan, metadata),
        PhysicalPlan::Sort(plan) => sort_to_format_tree(plan, metadata),
        PhysicalPlan::Limit(plan) => limit_to_format_tree(plan, metadata),
        PhysicalPlan::HashJoin(plan) => hash_join_to_format_tree(plan, metadata),
        PhysicalPlan::Exchange(plan) => exchange_to_format_tree(plan, metadata),
        PhysicalPlan::UnionAll(plan) => union_all_to_format_tree(plan, metadata),
        PhysicalPlan::ExchangeSource(_) | PhysicalPlan::ExchangeSink(_) => {
            Err(ErrorCode::LogicalError("Invalid physical plan"))
        }
    }
}

fn table_scan_to_format_tree(
    plan: &TableScan,
    metadata: &MetadataRef,
) -> Result<FormatTreeNode<String>> {
    let table = metadata.read().table(plan.table_index).clone();
    let table_name = format!("{}.{}.{}", table.catalog, table.database, table.name);
    let filters = plan
        .source
        .push_downs
        .as_ref()
        .map_or("".to_string(), |extras| {
            extras
                .filters
                .iter()
                .map(|f| f.column_name())
                .collect::<Vec<_>>()
                .join(", ")
        });

    let limit = plan
        .source
        .push_downs
        .as_ref()
        .map_or("NONE".to_string(), |extras| {
            extras
                .limit
                .map_or("NONE".to_string(), |limit| limit.to_string())
        });

    Ok(FormatTreeNode::with_children(
        "TableScan".to_string(),
        vec![
            FormatTreeNode::new(format!("table: {table_name}")),
            FormatTreeNode::new(format!("read rows: {}", plan.source.statistics.read_rows)),
            FormatTreeNode::new(format!("read bytes: {}", plan.source.statistics.read_bytes)),
            FormatTreeNode::new(format!(
                "partitions total: {}",
                plan.source.statistics.partitions_total
            )),
            FormatTreeNode::new(format!(
                "partitions scanned: {}",
                plan.source.statistics.partitions_scanned
            )),
            FormatTreeNode::new(format!(
                "push downs: [filters: [{filters}], limit: {limit}]"
            )),
        ],
    ))
}

fn filter_to_format_tree(plan: &Filter, metadata: &MetadataRef) -> Result<FormatTreeNode<String>> {
    let filter = plan
        .predicates
        .iter()
        .map(|scalar| scalar.pretty_display(metadata))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    Ok(FormatTreeNode::with_children("Filter".to_string(), vec![
        FormatTreeNode::new(format!("filters: [{filter}]")),
        to_format_tree(&plan.input, metadata)?,
    ]))
}

fn project_to_format_tree(
    plan: &Project,
    metadata: &MetadataRef,
) -> Result<FormatTreeNode<String>> {
    let columns = plan
        .columns
        .iter()
        .sorted()
        .map(|column| format!("{} (#{})", metadata.read().column(*column).name, column))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(FormatTreeNode::with_children("Project".to_string(), vec![
        FormatTreeNode::new(format!("columns: [{columns}]")),
        to_format_tree(&plan.input, metadata)?,
    ]))
}

fn eval_scalar_to_format_tree(
    plan: &EvalScalar,
    metadata: &MetadataRef,
) -> Result<FormatTreeNode<String>> {
    let scalars = plan
        .scalars
        .iter()
        .map(|(scalar, _)| scalar.pretty_display(metadata))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    Ok(FormatTreeNode::with_children(
        "EvalScalar".to_string(),
        vec![
            FormatTreeNode::new(format!("expressions: [{scalars}]")),
            to_format_tree(&plan.input, metadata)?,
        ],
    ))
}

fn aggregate_partial_to_format_tree(
    plan: &AggregatePartial,
    metadata: &MetadataRef,
) -> Result<FormatTreeNode<String>> {
    let group_by = plan
        .group_by
        .iter()
        .map(|column| {
            let index = column.parse::<IndexType>()?;
            let column = metadata.read().column(index).clone();
            Ok(column.name)
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");

    let agg_funcs = plan
        .agg_funcs
        .iter()
        .map(|agg| agg.pretty_display(metadata))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    Ok(FormatTreeNode::with_children(
        "AggregatePartial".to_string(),
        vec![
            FormatTreeNode::new(format!("group by: [{group_by}]")),
            FormatTreeNode::new(format!("aggregate functions: [{agg_funcs}]")),
            to_format_tree(&plan.input, metadata)?,
        ],
    ))
}

fn aggregate_final_to_format_tree(
    plan: &AggregateFinal,
    metadata: &MetadataRef,
) -> Result<FormatTreeNode<String>> {
    let group_by = plan
        .group_by
        .iter()
        .map(|column| {
            let index = column.parse::<IndexType>()?;
            let column = metadata.read().column(index).clone();
            Ok(column.name)
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");

    let agg_funcs = plan
        .agg_funcs
        .iter()
        .map(|agg| agg.pretty_display(metadata))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    Ok(FormatTreeNode::with_children(
        "AggregateFinal".to_string(),
        vec![
            FormatTreeNode::new(format!("group by: [{group_by}]")),
            FormatTreeNode::new(format!("aggregate functions: [{agg_funcs}]")),
            to_format_tree(&plan.input, metadata)?,
        ],
    ))
}

fn sort_to_format_tree(plan: &Sort, metadata: &MetadataRef) -> Result<FormatTreeNode<String>> {
    let sort_keys = plan
        .order_by
        .iter()
        .map(|sort_key| {
            let index = sort_key.order_by.parse::<IndexType>()?;
            let column = metadata.read().column(index).clone();
            Ok(format!(
                "{} {} {}",
                column.name,
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
    Ok(FormatTreeNode::with_children("Sort".to_string(), vec![
        FormatTreeNode::new(format!("sort keys: [{sort_keys}]")),
        to_format_tree(&plan.input, metadata)?,
    ]))
}

fn limit_to_format_tree(plan: &Limit, metadata: &MetadataRef) -> Result<FormatTreeNode<String>> {
    Ok(FormatTreeNode::with_children("Limit".to_string(), vec![
        FormatTreeNode::new(format!(
            "limit: {}",
            plan.limit
                .map_or("NONE".to_string(), |limit| limit.to_string())
        )),
        FormatTreeNode::new(format!("offset: {}", plan.offset)),
        to_format_tree(&plan.input, metadata)?,
    ]))
}

fn hash_join_to_format_tree(
    plan: &HashJoin,
    metadata: &MetadataRef,
) -> Result<FormatTreeNode<String>> {
    let build_keys = plan
        .build_keys
        .iter()
        .map(|scalar| scalar.pretty_display(metadata))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let probe_keys = plan
        .probe_keys
        .iter()
        .map(|scalar| scalar.pretty_display(metadata))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let filters = plan
        .other_conditions
        .iter()
        .map(|filter| filter.pretty_display(metadata))
        .collect::<Result<Vec<_>>>()?
        .join(", ");

    let mut build_child = to_format_tree(&plan.build, metadata)?;
    let mut probe_child = to_format_tree(&plan.probe, metadata)?;

    build_child.payload = format!("{}(Build)", build_child.payload);
    probe_child.payload = format!("{}(Probe)", probe_child.payload);

    Ok(FormatTreeNode::with_children("HashJoin".to_string(), vec![
        FormatTreeNode::new(format!("join type: {}", plan.join_type)),
        FormatTreeNode::new(format!("build keys: [{build_keys}]")),
        FormatTreeNode::new(format!("probe keys: [{probe_keys}]")),
        FormatTreeNode::new(format!("filters: [{filters}]")),
        build_child,
        probe_child,
    ]))
}

fn exchange_to_format_tree(
    plan: &Exchange,
    metadata: &MetadataRef,
) -> Result<FormatTreeNode<String>> {
    Ok(FormatTreeNode::with_children("Exchange".to_string(), vec![
        FormatTreeNode::new(format!("exchange type: {}", match plan.kind {
            StageKind::Normal => format!(
                "Hash({})",
                plan.keys
                    .iter()
                    .map(|scalar| { scalar.pretty_display(metadata) })
                    .collect::<Result<Vec<_>>>()?
                    .join(", ")
            ),
            StageKind::Expansive => "Broadcast".to_string(),
            StageKind::Merge => "Merge".to_string(),
        })),
        to_format_tree(&plan.input, metadata)?,
    ]))
}

fn union_all_to_format_tree(
    plan: &UnionAll,
    metadata: &MetadataRef,
) -> Result<FormatTreeNode<String>> {
    Ok(FormatTreeNode::with_children("UnionAll".to_string(), vec![
        to_format_tree(&plan.left, metadata)?,
        to_format_tree(&plan.right, metadata)?,
    ]))
}
