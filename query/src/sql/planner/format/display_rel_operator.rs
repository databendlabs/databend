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

use std::fmt::Display;

use common_datavalues::format_data_type_sql;
use itertools::Itertools;

use super::FormatTreeNode;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::AggregatePlan;
use crate::sql::plans::AndExpr;
use crate::sql::plans::ComparisonExpr;
use crate::sql::plans::ComparisonOp;
use crate::sql::plans::CrossApply;
use crate::sql::plans::EvalScalar;
use crate::sql::plans::Filter;
use crate::sql::plans::LimitPlan;
use crate::sql::plans::LogicalGet;
use crate::sql::plans::LogicalInnerJoin;
use crate::sql::plans::PhysicalHashJoin;
use crate::sql::plans::PhysicalScan;
use crate::sql::plans::Project;
use crate::sql::plans::RelOperator;
use crate::sql::plans::Scalar;
use crate::sql::plans::SortPlan;
use crate::sql::MetadataRef;

pub struct FormatContext {
    metadata: MetadataRef,
    rel_operator: RelOperator,
}

impl SExpr {
    pub fn to_format_tree(&self, metadata: &MetadataRef) -> FormatTreeNode<FormatContext> {
        let children: Vec<FormatTreeNode<FormatContext>> = self
            .children()
            .iter()
            .map(|child| child.to_format_tree(metadata))
            .collect();

        FormatTreeNode::with_children(
            FormatContext {
                metadata: metadata.clone(),
                rel_operator: self.plan().clone(),
            },
            children,
        )
    }
}

impl Display for FormatContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.rel_operator {
            RelOperator::LogicalGet(op) => format_logical_get(f, &self.metadata, op),
            RelOperator::LogicalInnerJoin(op) => format_logical_inner_join(f, &self.metadata, op),
            RelOperator::PhysicalScan(op) => format_physical_scan(f, &self.metadata, op),
            RelOperator::PhysicalHashJoin(op) => format_hash_join(f, &self.metadata, op),
            RelOperator::Project(op) => format_project(f, &self.metadata, op),
            RelOperator::EvalScalar(op) => format_eval_scalar(f, &self.metadata, op),
            RelOperator::Filter(op) => format_filter(f, &self.metadata, op),
            RelOperator::Aggregate(op) => format_aggregate(f, &self.metadata, op),
            RelOperator::Sort(op) => format_sort(f, &self.metadata, op),
            RelOperator::Limit(op) => format_limit(f, &self.metadata, op),
            RelOperator::CrossApply(op) => format_cross_apply(f, &self.metadata, op),
            RelOperator::Max1Row(_) => write!(f, "Max1Row"),
            RelOperator::Pattern(_) => write!(f, "Pattern"),
        }
    }
}

pub fn format_scalar(metadata: &MetadataRef, scalar: &Scalar) -> String {
    match scalar {
        Scalar::BoundColumnRef(column_ref) => column_ref.column.column_name.clone(),
        Scalar::ConstantExpr(constant) => constant.value.to_string(),
        Scalar::AndExpr(and) => format!(
            "{} AND {}",
            format_scalar(metadata, &and.left),
            format_scalar(metadata, &and.right)
        ),
        Scalar::OrExpr(or) => format!(
            "{} OR {}",
            format_scalar(metadata, &or.left),
            format_scalar(metadata, &or.right)
        ),
        Scalar::ComparisonExpr(comp) => format!(
            "{} {} {}",
            format_scalar(metadata, &comp.left),
            comp.op.to_func_name(),
            format_scalar(metadata, &comp.right)
        ),
        Scalar::AggregateFunction(agg) => agg.display_name.clone(),
        Scalar::FunctionCall(func) => {
            format!(
                "{}({})",
                &func.func_name,
                func.arguments
                    .iter()
                    .map(|arg| { format_scalar(metadata, arg) })
                    .collect::<Vec<String>>()
                    .join(", ")
            )
        }
        Scalar::Cast(cast) => {
            format!(
                "CAST({} AS {})",
                format_scalar(metadata, &cast.argument),
                format_data_type_sql(&cast.target_type)
            )
        }
        Scalar::SubqueryExpr(_) => "SUBQUERY".to_string(),
    }
}

pub fn format_logical_get(
    f: &mut std::fmt::Formatter<'_>,
    metadata: &MetadataRef,
    op: &LogicalGet,
) -> std::fmt::Result {
    let table = metadata.read().table(op.table_index).clone();
    write!(
        f,
        "LogicalGet: {}.{}.{}",
        &table.catalog, &table.database, &table.name
    )
}

pub fn format_logical_inner_join(
    f: &mut std::fmt::Formatter<'_>,
    metadata: &MetadataRef,
    op: &LogicalInnerJoin,
) -> std::fmt::Result {
    let preds: Vec<Scalar> = op
        .left_conditions
        .iter()
        .zip(op.right_conditions.iter())
        .map(|(left, right)| {
            ComparisonExpr {
                op: ComparisonOp::Equal,
                left: Box::new(left.clone()),
                right: Box::new(right.clone()),
            }
            .into()
        })
        .collect();
    let pred: Scalar = preds.iter().fold(preds[0].clone(), |prev, next| {
        Scalar::AndExpr(AndExpr {
            left: Box::new(prev),
            right: Box::new(next.clone()),
        })
    });
    write!(f, "LogicalInnerJoin: {}", format_scalar(metadata, &pred))
}

pub fn format_hash_join(
    f: &mut std::fmt::Formatter<'_>,
    metadata: &MetadataRef,
    op: &PhysicalHashJoin,
) -> std::fmt::Result {
    let build_keys = op
        .build_keys
        .iter()
        .map(|scalar| format_scalar(metadata, scalar))
        .collect::<Vec<String>>()
        .join(", ");
    let probe_keys = op
        .probe_keys
        .iter()
        .map(|scalar| format_scalar(metadata, scalar))
        .collect::<Vec<String>>()
        .join(", ");
    write!(
        f,
        "PhysicalHashJoin: build keys: [{}], probe keys: [{}]",
        build_keys, probe_keys
    )
}

pub fn format_physical_scan(
    f: &mut std::fmt::Formatter<'_>,
    metadata: &MetadataRef,
    op: &PhysicalScan,
) -> std::fmt::Result {
    let table = metadata.read().table(op.table_index).clone();
    write!(
        f,
        "PhysicalScan: {}.{}.{}",
        &table.catalog, &table.database, &table.name
    )
}

pub fn format_project(
    f: &mut std::fmt::Formatter<'_>,
    metadata: &MetadataRef,
    op: &Project,
) -> std::fmt::Result {
    let column_names = metadata
        .read()
        .columns()
        .iter()
        .map(|entry| entry.name.clone())
        .collect::<Vec<String>>();
    // Sorted by column index to make display of Project stable
    let project_columns = op
        .columns
        .iter()
        .sorted()
        .map(|idx| column_names[*idx].clone())
        .collect::<Vec<String>>()
        .join(",");
    write!(f, "Project: [{}]", project_columns)
}

pub fn format_eval_scalar(
    f: &mut std::fmt::Formatter<'_>,
    metadata: &MetadataRef,
    op: &EvalScalar,
) -> std::fmt::Result {
    let scalars = op
        .items
        .iter()
        .map(|item| format_scalar(metadata, &item.scalar))
        .collect::<Vec<String>>()
        .join(", ");
    write!(f, "EvalScalar: [{}]", scalars)
}

pub fn format_filter(
    f: &mut std::fmt::Formatter<'_>,
    metadata: &MetadataRef,
    op: &Filter,
) -> std::fmt::Result {
    let scalars = op
        .predicates
        .iter()
        .map(|scalar| format_scalar(metadata, scalar))
        .collect::<Vec<String>>()
        .join(", ");
    write!(f, "Filter: [{}]", scalars)
}

pub fn format_aggregate(
    f: &mut std::fmt::Formatter<'_>,
    metadata: &MetadataRef,
    op: &AggregatePlan,
) -> std::fmt::Result {
    let group_items = op
        .group_items
        .iter()
        .map(|item| format_scalar(metadata, &item.scalar))
        .collect::<Vec<String>>()
        .join(", ");
    let agg_funcs = op
        .aggregate_functions
        .iter()
        .map(|item| format_scalar(metadata, &item.scalar))
        .collect::<Vec<String>>()
        .join(", ");
    write!(
        f,
        "Aggregate: group items: [{}], aggregate functions: [{}]",
        group_items, agg_funcs
    )
}

pub fn format_sort(
    f: &mut std::fmt::Formatter<'_>,
    metadata: &MetadataRef,
    op: &SortPlan,
) -> std::fmt::Result {
    let scalars = op
        .items
        .iter()
        .map(|item| {
            let name = metadata.read().column(item.index).name.clone();
            format!(
                "{} {}",
                name,
                if item.asc.unwrap_or(false) {
                    "ASC"
                } else {
                    "DESC"
                }
            )
        })
        .collect::<Vec<String>>()
        .join(", ");
    write!(f, "Sort: [{}]", scalars)
}

pub fn format_limit(
    f: &mut std::fmt::Formatter<'_>,
    _metadata: &MetadataRef,
    _op: &LimitPlan,
) -> std::fmt::Result {
    write!(f, "Limit")
}

pub fn format_cross_apply(
    f: &mut std::fmt::Formatter<'_>,
    _metadata: &MetadataRef,
    _op: &CrossApply,
) -> std::fmt::Result {
    write!(f, "CrossApply")
}
