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

use databend_common_ast::ast::FormatTreeNode;
use databend_common_base::base::format_byte_size;
use databend_common_exception::Result;
use itertools::Itertools;

use crate::optimizer::group::Group;
use crate::optimizer::MExpr;
use crate::optimizer::Memo;
use crate::plans::Exchange;
use crate::plans::RelOperator;

pub fn display_memo(memo: &Memo) -> Result<String> {
    let mem_size = format_byte_size(memo.mem_size());
    let mut children = vec![
        FormatTreeNode::new(format!("root group: #{}", memo.root.unwrap_or(0))),
        FormatTreeNode::new(format!("estimated memory: {}", mem_size)),
    ];

    children.extend(memo.groups.iter().map(group_to_format_tree));

    let root = FormatTreeNode::with_children("Memo".to_string(), children);

    Ok(root.format_pretty()?)
}

pub fn display_rel_op(rel_op: &RelOperator) -> String {
    match rel_op {
        RelOperator::Scan(_) => "Scan".to_string(),
        RelOperator::Join(_) => "Join".to_string(),
        RelOperator::EvalScalar(_) => "EvalScalar".to_string(),
        RelOperator::Filter(_) => "Filter".to_string(),
        RelOperator::Aggregate(_) => "Aggregate".to_string(),
        RelOperator::Sort(_) => "Sort".to_string(),
        RelOperator::Limit(_) => "Limit".to_string(),
        RelOperator::UnionAll(_) => "UnionAll".to_string(),
        RelOperator::Exchange(op) => {
            format!("Exchange: ({})", match op {
                Exchange::Hash(scalars) => format!(
                    "Hash({})",
                    scalars
                        .iter()
                        .map(|s| s.as_raw_expr().to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                ),
                Exchange::Broadcast => "Broadcast".to_string(),
                Exchange::Merge => "Merge".to_string(),
                Exchange::MergeSort => "MergeSort".to_string(),
            })
        }
        RelOperator::DummyTableScan(_) => "DummyTableScan".to_string(),
        RelOperator::ProjectSet(_) => "ProjectSet".to_string(),
        RelOperator::Window(_) => "WindowFunc".to_string(),
        RelOperator::ConstantTableScan(s) => s.name().to_string(),
        RelOperator::ExpressionScan(_) => "ExpressionScan".to_string(),
        RelOperator::CacheScan(_) => "CacheScan".to_string(),
        RelOperator::Udf(_) => "Udf".to_string(),
        RelOperator::RecursiveCteScan(_) => "RecursiveCteScan".to_string(),
        RelOperator::AsyncFunction(_) => "AsyncFunction".to_string(),
        RelOperator::Mutation(_) => "MergeInto".to_string(),
        RelOperator::MutationSource(_) => "MutationSource".to_string(),
        RelOperator::CompactBlock(_) => "CompactBlock".to_string(),
    }
}

fn group_to_format_tree(group: &Group) -> FormatTreeNode<String> {
    FormatTreeNode::with_children(
        format!("Group #{}", group.group_index),
        [
            vec![FormatTreeNode::with_children(
                "Best properties".to_string(),
                group
                    .best_props
                    .iter()
                    .map(|(prop, ccx)| {
                        format!(
                            "{}: expr: #{}, cost: {}, children: [{}]",
                            prop,
                            ccx.expr_index,
                            ccx.cost,
                            ccx.children_required_props
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join(", "),
                        )
                    })
                    .sorted()
                    .map(FormatTreeNode::new)
                    .collect::<Vec<_>>(),
            )],
            group.m_exprs.iter().map(m_expr_to_format_tree).collect(),
        ]
        .concat(),
    )
}

fn m_expr_to_format_tree(m_expr: &MExpr) -> FormatTreeNode<String> {
    FormatTreeNode::new(format!(
        "#{} {} [{}]",
        m_expr.index,
        display_rel_op(&m_expr.plan),
        m_expr
            .children
            .iter()
            .map(|child| format!("#{child}"))
            .collect::<Vec<_>>()
            .join(", ")
    ))
}
