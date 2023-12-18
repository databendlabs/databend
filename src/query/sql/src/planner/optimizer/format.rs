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
use databend_common_exception::Result;

use super::cost::CostContext;
use crate::optimizer::group::Group;
use crate::optimizer::MExpr;
use crate::optimizer::Memo;
use crate::plans::RelOperator;
use crate::IndexType;

pub fn display_memo(memo: &Memo, cost_map: &HashMap<IndexType, CostContext>) -> Result<String> {
    Ok(memo
        .groups
        .iter()
        .map(|grp| group_to_format_tree(grp, cost_map.get(&grp.group_index)).format_pretty())
        .collect::<Result<Vec<_>>>()?
        .join("\n"))
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
        RelOperator::Exchange(_) => "Exchange".to_string(),
        RelOperator::Pattern(_) => "Pattern".to_string(),
        RelOperator::DummyTableScan(_) => "DummyTableScan".to_string(),
        RelOperator::ProjectSet(_) => "ProjectSet".to_string(),
        RelOperator::Window(_) => "WindowFunc".to_string(),
        RelOperator::CteScan(_) => "CteScan".to_string(),
        RelOperator::MaterializedCte(_) => "MaterializedCte".to_string(),
        RelOperator::ConstantTableScan(_) => "ConstantTableScan".to_string(),
        RelOperator::AddRowNumber(_) => "AddRowNumber".to_string(),
        RelOperator::Udf(_) => "Udf".to_string(),
    }
}

fn group_to_format_tree(
    group: &Group,
    cost_context: Option<&CostContext>,
) -> FormatTreeNode<String> {
    FormatTreeNode::with_children(
        format!("Group #{}", group.group_index),
        [
            if let Some(cost_context) = cost_context {
                vec![FormatTreeNode::new(format!(
                    "best cost: [#{}] {}",
                    cost_context.expr_index, cost_context.cost
                ))]
            } else {
                vec![]
            },
            group.m_exprs.iter().map(m_expr_to_format_tree).collect(),
        ]
        .concat(),
    )
}

fn m_expr_to_format_tree(m_expr: &MExpr) -> FormatTreeNode<String> {
    FormatTreeNode::new(format!(
        "{} [{}]",
        display_rel_op(&m_expr.plan),
        m_expr
            .children
            .iter()
            .map(|child| format!("#{child}"))
            .collect::<Vec<_>>()
            .join(", ")
    ))
}
