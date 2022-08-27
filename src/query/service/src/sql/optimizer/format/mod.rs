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

use super::group::Group;
use super::MExpr;
use super::Memo;
use crate::sql::plans::RelOperator;

pub fn display_memo(memo: &Memo) -> String {
    memo.groups
        .iter()
        .map(display_group)
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn display_group(group: &Group) -> String {
    format!(
        "Group #{}: [{}]",
        group.group_index,
        group
            .m_exprs
            .iter()
            .map(display_m_expr)
            .collect::<Vec<_>>()
            .join(",\n")
    )
}

pub fn display_m_expr(m_expr: &MExpr) -> String {
    format!(
        "{} [{}]",
        display_rel_op(&m_expr.plan),
        m_expr
            .children
            .iter()
            .map(|child| format!("#{child}"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

pub fn display_rel_op(rel_op: &RelOperator) -> String {
    match rel_op {
        RelOperator::LogicalGet(_) => "LogicalGet".to_string(),
        RelOperator::LogicalInnerJoin(_) => "LogicalInnerJoin".to_string(),
        RelOperator::PhysicalScan(_) => "PhysicalScan".to_string(),
        RelOperator::PhysicalHashJoin(_) => "PhysicalHashJoin".to_string(),
        RelOperator::Project(_) => "Project".to_string(),
        RelOperator::EvalScalar(_) => "EvalScalar".to_string(),
        RelOperator::Filter(_) => "Filter".to_string(),
        RelOperator::Aggregate(_) => "Aggregate".to_string(),
        RelOperator::Sort(_) => "Sort".to_string(),
        RelOperator::Limit(_) => "Limit".to_string(),
        RelOperator::UnionAll(_) => "UnionAll".to_string(),
        RelOperator::Exchange(_) => "Exchange".to_string(),
        RelOperator::Pattern(_) => "Pattern".to_string(),
    }
}
