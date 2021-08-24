// Copyright 2020 Datafuse Labs.
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

use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::catalogs::utils::TableMeta;
use crate::sql::expression::Expression;
use crate::sql::parser::ast::BinaryOperator;
use crate::sql::parser::ast::JoinOperator;
use crate::sql::planner::binder::ColumnBinding;
use crate::sql::planner::binder::IndexType;

#[derive(Debug, Clone)]
pub enum Logical {
    Union(Union),
    EquiJoin(EquiJoin),
    Aggregation(Aggregation),
    Projection(Projection),
    Filter(Filter),
    Get(Get),
}

impl Logical {
    pub fn get_column_bindings(&self) -> Vec<ColumnBinding> {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct Union {}

// EquiJoin represents join operator with equivalence comparison predicate(i.e. =)
#[derive(Debug, Clone)]
pub struct EquiJoin {
    pub op: JoinOperator,
    // Equi-join conditions of left and right side
    // Assert `left_conditions.len() == right_conditions.len()`
    pub left_conditions: Vec<Expression>,
    pub right_conditions: Vec<Expression>,
    pub left_child: Box<Logical>,
    pub right_child: Box<Logical>,
}

impl EquiJoin {
    pub fn new(
        op: JoinOperator,
        conditions: Vec<Expression>,
        left_plan: Logical,
        right_plan: Logical,
    ) -> Result<Self> {
        for cond in conditions.iter() {
            // Check equivalence(=) predicate
            if !cond.is_equal() {
                return Err(ErrorCode::UnsupportedJoinType("Unsupported non-equi join"));
            }
        }

        let left_bindings: HashSet<ColumnBinding> =
            left_plan.get_column_bindings().into_iter().collect();
        let right_bindings: HashSet<ColumnBinding> =
            right_plan.get_column_bindings().into_iter().collect();

        let checked_conditions: Vec<(Expression, Expression)> = conditions
            .into_iter()
            .map(|expr| match expr {
                Expression::BinaryExpression { left, right, op } if op == BinaryOperator::Eq => {
                    Self::check_join_condition(*left, *right, &left_bindings, &right_bindings)
                }
                _ => Err(ErrorCode::UnsupportedJoinType("Unsupported non-equi join")),
            })
            .collect::<Result<_>>()?;

        let (left_conditions, right_conditions) = checked_conditions.into_iter().unzip();

        Ok(EquiJoin {
            op,
            left_conditions,
            right_conditions,
            left_child: Box::new(left_plan),
            right_child: Box::new(right_plan),
        })
    }

    // Check join condition for EquiJoin
    // TODO: we only support both side join now
    pub fn check_join_condition(
        left: Expression,
        right: Expression,
        left_bindings: &HashSet<ColumnBinding>,
        right_bindings: &HashSet<ColumnBinding>,
    ) -> Result<(Expression, Expression)> {
        // Check if condition dependents both side of the join operator
        let left_dependencies = left.get_column_bindings();
        let right_dependencies = right.get_column_bindings();
        if left_dependencies.is_subset(left_bindings)
            && right_dependencies.is_subset(right_bindings)
        {
            Ok((left, right))
        } else if left_dependencies.is_subset(right_bindings)
            && right_dependencies.is_subset(left_bindings)
        {
            Ok((right, left))
        } else {
            Err(ErrorCode::UnsupportedJoinType("Unsupported join condition"))
        }
    }
}

#[derive(Debug, Clone)]
pub struct Aggregation {
    pub group_by: Vec<Expression>,
    pub agg_funcs: Vec<Expression>,

    pub child: Box<Logical>,
}

impl Aggregation {
    pub fn new(group_by: Vec<Expression>, agg_funcs: Vec<Expression>, child: Logical) -> Self {
        Aggregation {
            group_by,
            agg_funcs,
            child: Box::new(child),
        }
    }

    // Check validity of group keys
    pub fn check_group_by(expressions: &Vec<Expression>) -> Result<()> {
        for expr in expressions.iter() {
            if let Expression::Literal { value } = expr {
                // Check if expr is a position literal, like `SELECT a FROM t GROUP BY 1`
                let position = value.as_u64();
                if let Err(_) = position {
                    return Err(ErrorCode::SyntaxException(
                        "non-integer constant in GROUP BY",
                    ));
                }
                continue;
            } else if expr.is_constant() {
                // Check if expr is a constant but not a position literal
                return Err(ErrorCode::SyntaxException(
                    "non-integer constant in GROUP BY",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Projection {
    pub alias: Vec<(Expression, String)>,
    pub child: Box<Logical>,
}

impl Projection {
    pub fn new(alias: Vec<(Expression, String)>, child: Logical) -> Self {
        Projection {
            alias,
            child: Box::new(child),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Filter {
    // Conjunctions
    pub predicates: Vec<Expression>,
    pub child: Box<Logical>,
}

impl Filter {
    pub fn new(predicates: Vec<Expression>, child: Logical) -> Self {
        Filter {
            predicates,
            child: Box::new(child),
        }
    }
}

#[derive(Clone)]
pub struct Get {
    pub table: Arc<TableMeta>,
    pub table_index: IndexType,
    pub table_alias: String,
    pub column_names: Vec<String>,
    pub column_indexes: Vec<IndexType>,
    pub data_types: Vec<DataType>,
}

impl Get {
    pub fn new(
        table: Arc<TableMeta>,
        table_alias: String,
        table_index: IndexType,
        column_names: Vec<String>,
        data_types: Vec<DataType>,
    ) -> Self {
        Get {
            table,
            table_index,
            table_alias,
            column_indexes: column_names
                .iter()
                .enumerate()
                .map(|(idx, _)| idx)
                .collect(),
            column_names,
            data_types,
        }
    }
}

impl Debug for Get {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Get {{ table_alias: {:?}, table_index: {:?}, column_names: {:?}, data_types: {:?} }}",
            &self.table_alias, &self.table_index, &self.column_names, &self.data_types
        )?;
        Ok(())
    }
}
