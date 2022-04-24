// Copyright 2021 Datafuse Labs.
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

use std::fmt::Debug;
use std::fmt::Formatter;

use common_exception::Result;
use common_planners::Expression;

// Intermediate representation for query AST(after normalize)
pub struct QueryASTIR {
    pub filter_predicate: Option<Expression>,
    pub having_predicate: Option<Expression>,
    pub group_by_expressions: Vec<Expression>,
    pub aggregate_expressions: Vec<Expression>,
    pub projection_expressions: Vec<Expression>,
    pub distinct: bool,
    pub order_by_expressions: Vec<Expression>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

pub trait QueryASTIRVisitor<Data> {
    fn visit(ir: &mut QueryASTIR, data: &mut Data) -> Result<()> {
        if let Some(predicate) = &mut ir.filter_predicate {
            Self::visit_filter(predicate, data)?;
        }

        if let Some(predicate) = &mut ir.having_predicate {
            Self::visit_having(predicate, data)?;
        }

        Self::visit_group_by(&mut ir.group_by_expressions, data)?;
        Self::visit_order_by(&mut ir.order_by_expressions, data)?;
        Self::visit_aggregates(&mut ir.aggregate_expressions, data)?;
        Self::visit_projection(&mut ir.projection_expressions, data)?;
        Ok(())
    }

    fn visit_expr(_expr: &mut Expression, _data: &mut Data) -> Result<()> {
        Ok(())
    }

    fn visit_recursive_expr(expr: &mut Expression, data: &mut Data) -> Result<()> {
        match expr {
            Expression::Alias(_, expr) => Self::visit_recursive_expr(expr, data),
            Expression::UnaryExpression { expr, .. } => Self::visit_recursive_expr(expr, data),
            Expression::BinaryExpression { left, right, .. } => {
                Self::visit_recursive_expr(left, data)?;
                Self::visit_recursive_expr(right, data)
            }
            Expression::ScalarFunction { args, .. } => {
                for arg in args {
                    Self::visit_recursive_expr(arg, data)?;
                }

                Ok(())
            }
            Expression::AggregateFunction { args, .. } => {
                for arg in args {
                    Self::visit_recursive_expr(arg, data)?;
                }

                Ok(())
            }
            Expression::Sort {
                expr, origin_expr, ..
            } => {
                Self::visit_recursive_expr(expr, data)?;
                Self::visit_recursive_expr(origin_expr, data)
            }
            Expression::Cast { expr, .. } => Self::visit_recursive_expr(expr, data),
            Expression::MapAccess { args, .. } => {
                for arg in args {
                    Self::visit_recursive_expr(arg, data)?;
                }

                Ok(())
            }
            _ => Self::visit_expr(expr, data),
        }
    }

    fn visit_filter(predicate: &mut Expression, data: &mut Data) -> Result<()> {
        Self::visit_recursive_expr(predicate, data)
    }

    fn visit_having(predicate: &mut Expression, data: &mut Data) -> Result<()> {
        Self::visit_recursive_expr(predicate, data)
    }

    fn visit_group_by(exprs: &mut Vec<Expression>, data: &mut Data) -> Result<()> {
        for expr in exprs {
            Self::visit_recursive_expr(expr, data)?;
        }

        Ok(())
    }

    fn visit_aggregates(exprs: &mut Vec<Expression>, data: &mut Data) -> Result<()> {
        for expr in exprs {
            Self::visit_recursive_expr(expr, data)?;
        }

        Ok(())
    }

    fn visit_order_by(exprs: &mut Vec<Expression>, data: &mut Data) -> Result<()> {
        for expr in exprs {
            Self::visit_recursive_expr(expr, data)?;
        }

        Ok(())
    }

    fn visit_projection(exprs: &mut Vec<Expression>, data: &mut Data) -> Result<()> {
        for expr in exprs {
            Self::visit_recursive_expr(expr, data)?;
        }

        Ok(())
    }
}

impl Debug for QueryASTIR {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        let mut debug_struct = f.debug_struct("NormalQuery");

        if let Some(predicate) = &self.filter_predicate {
            debug_struct.field("filter", predicate);
        }

        if let Some(predicate) = &self.having_predicate {
            debug_struct.field("having", predicate);
        }

        if !self.group_by_expressions.is_empty() {
            debug_struct.field("group by", &self.group_by_expressions);
        }

        if !self.aggregate_expressions.is_empty() {
            debug_struct.field("aggregate", &self.aggregate_expressions);
        }

        if self.distinct {
            debug_struct.field("distinct", &true);
        }

        if !self.order_by_expressions.is_empty() {
            debug_struct.field("order by", &self.order_by_expressions);
        }

        if !self.projection_expressions.is_empty() {
            debug_struct.field("projection", &self.projection_expressions);
        }

        debug_struct.finish()
    }
}
