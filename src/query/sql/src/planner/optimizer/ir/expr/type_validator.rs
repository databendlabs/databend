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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::AggregateFunctionFactory;

use super::SExpr;
use super::SExprVisitor;
use super::VisitAction;
use crate::MetadataRef;
use crate::Symbol;
use crate::plans::AggregateFunction;
use crate::plans::BoundColumnRef;
use crate::plans::Operator;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::Visitor as ScalarExprVisitor;
use crate::plans::WindowFuncType;

impl SExpr {
    /// Validate embedded type declarations against the types that can be inferred
    /// from scalar expressions and aggregate function signatures.
    ///
    /// Nullability is intentionally ignored when comparing a symbol with metadata:
    /// outer joins may add a nullable wrapper without changing the global symbol.
    pub fn validate_types(&self, metadata: &MetadataRef) -> Result<()> {
        self.accept(&mut SExprTypeValidator { metadata })
            .map(|_| ())
    }
}

struct SExprTypeValidator<'a> {
    metadata: &'a MetadataRef,
}

impl SExprVisitor for SExprTypeValidator<'_> {
    fn visit(&mut self, s_expr: &SExpr) -> Result<VisitAction> {
        for scalar in s_expr.plan().scalar_expr_iter() {
            // Resolve the complete expression first, then validate all embedded
            // symbol and aggregate declarations.
            scalar.data_type()?;
            ScalarTypeValidator {
                metadata: self.metadata,
            }
            .visit(scalar)?;
        }

        match s_expr.plan() {
            RelOperator::EvalScalar(eval) => self.validate_items(&eval.items)?,
            RelOperator::Aggregate(aggregate) => {
                self.validate_items(&aggregate.group_items)?;
                self.validate_items(&aggregate.aggregate_functions)?;
            }
            RelOperator::ProjectSet(project_set) => self.validate_items(&project_set.srfs)?,
            RelOperator::AsyncFunction(async_function) => {
                self.validate_items(&async_function.items)?;
            }
            RelOperator::Udf(udf) => self.validate_items(&udf.items)?,
            RelOperator::Window(window) => {
                self.validate_symbol_type(window.index, &window.function.return_type(), "window")?;
                self.validate_window_function(&window.function)?;
            }
            RelOperator::WindowGroup(group) => {
                self.validate_items(&group.scalar_items)?;
                for window in &group.windows {
                    self.validate_symbol_type(
                        window.index,
                        &window.function.return_type(),
                        "window",
                    )?;
                    self.validate_window_function(&window.function)?;
                }
            }
            RelOperator::UnionAll(union) => {
                if union.left_outputs.len() != union.right_outputs.len()
                    || union.left_outputs.len() != union.output_indexes.len()
                {
                    return Err(ErrorCode::Internal(format!(
                        "SExpr union output length mismatch: left {}, right {}, output {}",
                        union.left_outputs.len(),
                        union.right_outputs.len(),
                        union.output_indexes.len()
                    )));
                }
                for (((left, left_cast), (right, right_cast)), output) in union
                    .left_outputs
                    .iter()
                    .zip(&union.right_outputs)
                    .zip(&union.output_indexes)
                {
                    let left_type = match left_cast {
                        Some(cast) => cast.data_type()?,
                        None => self.metadata_type(*left)?,
                    };
                    let right_type = match right_cast {
                        Some(cast) => cast.data_type()?,
                        None => self.metadata_type(*right)?,
                    };
                    self.validate_symbol_type(*output, &left_type, "union left output")?;
                    self.validate_symbol_type(*output, &right_type, "union right output")?;
                }
            }
            _ => {}
        }

        Ok(VisitAction::Continue)
    }
}

impl SExprTypeValidator<'_> {
    fn validate_items(&self, items: &[ScalarItem]) -> Result<()> {
        for item in items {
            self.validate_symbol_type(item.index, &item.scalar.data_type()?, "scalar producer")?;
        }
        Ok(())
    }

    fn validate_symbol_type(&self, index: Symbol, actual: &DataType, source: &str) -> Result<()> {
        let metadata_type = self.metadata_type(index)?;
        if metadata_type.remove_nullable() != actual.remove_nullable() {
            return Err(ErrorCode::Internal(format!(
                "SExpr type mismatch for {source} {index}: metadata declares {metadata_type:?}, expression declares {actual:?}"
            )));
        }
        Ok(())
    }

    fn metadata_type(&self, index: Symbol) -> Result<DataType> {
        let metadata = self.metadata.read();
        metadata
            .columns()
            .get(index.as_usize())
            .map(|column| column.data_type())
            .ok_or_else(|| {
                ErrorCode::Internal(format!("SExpr references unknown metadata symbol {index}"))
            })
    }

    fn validate_window_function(&self, function: &WindowFuncType) -> Result<()> {
        if let WindowFuncType::Aggregate(aggregate) = function {
            ScalarTypeValidator {
                metadata: self.metadata,
            }
            .validate_aggregate_function(aggregate)?;
        }
        Ok(())
    }
}

struct ScalarTypeValidator<'a> {
    metadata: &'a MetadataRef,
}

impl ScalarTypeValidator<'_> {
    fn validate_aggregate_function(&mut self, aggregate: &AggregateFunction) -> Result<()> {
        let argument_types = aggregate
            .args
            .iter()
            .map(ScalarExpr::data_type)
            .collect::<Result<Vec<_>>>()?;
        let sort_descs = aggregate
            .sort_descs
            .iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;
        let inferred = AggregateFunctionFactory::instance()
            .get(
                &aggregate.func_name,
                aggregate.params.clone(),
                argument_types,
                sort_descs,
            )?
            .return_type()?;
        if inferred != *aggregate.return_type {
            return Err(ErrorCode::Internal(format!(
                "SExpr aggregate return type mismatch for {}: stored {:?}, inferred {inferred:?}",
                aggregate.display_name, aggregate.return_type
            )));
        }
        Ok(())
    }
}

impl ScalarExprVisitor<'_> for ScalarTypeValidator<'_> {
    fn visit_bound_column_ref(&mut self, column: &BoundColumnRef) -> Result<()> {
        SExprTypeValidator {
            metadata: self.metadata,
        }
        .validate_symbol_type(
            column.column.index,
            &column.column.data_type,
            "bound column",
        )
    }

    fn visit_aggregate_function(&mut self, aggregate: &AggregateFunction) -> Result<()> {
        self.validate_aggregate_function(aggregate)?;

        for expr in aggregate.exprs() {
            self.visit(expr)?;
        }
        Ok(())
    }
}
