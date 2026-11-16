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

use std::borrow::Cow;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::aggregate::aggregate_function::RawAggregateCall;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::AGGR_REGISTRY;

use super::Logical;
use super::PExpr;
use super::Physical;
use super::SExpr;
use super::node::Expr;
use super::node::ExprKind;
use super::visitor::ExprVisitor;
use super::visitor::VisitAction;
use crate::MetadataRef;
use crate::Symbol;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::BoundColumnRef;
use crate::plans::FunctionCall;
use crate::plans::LambdaFunc;
use crate::plans::Operator;
use crate::plans::PhysicalRelOperator;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::UnionAll;
use crate::plans::Visitor as ScalarExprVisitor;
use crate::plans::Window;
use crate::plans::WindowFuncType;
use crate::plans::WindowGroup;

impl<K: ExprKind> Expr<K> {
    /// Validate embedded type declarations against the types that can be inferred
    /// from scalar expressions and aggregate function signatures.
    ///
    /// Nullability is intentionally ignored when comparing a symbol with metadata:
    /// outer joins may add a nullable wrapper without changing the global symbol.
    pub fn validate_types(&self, metadata: &MetadataRef) -> Result<()> {
        K::validate_types(self, metadata)
    }
}

pub(super) fn validate_logical_types(expr: &SExpr, metadata: &MetadataRef) -> Result<()> {
    expr.accept(&mut ExprTypeValidator {
        metadata,
        name: Logical::NAME,
    })
    .map(|_| ())
}

pub(super) fn validate_physical_types(expr: &PExpr, metadata: &MetadataRef) -> Result<()> {
    expr.accept(&mut ExprTypeValidator {
        metadata,
        name: Physical::NAME,
    })
    .map(|_| ())
}

struct ExprTypeValidator<'a> {
    name: &'static str,
    metadata: &'a MetadataRef,
}

impl ExprVisitor<Logical> for ExprTypeValidator<'_> {
    fn visit(&mut self, expr: &SExpr) -> Result<VisitAction<Logical>> {
        let plan = expr.plan();
        if let RelOperator::Scan(scan) = plan {
            self.validate_scan_scalars(scan)?;
        } else {
            self.validate_operator_scalars(plan)?;
        }
        match plan {
            RelOperator::EvalScalar(eval) => self.validate_items(&eval.items)?,
            RelOperator::Aggregate(aggregate) => self.validate_aggregate(aggregate)?,
            RelOperator::ProjectSet(project_set) => self.validate_items(&project_set.srfs)?,
            RelOperator::AsyncFunction(function) => self.validate_items(&function.items)?,
            RelOperator::Udf(udf) => self.validate_items(&udf.items)?,
            RelOperator::Window(window) => self.validate_window(window)?,
            RelOperator::WindowGroup(group) => self.validate_window_group(group)?,
            RelOperator::UnionAll(union) => self.validate_union(union)?,
            RelOperator::Scan(_)
            | RelOperator::Join(_)
            | RelOperator::Filter(_)
            | RelOperator::Sort(_)
            | RelOperator::Limit(_)
            | RelOperator::TopN(_)
            | RelOperator::DummyTableScan(_)
            | RelOperator::ConstantTableScan(_)
            | RelOperator::ExpressionScan(_)
            | RelOperator::CacheScan(_)
            | RelOperator::RecursiveCteScan(_)
            | RelOperator::Mutation(_)
            | RelOperator::CompactBlock(_)
            | RelOperator::MutationSource(_)
            | RelOperator::MaterializedCTE(_)
            | RelOperator::MaterializedCTERef(_)
            | RelOperator::Sequence(_) => {}
        }
        Ok(VisitAction::Continue)
    }
}

impl ExprVisitor<Physical> for ExprTypeValidator<'_> {
    fn visit(&mut self, expr: &PExpr) -> Result<VisitAction<Physical>> {
        let plan = expr.plan();
        if let PhysicalRelOperator::Scan(scan) = plan {
            self.validate_scan_scalars(scan)?;
        } else {
            self.validate_operator_scalars(plan)?;
        }
        match plan {
            PhysicalRelOperator::EvalScalar(eval) => self.validate_items(&eval.items)?,
            PhysicalRelOperator::Aggregate(aggregate) => {
                self.validate_aggregate(&aggregate.aggregate)?
            }
            PhysicalRelOperator::ProjectSet(project_set) => {
                self.validate_items(&project_set.srfs)?
            }
            PhysicalRelOperator::AsyncFunction(function) => self.validate_items(&function.items)?,
            PhysicalRelOperator::Udf(udf) => self.validate_items(&udf.items)?,
            PhysicalRelOperator::Window(window) => self.validate_window(window)?,
            PhysicalRelOperator::WindowGroup(group) => self.validate_window_group(group)?,
            PhysicalRelOperator::UnionAll(union) => self.validate_union(union)?,
            PhysicalRelOperator::Scan(_)
            | PhysicalRelOperator::Join(_)
            | PhysicalRelOperator::Filter(_)
            | PhysicalRelOperator::Sort(_)
            | PhysicalRelOperator::Limit(_)
            | PhysicalRelOperator::TopN(_)
            | PhysicalRelOperator::Exchange(_)
            | PhysicalRelOperator::DummyTableScan(_)
            | PhysicalRelOperator::ConstantTableScan(_)
            | PhysicalRelOperator::ExpressionScan(_)
            | PhysicalRelOperator::CacheScan(_)
            | PhysicalRelOperator::RecursiveCteScan(_)
            | PhysicalRelOperator::Mutation(_)
            | PhysicalRelOperator::CompactBlock(_)
            | PhysicalRelOperator::MutationSource(_)
            | PhysicalRelOperator::MaterializedCTE(_)
            | PhysicalRelOperator::MaterializedCTERef(_)
            | PhysicalRelOperator::Sequence(_) => {}
        }
        Ok(VisitAction::Continue)
    }
}

impl ExprTypeValidator<'_> {
    fn validate_operator_scalars(&self, plan: &impl Operator) -> Result<()> {
        for scalar in plan.scalar_expr_iter() {
            self.validate_scalar(scalar, SymbolTypeSource::create(self.metadata, self.name))?;
        }
        Ok(())
    }

    fn validate_aggregate(&self, aggregate: &Aggregate) -> Result<()> {
        self.validate_items(&aggregate.group_items)?;
        self.validate_items(&aggregate.aggregate_functions)
    }

    fn validate_window(&self, window: &Window) -> Result<()> {
        self.validate_symbol_type(window.index, &window.function.return_type(), "window")?;
        self.validate_window_function(&window.function)
    }

    fn validate_window_group(&self, group: &WindowGroup) -> Result<()> {
        self.validate_items(&group.scalar_items)?;
        for window in &group.windows {
            self.validate_window(window)?;
        }
        Ok(())
    }

    fn validate_union(&self, union: &UnionAll) -> Result<()> {
        if union.left_outputs.len() != union.right_outputs.len()
            || union.left_outputs.len() != union.output_indexes.len()
        {
            return Err(ErrorCode::Internal(format!(
                "{} union output length mismatch: left {}, right {}, output {}",
                self.name,
                union.left_outputs.len(),
                union.right_outputs.len(),
                union.output_indexes.len(),
            )));
        }
        for (((left, left_cast), (right, right_cast)), output) in union
            .left_outputs
            .iter()
            .zip(&union.right_outputs)
            .zip(&union.output_indexes)
        {
            let left_type = match left_cast {
                Some(cast) => cast.data_type(),
                None => Cow::Owned(self.metadata_type(*left)?),
            };
            let right_type = match right_cast {
                Some(cast) => cast.data_type(),
                None => Cow::Owned(self.metadata_type(*right)?),
            };
            self.validate_symbol_type(*output, left_type.as_ref(), "union left output")?;
            self.validate_symbol_type(*output, right_type.as_ref(), "union right output")?;
        }
        Ok(())
    }

    fn validate_items(&self, items: &[ScalarItem]) -> Result<()> {
        for item in items {
            self.validate_symbol_type(
                item.index,
                item.scalar.data_type().as_ref(),
                "scalar producer",
            )?;
        }
        Ok(())
    }

    fn validate_symbol_type(&self, index: Symbol, actual: &DataType, source: &str) -> Result<()> {
        let metadata_type = self.metadata_type(index)?;
        if metadata_type.remove_nullable() != actual.remove_nullable() {
            return Err(ErrorCode::Internal(format!(
                "{} type mismatch for {source} {index}: metadata declares {metadata_type:?}, expression declares {actual:?}",
                self.name
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
                ErrorCode::Internal(format!(
                    "{} references unknown metadata symbol {index}",
                    self.name
                ))
            })
    }

    fn validate_window_function(&self, function: &WindowFuncType) -> Result<()> {
        if let WindowFuncType::Aggregate(aggregate) = function {
            ScalarTypeValidator {
                symbol_types: SymbolTypeSource::create(self.metadata, self.name),
            }
            .validate_aggregate_function(aggregate)?;
        }
        Ok(())
    }

    fn validate_scan_scalars(&self, scan: &crate::plans::Scan) -> Result<()> {
        let metadata_symbols = SymbolTypeSource::create(self.metadata, self.name);
        for scalar in scan
            .push_down_predicates
            .iter()
            .flatten()
            .chain(scan.secure_predicates.iter().flatten())
            .chain(
                scan.prewhere
                    .iter()
                    .flat_map(|prewhere| prewhere.predicates.iter()),
            )
        {
            self.validate_scalar(scalar, metadata_symbols)?;
        }

        Ok(())
    }

    fn validate_scalar(&self, scalar: &ScalarExpr, symbol_types: SymbolTypeSource) -> Result<()> {
        ScalarTypeValidator { symbol_types }.visit(scalar)
    }
}

#[derive(Clone, Copy)]
struct SymbolTypeSource<'a> {
    name: &'static str,
    metadata: &'a MetadataRef,
}

impl<'a> SymbolTypeSource<'a> {
    fn create(metadata: &'a MetadataRef, name: &'static str) -> SymbolTypeSource<'a> {
        Self { metadata, name }
    }

    fn validate(&self, index: Symbol, actual: &DataType) -> Result<()> {
        let metadata = self.metadata.read();
        let expected = metadata
            .columns()
            .get(index.as_usize())
            .map(|column| column.data_type())
            .ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "{} references unknown metadata symbol {index}",
                    self.name
                ))
            })?;

        let types_match = expected.remove_nullable() == actual.remove_nullable();
        if !types_match {
            return Err(ErrorCode::Internal(format!(
                "{} bound column type mismatch for {index}: source declares {expected:?}, expression declares {actual:?}",
                self.name
            )));
        }
        Ok(())
    }
}

struct ScalarTypeValidator<'a> {
    symbol_types: SymbolTypeSource<'a>,
}

impl ScalarTypeValidator<'_> {
    fn validate_function_call(&mut self, function: &FunctionCall) -> Result<()> {
        let inferred = function.infer_return_type()?;
        if inferred != *function.return_type {
            return Err(ErrorCode::Internal(format!(
                "{} function return type mismatch for {}: stored {:?}, inferred {inferred:?}",
                self.symbol_types.name, function.func_name, function.return_type
            )));
        }
        Ok(())
    }

    fn validate_lambda_function(&mut self, function: &LambdaFunc) -> Result<()> {
        let mut refreshed = function.clone();
        refreshed.refresh_return_type()?;
        if refreshed.return_type != function.return_type {
            return Err(ErrorCode::Internal(format!(
                "{} lambda return type mismatch for {}: stored {:?}, expected {:?}",
                self.symbol_types.name,
                function.func_name,
                function.return_type,
                refreshed.return_type
            )));
        }
        Ok(())
    }

    fn validate_aggregate_function(&mut self, aggregate: &AggregateFunction) -> Result<()> {
        let argument_types = aggregate
            .args
            .iter()
            .map(|argument| argument.data_type().into_owned())
            .collect::<Vec<_>>();
        let order_by = aggregate.bound_order_by()?;
        let inferred = AGGR_REGISTRY
            .resolve(RawAggregateCall {
                name: &aggregate.func_name,
                params: &aggregate.params.clone(),
                args_type: &argument_types,
                distinct: false,
                order_by: &order_by,
            })?
            .signature()
            .return_type
            .clone();
        if inferred != *aggregate.return_type {
            return Err(ErrorCode::Internal(format!(
                "{} aggregate return type mismatch for {}: stored {:?}, inferred {inferred:?}",
                self.symbol_types.name, aggregate.display_name, aggregate.return_type
            )));
        }
        Ok(())
    }
}

impl ScalarExprVisitor<'_> for ScalarTypeValidator<'_> {
    fn visit_bound_column_ref(&mut self, column: &BoundColumnRef) -> Result<()> {
        self.symbol_types
            .validate(column.column.index, &column.column.data_type)
    }

    fn visit_aggregate_function(&mut self, aggregate: &AggregateFunction) -> Result<()> {
        self.validate_aggregate_function(aggregate)?;

        for expr in aggregate.exprs() {
            self.visit(expr)?;
        }
        Ok(())
    }

    fn visit_function_call(&mut self, function: &FunctionCall) -> Result<()> {
        self.validate_function_call(function)?;

        for argument in &function.arguments {
            self.visit(argument)?;
        }
        Ok(())
    }

    fn visit_lambda_function(&mut self, function: &LambdaFunc) -> Result<()> {
        self.validate_lambda_function(function)?;

        for argument in &function.args {
            self.visit(argument)?;
        }
        Ok(())
    }
}
