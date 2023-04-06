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

use std::collections::HashMap;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check;
use common_expression::types::DataType;
use common_expression::ColumnIndex;
use common_expression::DataSchema;
use common_expression::Expr;
use common_expression::RawExpr;
use common_functions::BUILTIN_FUNCTIONS;

use crate::plans::ScalarExpr;
use crate::ColumnEntry;
use crate::IndexType;
use crate::Metadata;

const DUMMY_NAME: &str = "DUMMY";
const DUMMY_INDEX: usize = usize::MAX;

pub trait LoweringContext {
    type ColumnID: ColumnIndex;

    fn resolve_column_type(&self, column_id: &Self::ColumnID) -> Result<DataType>;
}

impl LoweringContext for Metadata {
    type ColumnID = IndexType;

    fn resolve_column_type(&self, column_id: &Self::ColumnID) -> Result<DataType> {
        let column_entry = self.column(*column_id);
        match column_entry {
            ColumnEntry::BaseTableColumn(column) => Ok(DataType::from(&column.data_type)),
            ColumnEntry::DerivedColumn(column) => Ok(column.data_type.clone()),
            ColumnEntry::InternalColumn(column) => Ok(column.internal_column.data_type()),
            ColumnEntry::VirtualColumn(column) => Ok(DataType::from(&column.data_type)),
        }
    }
}

impl LoweringContext for DataSchema {
    type ColumnID = IndexType;

    fn resolve_column_type(&self, column_id: &Self::ColumnID) -> Result<DataType> {
        let column = self.field_with_name(&column_id.to_string())?;
        Ok(column.data_type().clone())
    }
}

impl<Index> LoweringContext for HashMap<Index, DataType>
where Index: ColumnIndex
{
    type ColumnID = Index;

    fn resolve_column_type(&self, column_id: &Self::ColumnID) -> Result<DataType> {
        self.get(column_id).cloned().ok_or_else(|| {
            ErrorCode::Internal(format!(
                "Logical error: can not find column {:?}",
                column_id
            ))
        })
    }
}

fn resolve_column_type<C: LoweringContext>(
    raw_expr: &RawExpr<C::ColumnID>,
    context: &C,
) -> Result<RawExpr<C::ColumnID>> {
    match raw_expr {
        RawExpr::ColumnRef {
            span,
            id,
            display_name,
            ..
        } => {
            let data_type = context.resolve_column_type(id)?;
            Ok(RawExpr::ColumnRef {
                id: id.clone(),
                span: *span,
                display_name: display_name.clone(),
                data_type,
            })
        }
        RawExpr::Cast {
            span,
            is_try,
            expr,
            dest_type,
        } => Ok(RawExpr::Cast {
            span: *span,
            is_try: *is_try,
            expr: Box::new(resolve_column_type(expr, context)?),
            dest_type: dest_type.clone(),
        }),
        RawExpr::FunctionCall {
            span,
            name,
            params,
            args,
        } => {
            let args = args
                .iter()
                .map(|arg| resolve_column_type(arg, context))
                .collect::<Result<Vec<_>>>()?;
            Ok(RawExpr::FunctionCall {
                span: *span,
                name: name.clone(),
                params: params.clone(),
                args,
            })
        }
        RawExpr::Constant { .. } => Ok(raw_expr.clone()),
    }
}

pub trait TypeCheck<Index: ColumnIndex> {
    /// Resolve data type with `LoweringContext` and perform type check.
    fn resolve_and_check(
        &self,
        ctx: &impl LoweringContext<ColumnID = Index>,
    ) -> Result<Expr<Index>>;

    /// Perform type check without resolving data type.
    fn type_check(&self) -> Result<Expr<Index>>;
}

impl<Index: ColumnIndex> TypeCheck<Index> for RawExpr<Index> {
    fn resolve_and_check(
        &self,
        resolver: &impl LoweringContext<ColumnID = Index>,
    ) -> Result<Expr<Index>> {
        let raw_expr = resolve_column_type(self, resolver)?;
        check(&raw_expr, &BUILTIN_FUNCTIONS)
    }

    fn type_check(&self) -> Result<Expr<Index>> {
        check(self, &BUILTIN_FUNCTIONS)
    }
}

impl TypeCheck<String> for ScalarExpr {
    fn resolve_and_check(
        &self,
        resolver: &impl LoweringContext<ColumnID = String>,
    ) -> Result<Expr<String>> {
        let raw_expr = self.as_raw_expr_with_col_name();
        raw_expr.resolve_and_check(resolver)
    }

    fn type_check(&self) -> Result<Expr<String>> {
        let raw_expr = self.as_raw_expr_with_col_name();
        raw_expr.type_check()
    }
}

impl TypeCheck<IndexType> for ScalarExpr {
    fn resolve_and_check(
        &self,
        resolver: &impl LoweringContext<ColumnID = IndexType>,
    ) -> Result<Expr<IndexType>> {
        let raw_expr = self.as_raw_expr_with_col_index();
        raw_expr.resolve_and_check(resolver)
    }

    fn type_check(&self) -> Result<Expr<IndexType>> {
        let raw_expr = self.as_raw_expr_with_col_index();
        raw_expr.type_check()
    }
}

impl ScalarExpr {
    /// Lowering `Scalar` into `RawExpr` to utilize with `common_expression::types::type_check`.
    /// Specific variants will be replaced with a `RawExpr::ColumnRef` with a dummy name.
    pub fn as_raw_expr_with_col_name(&self) -> RawExpr<String> {
        match self {
            ScalarExpr::BoundColumnRef(column_ref) => RawExpr::ColumnRef {
                span: column_ref.span,
                id: column_ref.column.column_name.clone(),
                data_type: *column_ref.column.data_type.clone(),
                display_name: format!(
                    "{}{} (#{})",
                    column_ref
                        .column
                        .table_name
                        .as_ref()
                        .map_or("".to_string(), |t| t.to_string() + "."),
                    column_ref.column.column_name.clone(),
                    column_ref.column.index
                ),
            },
            ScalarExpr::BoundInternalColumnRef(column_ref) => RawExpr::ColumnRef {
                span: None,
                id: column_ref.column.internal_column.column_name().clone(),
                data_type: column_ref.column.internal_column.data_type(),
                display_name: format!(
                    "{}{} (#{})",
                    column_ref
                        .column
                        .table_name
                        .as_ref()
                        .map_or("".to_string(), |t| t.to_string() + "."),
                    column_ref.column.internal_column.column_name().clone(),
                    column_ref.column.index
                ),
            },
            ScalarExpr::ConstantExpr(constant) => RawExpr::Constant {
                span: constant.span,
                scalar: constant.value.clone(),
            },
            ScalarExpr::AndExpr(expr) => RawExpr::FunctionCall {
                span: None,
                name: "and".to_string(),
                params: vec![],
                args: vec![
                    expr.left.as_raw_expr_with_col_name(),
                    expr.right.as_raw_expr_with_col_name(),
                ],
            },
            ScalarExpr::OrExpr(expr) => RawExpr::FunctionCall {
                span: None,
                name: "or".to_string(),
                params: vec![],
                args: vec![
                    expr.left.as_raw_expr_with_col_name(),
                    expr.right.as_raw_expr_with_col_name(),
                ],
            },
            ScalarExpr::NotExpr(expr) => RawExpr::FunctionCall {
                span: None,
                name: "not".to_string(),
                params: vec![],
                args: vec![expr.argument.as_raw_expr_with_col_name()],
            },
            ScalarExpr::ComparisonExpr(expr) => RawExpr::FunctionCall {
                span: None,
                name: expr.op.to_func_name().to_string(),
                params: vec![],
                args: vec![
                    expr.left.as_raw_expr_with_col_name(),
                    expr.right.as_raw_expr_with_col_name(),
                ],
            },
            ScalarExpr::WindowFunction(win) => RawExpr::ColumnRef {
                span: None,
                id: win.display_name.clone(),
                data_type: win.func.return_type(),
                display_name: win.display_name.clone(),
            },
            ScalarExpr::AggregateFunction(agg) => RawExpr::ColumnRef {
                span: None,
                id: agg.display_name.clone(),
                data_type: (*agg.return_type).clone(),
                display_name: agg.display_name.clone(),
            },
            ScalarExpr::FunctionCall(func) => RawExpr::FunctionCall {
                span: func.span,
                name: func.func_name.clone(),
                params: func.params.clone(),
                args: func
                    .arguments
                    .iter()
                    .map(ScalarExpr::as_raw_expr_with_col_name)
                    .collect(),
            },
            ScalarExpr::CastExpr(cast) => RawExpr::Cast {
                span: cast.span,
                is_try: cast.is_try,
                expr: Box::new(cast.argument.as_raw_expr_with_col_name()),
                dest_type: (*cast.target_type).clone(),
            },
            ScalarExpr::SubqueryExpr(subquery) => RawExpr::ColumnRef {
                span: subquery.span,
                id: DUMMY_NAME.to_string(),
                data_type: subquery.data_type(),
                display_name: DUMMY_NAME.to_string(),
            },
        }
    }

    pub fn as_expr_with_col_name(&self) -> Result<Expr<String>> {
        self.as_raw_expr_with_col_name().type_check()
    }

    pub fn as_raw_expr_with_col_index(&self) -> RawExpr {
        match self {
            ScalarExpr::BoundColumnRef(column_ref) => RawExpr::ColumnRef {
                span: column_ref.span,
                id: column_ref.column.index,
                data_type: *column_ref.column.data_type.clone(),
                display_name: format!(
                    "{}{} (#{})",
                    column_ref
                        .column
                        .table_name
                        .as_ref()
                        .map_or("".to_string(), |t| t.to_string() + "."),
                    column_ref.column.column_name.clone(),
                    column_ref.column.index
                ),
            },
            ScalarExpr::BoundInternalColumnRef(column_ref) => RawExpr::ColumnRef {
                span: None,
                id: column_ref.column.index,
                data_type: column_ref.column.internal_column.data_type(),
                display_name: format!(
                    "{}{} (#{})",
                    column_ref
                        .column
                        .table_name
                        .as_ref()
                        .map_or("".to_string(), |t| t.to_string() + "."),
                    column_ref.column.internal_column.column_name().clone(),
                    column_ref.column.index
                ),
            },
            ScalarExpr::ConstantExpr(constant) => RawExpr::Constant {
                span: constant.span,
                scalar: constant.value.clone(),
            },
            ScalarExpr::AndExpr(expr) => RawExpr::FunctionCall {
                span: None,
                name: "and".to_string(),
                params: vec![],
                args: vec![
                    expr.left.as_raw_expr_with_col_index(),
                    expr.right.as_raw_expr_with_col_index(),
                ],
            },
            ScalarExpr::OrExpr(expr) => RawExpr::FunctionCall {
                span: None,
                name: "or".to_string(),
                params: vec![],
                args: vec![
                    expr.left.as_raw_expr_with_col_index(),
                    expr.right.as_raw_expr_with_col_index(),
                ],
            },
            ScalarExpr::NotExpr(expr) => RawExpr::FunctionCall {
                span: None,
                name: "not".to_string(),
                params: vec![],
                args: vec![expr.argument.as_raw_expr_with_col_index()],
            },
            ScalarExpr::ComparisonExpr(expr) => RawExpr::FunctionCall {
                span: None,
                name: expr.op.to_func_name().to_string(),
                params: vec![],
                args: vec![
                    expr.left.as_raw_expr_with_col_index(),
                    expr.right.as_raw_expr_with_col_index(),
                ],
            },
            ScalarExpr::WindowFunction(win) => RawExpr::ColumnRef {
                span: None,
                id: DUMMY_INDEX,
                data_type: win.func.return_type(),
                display_name: win.display_name.clone(),
            },
            ScalarExpr::AggregateFunction(agg) => RawExpr::ColumnRef {
                span: None,
                id: DUMMY_INDEX,
                data_type: (*agg.return_type).clone(),
                display_name: agg.display_name.clone(),
            },
            ScalarExpr::FunctionCall(func) => RawExpr::FunctionCall {
                span: func.span,
                name: func.func_name.clone(),
                params: func.params.clone(),
                args: func
                    .arguments
                    .iter()
                    .map(ScalarExpr::as_raw_expr_with_col_index)
                    .collect(),
            },
            ScalarExpr::CastExpr(cast) => RawExpr::Cast {
                span: cast.span,
                is_try: cast.is_try,
                expr: Box::new(cast.argument.as_raw_expr_with_col_index()),
                dest_type: (*cast.target_type).clone(),
            },
            ScalarExpr::SubqueryExpr(subquery) => RawExpr::ColumnRef {
                span: subquery.span,
                id: DUMMY_INDEX,
                data_type: subquery.data_type(),
                display_name: DUMMY_NAME.to_string(),
            },
        }
    }

    pub fn as_expr_with_col_index(&self) -> Result<Expr> {
        self.as_raw_expr_with_col_index().type_check()
    }
}
