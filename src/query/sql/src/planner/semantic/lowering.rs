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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check;
use common_expression::types::DataType;
use common_expression::ColumnIndex;
use common_expression::DataSchema;
use common_expression::Expr;
use common_expression::RawExpr;
use common_functions::BUILTIN_FUNCTIONS;

use crate::binder::ColumnBindingBuilder;
use crate::plans::ScalarExpr;
use crate::ColumnBinding;
use crate::ColumnEntry;
use crate::IndexType;
use crate::Metadata;
use crate::Visibility;

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

impl TypeCheck<IndexType> for ScalarExpr {
    fn resolve_and_check(
        &self,
        resolver: &impl LoweringContext<ColumnID = IndexType>,
    ) -> Result<Expr<IndexType>> {
        let raw_expr = self.as_raw_expr().project_column_ref(|col| col.index);
        raw_expr.resolve_and_check(resolver)
    }

    fn type_check(&self) -> Result<Expr<IndexType>> {
        let raw_expr = self.as_raw_expr().project_column_ref(|col| col.index);
        raw_expr.type_check()
    }
}

impl ScalarExpr {
    /// Lowering `Scalar` into `RawExpr` to utilize with `common_expression::types::type_check`.
    /// Specific variants will be replaced with a `RawExpr::ColumnRef` with a dummy name.
    pub fn as_raw_expr(&self) -> RawExpr<ColumnBinding> {
        match self {
            ScalarExpr::BoundColumnRef(column_ref) => RawExpr::ColumnRef {
                span: column_ref.span,
                id: column_ref.column.clone(),
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
            ScalarExpr::ConstantExpr(constant) => RawExpr::Constant {
                span: constant.span,
                scalar: constant.value.clone(),
            },
            ScalarExpr::WindowFunction(win) => RawExpr::ColumnRef {
                span: None,
                id: ColumnBindingBuilder::new(
                    win.display_name.clone(),
                    usize::MAX,
                    Box::new(win.func.return_type()),
                    Visibility::Visible,
                )
                .build(),
                data_type: win.func.return_type(),
                display_name: win.display_name.clone(),
            },
            ScalarExpr::AggregateFunction(agg) => RawExpr::ColumnRef {
                span: None,
                id: ColumnBindingBuilder::new(
                    agg.display_name.clone(),
                    usize::MAX,
                    Box::new((*agg.return_type).clone()),
                    Visibility::Visible,
                )
                .build(),
                data_type: (*agg.return_type).clone(),
                display_name: agg.display_name.clone(),
            },
            ScalarExpr::LambdaFunction(func) => RawExpr::ColumnRef {
                span: None,
                id: ColumnBindingBuilder::new(
                    func.display_name.clone(),
                    usize::MAX,
                    Box::new((*func.return_type).clone()),
                    Visibility::Visible,
                )
                .build(),
                data_type: (*func.return_type).clone(),
                display_name: func.display_name.clone(),
            },
            ScalarExpr::FunctionCall(func) => RawExpr::FunctionCall {
                span: func.span,
                name: func.func_name.clone(),
                params: func.params.clone(),
                args: func.arguments.iter().map(ScalarExpr::as_raw_expr).collect(),
            },
            ScalarExpr::CastExpr(cast) => RawExpr::Cast {
                span: cast.span,
                is_try: cast.is_try,
                expr: Box::new(cast.argument.as_raw_expr()),
                dest_type: (*cast.target_type).clone(),
            },
            ScalarExpr::SubqueryExpr(subquery) => RawExpr::ColumnRef {
                span: subquery.span,
                id: new_dummy_column(subquery.data_type()),
                data_type: subquery.data_type(),
                display_name: "DUMMY".to_string(),
            },
        }
    }

    pub fn as_expr(&self) -> Result<Expr<ColumnBinding>> {
        self.as_raw_expr().type_check()
    }
}

fn new_dummy_column(data_type: DataType) -> ColumnBinding {
    ColumnBindingBuilder::new(
        "DUMMY".to_string(),
        usize::MAX,
        Box::new(data_type),
        Visibility::Visible,
    )
    .build()
}
