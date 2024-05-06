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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check;
use databend_common_expression::types::DataType;
use databend_common_expression::ColumnIndex;
use databend_common_expression::DataSchema;
use databend_common_expression::Expr;
use databend_common_expression::RawExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::binder::ColumnBindingBuilder;
use crate::plans::ScalarExpr;
use crate::ColumnBinding;
use crate::ColumnEntry;
use crate::IndexType;
use crate::Metadata;
use crate::Visibility;

pub trait TypeProvider<ColumnID: ColumnIndex> {
    fn get_type(&self, column_id: &ColumnID) -> Result<DataType>;
}

impl TypeProvider<IndexType> for Metadata {
    fn get_type(&self, column_id: &IndexType) -> Result<DataType> {
        let column_entry = self.column(*column_id);
        match column_entry {
            ColumnEntry::BaseTableColumn(column) => Ok(DataType::from(&column.data_type)),
            ColumnEntry::DerivedColumn(column) => Ok(column.data_type.clone()),
            ColumnEntry::InternalColumn(column) => Ok(column.internal_column.data_type()),
            ColumnEntry::VirtualColumn(column) => Ok(DataType::from(&column.data_type)),
        }
    }
}

impl TypeProvider<ColumnBinding> for Metadata {
    fn get_type(&self, column_id: &ColumnBinding) -> Result<DataType> {
        self.get_type(&column_id.index)
    }
}

impl TypeProvider<IndexType> for DataSchema {
    fn get_type(&self, column_id: &IndexType) -> Result<DataType> {
        let column = self.field_with_name(&column_id.to_string())?;
        Ok(column.data_type().clone())
    }
}

impl TypeProvider<String> for DataSchema {
    fn get_type(&self, column_name: &String) -> Result<DataType> {
        let column = self.field_with_name(column_name)?;
        Ok(column.data_type().clone())
    }
}

impl<ColumnID: ColumnIndex> TypeProvider<ColumnID> for HashMap<ColumnID, DataType> {
    fn get_type(&self, column_id: &ColumnID) -> Result<DataType> {
        self.get(column_id).cloned().ok_or_else(|| {
            ErrorCode::Internal(format!(
                "Logical error: can not find column {:?}",
                column_id
            ))
        })
    }
}

fn update_column_type<ColumnID: ColumnIndex, TP: TypeProvider<ColumnID>>(
    raw_expr: &RawExpr<ColumnID>,
    type_provider: &TP,
) -> Result<RawExpr<ColumnID>> {
    match raw_expr {
        RawExpr::ColumnRef {
            span,
            id,
            display_name,
            ..
        } => {
            let data_type = type_provider.get_type(id)?;
            Ok(RawExpr::ColumnRef {
                id: id.clone(),
                span: *span,
                display_name: display_name.clone(),
                data_type,
            })
        }
        RawExpr::Constant { .. } => Ok(raw_expr.clone()),
        RawExpr::Cast {
            span,
            is_try,
            expr,
            dest_type,
        } => Ok(RawExpr::Cast {
            span: *span,
            is_try: *is_try,
            expr: Box::new(update_column_type(expr, type_provider)?),
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
                .map(|arg| update_column_type(arg, type_provider))
                .collect::<Result<Vec<_>>>()?;
            Ok(RawExpr::FunctionCall {
                span: *span,
                name: name.clone(),
                params: params.clone(),
                args,
            })
        }
        RawExpr::LambdaFunctionCall {
            span,
            name,
            args,
            lambda_expr,
            lambda_display,
            return_type,
        } => {
            let args = args
                .iter()
                .map(|arg| update_column_type(arg, type_provider))
                .collect::<Result<Vec<_>>>()?;
            Ok(RawExpr::LambdaFunctionCall {
                span: *span,
                name: name.clone(),
                args,
                lambda_expr: lambda_expr.clone(),
                lambda_display: lambda_display.clone(),
                return_type: return_type.clone(),
            })
        }
    }
}

pub trait TypeCheck<Index: ColumnIndex> {
    /// Resolve data type with `LoweringContext` and perform type check.
    fn type_check(&self, ctx: &impl TypeProvider<Index>) -> Result<Expr<Index>>;
}

impl<ColumnID: ColumnIndex> TypeCheck<ColumnID> for RawExpr<ColumnID> {
    fn type_check(&self, type_provider: &impl TypeProvider<ColumnID>) -> Result<Expr<ColumnID>> {
        let raw_expr = update_column_type(self, type_provider)?;
        type_check::check(&raw_expr, &BUILTIN_FUNCTIONS)
    }
}

impl TypeCheck<IndexType> for ScalarExpr {
    fn type_check(&self, type_provider: &impl TypeProvider<IndexType>) -> Result<Expr<IndexType>> {
        let raw_expr = self.as_raw_expr().project_column_ref(|col| col.index);
        raw_expr.type_check(type_provider)
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
            ScalarExpr::LambdaFunction(func) => RawExpr::LambdaFunctionCall {
                span: None,
                name: func.func_name.clone(),
                args: func.args.iter().map(ScalarExpr::as_raw_expr).collect(),
                lambda_expr: (*func.lambda_expr).clone(),
                lambda_display: func.lambda_display.clone(),
                return_type: (*func.return_type).clone(),
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
            ScalarExpr::UDFCall(udf) => RawExpr::ColumnRef {
                span: None,
                id: ColumnBindingBuilder::new(
                    udf.display_name.clone(),
                    usize::MAX,
                    Box::new((*udf.return_type).clone()),
                    Visibility::Visible,
                )
                .build(),
                data_type: (*udf.return_type).clone(),
                display_name: udf.display_name.clone(),
            },

            ScalarExpr::UDFLambdaCall(udf) => {
                let scalar = &udf.scalar;
                scalar.as_raw_expr()
            }

            ScalarExpr::AsyncFunctionCall(table_func) => RawExpr::ColumnRef {
                span: None,
                id: ColumnBindingBuilder::new(
                    table_func.display_name.clone(),
                    usize::MAX,
                    Box::new(table_func.return_type.as_ref().clone()),
                    Visibility::Visible,
                )
                .build(),
                data_type: table_func.return_type.as_ref().clone(),
                display_name: table_func.display_name.clone(),
            },
        }
    }

    pub fn as_expr(&self) -> Result<Expr<ColumnBinding>> {
        type_check::check(&self.as_raw_expr(), &BUILTIN_FUNCTIONS)
    }

    pub fn is_column_ref(&self) -> bool {
        matches!(self, ScalarExpr::BoundColumnRef(_))
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
