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

use std::collections::HashSet;

use databend_common_ast::Span;
use databend_common_ast::ast::Literal;
use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::cast_scalar;
use databend_common_expression::shrink_scalar;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::decimal::DecimalScalar;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::i256;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::TypeChecker;
use super::core_expr::CoreExprArena;
use super::core_expr::CoreExprArgs;
use super::core_expr::CoreMapEntries;
use crate::plans::ConstantExpr;
use crate::plans::ScalarExpr;

impl<'a, A> TypeChecker<'a, A> {
    #[inline]
    pub(super) fn resolve_literal(
        &self,
        span: Span,
        literal: &databend_common_ast::ast::Literal,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let (value, data_type) = literal_scalar(literal);

        let scalar_expr = ScalarExpr::ConstantExpr(ConstantExpr { span, value });
        Ok(Box::new((scalar_expr, data_type)))
    }
}

impl<'a, A> TypeChecker<'a, A>
where A: super::TypeCheckAdapter
{
    pub(super) fn resolve_core_array(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        exprs: &CoreExprArgs,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut elems = Vec::with_capacity(exprs.len());
        let mut constant_values: Option<Vec<(Scalar, DataType)>> =
            Some(Vec::with_capacity(exprs.len()));
        let mut element_type: Option<DataType> = None;
        let mut data_type_set = HashSet::with_capacity(2);

        for expr in exprs {
            let box (arg, data_type) = self.resolve_core(arena, *expr)?;
            if let Some(values) = constant_values.as_mut() {
                let maybe_constant = match &arg {
                    ScalarExpr::ConstantExpr(constant) => Some(constant.value.clone()),
                    ScalarExpr::TypedConstantExpr(constant, _) => Some(constant.value.clone()),
                    _ => None,
                };
                if let Some(value) = maybe_constant {
                    if data_type_set.contains(&data_type) {
                        elems.push(arg);
                        values.push((value, data_type));
                        continue;
                    }
                    element_type = if let Some(current_ty) = element_type.clone() {
                        common_super_type(
                            current_ty.clone(),
                            data_type.clone(),
                            &BUILTIN_FUNCTIONS.default_cast_rules,
                        )
                    } else {
                        Some(data_type.clone())
                    };

                    if element_type.is_some() {
                        data_type_set.insert(data_type.clone());
                        values.push((value, data_type));
                    } else {
                        constant_values = None;
                        element_type = None;
                    }
                } else {
                    constant_values = None;
                    element_type = None;
                }
            }
            elems.push(arg);
        }

        if let (Some(values), Some(element_ty)) = (constant_values, element_type) {
            let mut casted = Vec::with_capacity(values.len());
            for (value, ty) in values {
                if ty == element_ty {
                    casted.push(value);
                } else {
                    casted.push(cast_scalar(span, value, &element_ty, &BUILTIN_FUNCTIONS)?);
                }
            }
            return Ok(Self::build_core_constant_array(span, element_ty, casted));
        }

        self.resolve_scalar_function_call(span, "array", vec![], elems)
    }

    fn build_core_constant_array(
        span: Span,
        element_ty: DataType,
        values: Vec<Scalar>,
    ) -> Box<(ScalarExpr, DataType)> {
        let mut builder = ColumnBuilder::with_capacity(&element_ty, values.len());
        for value in &values {
            builder.push(value.as_ref());
        }
        let scalar = Scalar::Array(builder.build());
        Box::new((
            ConstantExpr {
                span,
                value: scalar,
            }
            .into(),
            DataType::Array(Box::new(element_ty)),
        ))
    }

    pub(super) fn resolve_core_map(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        kvs: &CoreMapEntries,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut keys = Vec::with_capacity(kvs.len());
        let mut vals = Vec::with_capacity(kvs.len());
        for (key_expr, val_expr) in kvs {
            let box (key_arg, _data_type) = self.resolve_literal(span, key_expr)?;
            keys.push(key_arg);
            let box (val_arg, _data_type) = self.resolve_core(arena, *val_expr)?;
            vals.push(val_arg);
        }
        let box (key_arg, _data_type) =
            self.resolve_scalar_function_call(span, "array", vec![], keys)?;
        let box (val_arg, _data_type) =
            self.resolve_scalar_function_call(span, "array", vec![], vals)?;
        self.resolve_scalar_function_call(span, "map", vec![], vec![key_arg, val_arg])
    }

    pub(super) fn resolve_core_tuple(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        exprs: &CoreExprArgs,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut args = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let box (arg, _data_type) = self.resolve_core(arena, *expr)?;
            args.push(arg);
        }
        self.resolve_scalar_function_call(span, "tuple", vec![], args)
    }
}

pub(super) fn literal_scalar(literal: &databend_common_ast::ast::Literal) -> (Scalar, DataType) {
    infer_literal_data_type(literal_value(literal))
}

pub(super) fn literal_value(literal: &databend_common_ast::ast::Literal) -> Scalar {
    match literal {
        Literal::UInt64(value) => Scalar::Number(NumberScalar::UInt64(*value)),
        Literal::Decimal256 {
            value,
            precision,
            scale,
        } => Scalar::Decimal(DecimalScalar::Decimal256(
            i256(*value),
            DecimalSize::new_unchecked(*precision, *scale),
        )),
        Literal::Float64(float) => Scalar::Number(NumberScalar::Float64((*float).into())),
        Literal::String(string) => Scalar::String(string.clone()),
        Literal::Binary(bytes) => Scalar::Binary(bytes.clone()),
        Literal::Boolean(boolean) => Scalar::Boolean(*boolean),
        Literal::Null => Scalar::Null,
    }
}

pub(super) fn infer_literal_data_type(value: Scalar) -> (Scalar, DataType) {
    let value = shrink_scalar(value);
    let data_type = value.as_ref().infer_data_type();
    (value, data_type)
}
