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
use std::fmt::Debug;
use std::fmt::Write;
use std::hash::Hash;
use std::sync::Arc;

use databend_common_ast::Span;
use educe::Educe;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

use crate::function::Function;
use crate::function::FunctionID;
use crate::function::FunctionRegistry;
use crate::types::DataType;
use crate::values::Scalar;

pub trait ColumnIndex: Debug + Clone + Serialize + Hash + Eq + 'static {
    fn unique_name<W: Write>(&self, f: &mut W) -> std::fmt::Result;
}

impl ColumnIndex for usize {
    fn unique_name<W: Write>(&self, f: &mut W) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl ColumnIndex for String {
    fn unique_name<W: Write>(&self, f: &mut W) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

/// An unchecked expression that is directly discarded from SQL or constructed by the planner.
/// It can be type-checked and then converted to an evaluable [`Expr`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum RawExpr<Index: ColumnIndex = usize> {
    Constant {
        span: Span,
        scalar: Scalar,
        data_type: Option<DataType>,
    },
    ColumnRef {
        span: Span,
        id: Index,
        data_type: DataType,

        // The name used to pretty print the expression.
        display_name: String,
    },
    Cast {
        span: Span,
        is_try: bool,
        expr: Box<RawExpr<Index>>,
        dest_type: DataType,
    },
    FunctionCall {
        span: Span,
        name: String,
        params: Vec<Scalar>,
        args: Vec<RawExpr<Index>>,
    },
    LambdaFunctionCall {
        span: Span,
        name: String,
        args: Vec<RawExpr<Index>>,
        lambda_expr: Box<RemoteExpr>,
        lambda_display: String,
        return_type: DataType,
    },
}

/// A type-checked and ready to be evaluated expression, having all overloads chosen for function calls.
/// It is .
#[derive(Debug, Educe, EnumAsInner)]
#[educe(Hash(bound = false))]
pub enum Expr<Index: ColumnIndex = usize> {
    Constant {
        #[educe(Hash(ignore))]
        span: Span,
        scalar: Scalar,
        data_type: DataType,
    },
    ColumnRef {
        #[educe(Hash(ignore))]
        span: Span,
        id: Index,
        data_type: DataType,

        // The name used to pretty print the expression.
        display_name: String,
    },
    Cast {
        #[educe(Hash(ignore))]
        span: Span,
        is_try: bool,
        expr: Box<Expr<Index>>,
        dest_type: DataType,
    },
    FunctionCall {
        #[educe(Hash(ignore))]
        span: Span,
        id: Box<FunctionID>,
        #[educe(Hash(ignore))]
        function: Arc<Function>,
        generics: Vec<DataType>,
        args: Vec<Expr<Index>>,
        return_type: DataType,
    },
    LambdaFunctionCall {
        #[educe(Hash(ignore))]
        span: Span,
        name: String,
        args: Vec<Expr<Index>>,
        lambda_expr: Box<RemoteExpr>,
        lambda_display: String,
        return_type: DataType,
    },
}

impl<Index: ColumnIndex> Clone for Expr<Index> {
    #[recursive::recursive]
    fn clone(&self) -> Self {
        match self {
            Expr::Constant {
                span,
                scalar,
                data_type,
            } => Expr::Constant {
                span: *span,
                scalar: scalar.clone(),
                data_type: data_type.clone(),
            },
            Expr::ColumnRef {
                span,
                id,
                data_type,
                display_name,
            } => Expr::ColumnRef {
                span: *span,
                id: id.clone(),
                data_type: data_type.clone(),
                display_name: display_name.clone(),
            },
            Expr::Cast {
                span,
                is_try,
                expr,
                dest_type,
            } => Expr::Cast {
                span: *span,
                is_try: *is_try,
                expr: expr.clone(),
                dest_type: dest_type.clone(),
            },
            Expr::FunctionCall {
                span,
                id,
                function,
                generics,
                args,
                return_type,
            } => Expr::FunctionCall {
                span: *span,
                id: id.clone(),
                function: function.clone(),
                generics: generics.clone(),
                args: args.clone(),
                return_type: return_type.clone(),
            },
            Expr::LambdaFunctionCall {
                span,
                name,
                args,
                lambda_expr,
                lambda_display,
                return_type,
            } => Expr::LambdaFunctionCall {
                span: *span,
                name: name.clone(),
                args: args.clone(),
                lambda_expr: lambda_expr.clone(),
                lambda_display: lambda_display.clone(),
                return_type: return_type.clone(),
            },
        }
    }
}

impl<Index: ColumnIndex> Eq for Expr<Index> {}

impl<Index: ColumnIndex> PartialEq for Expr<Index> {
    #[recursive::recursive]
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Expr::Constant {
                    scalar: l_scalar,
                    data_type: l_data_type,
                    ..
                },
                Expr::Constant {
                    scalar: r_scalar,
                    data_type: r_data_type,
                    ..
                },
            ) => l_scalar.eq(r_scalar) && l_data_type.eq(r_data_type),
            (
                Expr::ColumnRef {
                    id: l_id,
                    data_type: l_data_type,
                    display_name: l_display_name,
                    ..
                },
                Expr::ColumnRef {
                    id: r_id,
                    data_type: r_data_type,
                    display_name: r_display_name,
                    ..
                },
            ) => l_id.eq(r_id) && l_data_type.eq(r_data_type) && l_display_name.eq(r_display_name),
            (
                Expr::Cast {
                    is_try: l_is_try,
                    expr: l_expr,
                    dest_type: l_dest_type,
                    ..
                },
                Expr::Cast {
                    is_try: r_is_try,
                    expr: r_expr,
                    dest_type: r_dest_type,
                    ..
                },
            ) => l_is_try.eq(r_is_try) && l_expr.eq(r_expr) && l_dest_type.eq(r_dest_type),
            (
                Expr::FunctionCall {
                    id: l_id,
                    generics: l_generics,
                    args: l_args,
                    return_type: l_return_type,
                    ..
                },
                Expr::FunctionCall {
                    id: r_id,
                    generics: r_generics,
                    args: r_args,
                    return_type: r_return_type,
                    ..
                },
            ) => {
                l_id.eq(r_id)
                    && l_generics.eq(r_generics)
                    && l_args.eq(r_args)
                    && l_return_type.eq(r_return_type)
            }
            (
                Expr::LambdaFunctionCall {
                    name: l_name,
                    args: l_args,
                    lambda_expr: l_lambda_expr,
                    lambda_display: l_lambda_display,
                    return_type: l_return_type,
                    ..
                },
                Expr::LambdaFunctionCall {
                    name: r_name,
                    args: r_args,
                    lambda_expr: r_lambda_expr,
                    lambda_display: r_lambda_display,
                    return_type: r_rteurn_type,
                    ..
                },
            ) => {
                l_name.eq(r_name)
                    && l_args.eq(r_args)
                    && l_lambda_expr.eq(r_lambda_expr)
                    && l_lambda_display.eq(r_lambda_display)
                    && l_return_type.eq(r_rteurn_type)
            }
            _ => false,
        }
    }
}

pub trait ExprVisitor<I: ColumnIndex>: Sized {
    type Error = !;

    fn enter_constant(&mut self, expr: &Expr<I>) -> Result<Option<Expr<I>>, Self::Error> {
        debug_assert!(expr.is_constant());
        Ok(None)
    }

    fn enter_column_ref(&mut self, expr: &Expr<I>) -> Result<Option<Expr<I>>, Self::Error> {
        debug_assert!(expr.is_column_ref());
        Ok(None)
    }

    fn enter_cast(&mut self, expr: &Expr<I>) -> Result<Option<Expr<I>>, Self::Error> {
        Self::visit_cast(expr, self)
    }

    fn visit_cast<V>(expr: &Expr<I>, visitor: &mut V) -> Result<Option<Expr<I>>, Self::Error>
    where
        I: ColumnIndex,
        V: ExprVisitor<I, Error = Self::Error>,
    {
        let Expr::Cast {
            span,
            is_try,
            expr: inner,
            dest_type,
        } = expr
        else {
            unreachable!()
        };
        if let Some(inner) = visit_expr(inner, visitor)? {
            Ok(Some(Expr::Cast {
                span: *span,
                is_try: *is_try,
                expr: Box::new(inner.clone()),
                dest_type: dest_type.clone(),
            }))
        } else {
            Ok(None)
        }
    }

    fn enter_function_call(&mut self, expr: &Expr<I>) -> Result<Option<Expr<I>>, Self::Error> {
        Self::visit_function_call(expr, self)
    }

    fn visit_function_call<V>(
        expr: &Expr<I>,
        visitor: &mut V,
    ) -> Result<Option<Expr<I>>, Self::Error>
    where
        V: ExprVisitor<I, Error = Self::Error>,
    {
        let Expr::FunctionCall {
            span,
            id,
            function,
            generics,
            args,
            return_type,
        } = expr
        else {
            unreachable!()
        };
        let new_args = args
            .iter()
            .map(|arg| Ok((visit_expr(arg, visitor)?, arg)))
            .collect::<Result<Vec<_>, _>>()?;
        if new_args.iter().all(|(v, _)| v.is_none()) {
            Ok(None)
        } else {
            Ok(Some(Expr::FunctionCall {
                span: *span,
                id: id.clone(),
                function: function.clone(),
                generics: generics.clone(),
                args: new_args
                    .into_iter()
                    .map(|(new, old)| new.unwrap_or_else(|| old.clone()))
                    .collect(),
                return_type: return_type.clone(),
            }))
        }
    }

    fn enter_lambda_function_call(
        &mut self,
        expr: &Expr<I>,
    ) -> Result<Option<Expr<I>>, Self::Error> {
        Self::visit_lambda_function_call(expr, self)
    }

    fn visit_lambda_function_call<V>(
        expr: &Expr<I>,
        visitor: &mut V,
    ) -> Result<Option<Expr<I>>, Self::Error>
    where
        V: ExprVisitor<I, Error = Self::Error>,
    {
        let Expr::LambdaFunctionCall {
            span,
            name,
            args,
            lambda_expr,
            lambda_display,
            return_type,
        } = expr
        else {
            unreachable!()
        };
        let new_args = args
            .iter()
            .map(|arg| Ok((visit_expr(arg, visitor)?, arg)))
            .collect::<Result<Vec<_>, _>>()?;
        if new_args.iter().all(|(v, _)| v.is_none()) {
            Ok(None)
        } else {
            Ok(Some(Expr::LambdaFunctionCall {
                span: *span,
                name: name.clone(),
                args: new_args
                    .into_iter()
                    .map(|(new, old)| new.unwrap_or_else(|| old.clone()))
                    .collect(),
                lambda_expr: lambda_expr.clone(),
                lambda_display: lambda_display.clone(),
                return_type: return_type.clone(),
            }))
        }
    }
}

#[recursive::recursive]
pub fn visit_expr<Index: ColumnIndex, V: ExprVisitor<Index>>(
    expr: &Expr<Index>,
    visitor: &mut V,
) -> Result<Option<Expr<Index>>, V::Error> {
    match expr {
        Expr::Constant { .. } => visitor.enter_constant(expr),
        Expr::ColumnRef { .. } => visitor.enter_column_ref(expr),
        Expr::Cast { .. } => visitor.enter_cast(expr),
        Expr::FunctionCall { .. } => visitor.enter_function_call(expr),
        Expr::LambdaFunctionCall { .. } => visitor.enter_lambda_function_call(expr),
    }
}

/// Serializable expression used to share executable expression between nodes.
///
/// The remote node will recover the `Arc` pointer within `FunctionCall` by looking
/// up the function registry with the `FunctionID`.
#[derive(Debug, Clone, Educe, Serialize, Deserialize, EnumAsInner)]
#[educe(PartialEq(bound(false)), Eq, Hash(bound(false)))]
pub enum RemoteExpr<Index: ColumnIndex = usize> {
    Constant {
        #[educe(Hash(ignore), PartialEq(ignore))]
        span: Span,
        scalar: Scalar,
        data_type: DataType,
    },
    ColumnRef {
        #[educe(Hash(ignore), PartialEq(ignore))]
        span: Span,
        id: Index,
        data_type: DataType,

        // The name used to pretty print the expression.
        display_name: String,
    },
    Cast {
        #[educe(Hash(ignore), PartialEq(ignore))]
        span: Span,
        is_try: bool,
        expr: Box<RemoteExpr<Index>>,
        dest_type: DataType,
    },
    FunctionCall {
        #[educe(Hash(ignore), PartialEq(ignore))]
        span: Span,
        id: Box<FunctionID>,
        generics: Vec<DataType>,
        args: Vec<RemoteExpr<Index>>,
        return_type: DataType,
    },
    LambdaFunctionCall {
        #[educe(Hash(ignore), PartialEq(ignore))]
        span: Span,
        name: String,
        args: Vec<RemoteExpr<Index>>,
        lambda_expr: Box<RemoteExpr>,
        lambda_display: String,
        return_type: DataType,
    },
}

impl<Index: ColumnIndex> RawExpr<Index> {
    pub fn column_refs(&self) -> HashMap<Index, DataType> {
        #[recursive::recursive]
        fn walk<Index: ColumnIndex>(expr: &RawExpr<Index>, buf: &mut HashMap<Index, DataType>) {
            match expr {
                RawExpr::ColumnRef { id, data_type, .. } => {
                    buf.insert(id.clone(), data_type.clone());
                }
                RawExpr::Cast { expr, .. } => walk(expr, buf),
                RawExpr::Constant { .. } => (),
                RawExpr::FunctionCall { args, .. } => args.iter().for_each(|expr| walk(expr, buf)),
                RawExpr::LambdaFunctionCall { args, .. } => {
                    args.iter().for_each(|expr| walk(expr, buf))
                }
            }
        }

        let mut buf = HashMap::new();
        walk(self, &mut buf);
        buf
    }

    pub fn project_column_ref<ToIndex: ColumnIndex>(
        &self,
        f: impl Fn(&Index) -> ToIndex + Copy,
    ) -> RawExpr<ToIndex> {
        match self {
            RawExpr::Constant {
                span,
                scalar,
                data_type,
            } => RawExpr::Constant {
                span: *span,
                scalar: scalar.clone(),
                data_type: data_type.clone(),
            },
            RawExpr::ColumnRef {
                span,
                id,
                data_type,
                display_name,
            } => RawExpr::ColumnRef {
                span: *span,
                id: f(id),
                data_type: data_type.clone(),
                display_name: display_name.clone(),
            },
            RawExpr::Cast {
                span,
                is_try,
                expr,
                dest_type,
            } => RawExpr::Cast {
                span: *span,
                is_try: *is_try,
                expr: Box::new(expr.project_column_ref(f)),
                dest_type: dest_type.clone(),
            },
            RawExpr::FunctionCall {
                span,
                name,
                params,
                args,
            } => RawExpr::FunctionCall {
                span: *span,
                name: name.clone(),
                params: params.clone(),
                args: args.iter().map(|expr| expr.project_column_ref(f)).collect(),
            },
            RawExpr::LambdaFunctionCall {
                span,
                name,
                args,
                lambda_expr,
                lambda_display,
                return_type,
            } => RawExpr::LambdaFunctionCall {
                span: *span,
                name: name.clone(),
                args: args.iter().map(|expr| expr.project_column_ref(f)).collect(),
                lambda_expr: lambda_expr.clone(),
                lambda_display: lambda_display.clone(),
                return_type: return_type.clone(),
            },
        }
    }
}

impl<Index: ColumnIndex> Expr<Index> {
    pub fn span(&self) -> Span {
        match self {
            Expr::Constant { span, .. } => *span,
            Expr::ColumnRef { span, .. } => *span,
            Expr::Cast { span, .. } => *span,
            Expr::FunctionCall { span, .. } => *span,
            Expr::LambdaFunctionCall { span, .. } => *span,
        }
    }

    pub fn runtime_filter_supported_types(&self) -> bool {
        self.data_type().remove_nullable().is_number()
            || self.data_type().remove_nullable().is_string()
            || self.data_type().remove_nullable().is_date()
    }

    pub fn data_type(&self) -> &DataType {
        match self {
            Expr::Constant { data_type, .. } => data_type,
            Expr::ColumnRef { data_type, .. } => data_type,
            Expr::Cast { dest_type, .. } => dest_type,
            Expr::FunctionCall { return_type, .. } => return_type,
            Expr::LambdaFunctionCall { return_type, .. } => return_type,
        }
    }

    pub fn column_refs(&self) -> HashMap<Index, DataType> {
        struct ColumnRef<I>(HashMap<I, DataType>);
        impl<I: ColumnIndex> ExprVisitor<I> for ColumnRef<I> {
            fn enter_column_ref(&mut self, expr: &Expr<I>) -> Result<Option<Expr<I>>, Self::Error> {
                let Expr::ColumnRef { id, data_type, .. } = expr else {
                    unreachable!()
                };
                self.0.insert(id.clone(), data_type.clone());
                Ok(None)
            }
        }

        let mut visitor = ColumnRef(HashMap::new());
        visit_expr(self, &mut visitor).unwrap();
        visitor.0
    }

    pub fn project_column_ref<ToIndex: ColumnIndex>(
        &self,
        f: impl Fn(&Index) -> ToIndex + Copy,
    ) -> Expr<ToIndex> {
        match self {
            Expr::Constant {
                span,
                scalar,
                data_type,
            } => Expr::Constant {
                span: *span,
                scalar: scalar.clone(),
                data_type: data_type.clone(),
            },
            Expr::ColumnRef {
                span,
                id,
                data_type,
                display_name,
            } => Expr::ColumnRef {
                span: *span,
                id: f(id),
                data_type: data_type.clone(),
                display_name: display_name.clone(),
            },
            Expr::Cast {
                span,
                is_try,
                expr,
                dest_type,
            } => Expr::Cast {
                span: *span,
                is_try: *is_try,
                expr: Box::new(expr.project_column_ref(f)),
                dest_type: dest_type.clone(),
            },
            Expr::FunctionCall {
                span,
                id,
                function,
                generics,
                args,
                return_type,
            } => Expr::FunctionCall {
                span: *span,
                id: id.clone(),
                function: function.clone(),
                generics: generics.clone(),
                args: args.iter().map(|expr| expr.project_column_ref(f)).collect(),
                return_type: return_type.clone(),
            },
            Expr::LambdaFunctionCall {
                span,
                name,
                args,
                lambda_expr,
                lambda_display,
                return_type,
            } => Expr::LambdaFunctionCall {
                span: *span,
                name: name.clone(),
                args: args.iter().map(|expr| expr.project_column_ref(f)).collect(),
                lambda_expr: lambda_expr.clone(),
                lambda_display: lambda_display.clone(),
                return_type: return_type.clone(),
            },
        }
    }

    pub fn fill_const_column(&self, consts: &HashMap<Index, Scalar>) -> Expr<Index> {
        struct FillConst<'a, Index>(&'a HashMap<Index, Scalar>);
        impl<Index: ColumnIndex> ExprVisitor<Index> for FillConst<'_, Index> {
            fn enter_column_ref(
                &mut self,
                expr: &Expr<Index>,
            ) -> Result<Option<Expr<Index>>, Self::Error> {
                let Expr::ColumnRef {
                    span,
                    id,
                    data_type,
                    ..
                } = expr
                else {
                    unreachable!()
                };
                Ok(self.0.get(id).map(|v| Expr::Constant {
                    span: *span,
                    scalar: v.clone(),
                    data_type: data_type.clone(),
                }))
            }
        }

        match visit_expr(self, &mut FillConst(consts)).unwrap() {
            Some(expr) => expr,
            None => self.clone(),
        }
    }

    pub fn as_remote_expr(&self) -> RemoteExpr<Index> {
        match self {
            Expr::Constant {
                span,
                scalar,
                data_type,
            } => RemoteExpr::Constant {
                span: *span,
                scalar: scalar.clone(),
                data_type: data_type.clone(),
            },
            Expr::ColumnRef {
                span,
                id,
                data_type,
                display_name,
            } => RemoteExpr::ColumnRef {
                span: *span,
                id: id.clone(),
                data_type: data_type.clone(),
                display_name: display_name.clone(),
            },
            Expr::Cast {
                span,
                is_try,
                expr,
                dest_type,
            } => RemoteExpr::Cast {
                span: *span,
                is_try: *is_try,
                expr: Box::new(expr.as_remote_expr()),
                dest_type: dest_type.clone(),
            },
            Expr::FunctionCall {
                span,
                id,
                function: _,
                generics,
                args,
                return_type,
            } => RemoteExpr::FunctionCall {
                span: *span,
                id: id.clone(),
                generics: generics.clone(),
                args: args.iter().map(Expr::as_remote_expr).collect(),
                return_type: return_type.clone(),
            },
            Expr::LambdaFunctionCall {
                span,
                name,
                args,
                lambda_expr,
                lambda_display,
                return_type,
            } => RemoteExpr::LambdaFunctionCall {
                span: *span,
                name: name.clone(),
                args: args.iter().map(Expr::as_remote_expr).collect(),
                lambda_expr: lambda_expr.clone(),
                lambda_display: lambda_display.clone(),
                return_type: return_type.clone(),
            },
        }
    }

    pub fn is_deterministic(&self, registry: &FunctionRegistry) -> bool {
        struct Deterministic<'a> {
            non_deterministic: bool,
            registry: &'a FunctionRegistry,
        }

        impl<I: ColumnIndex> ExprVisitor<I> for Deterministic<'_> {
            fn enter_function_call(
                &mut self,
                expr: &Expr<I>,
            ) -> Result<Option<Expr<I>>, Self::Error> {
                if self.non_deterministic {
                    return Ok(None);
                }

                let Expr::FunctionCall { function, .. } = expr else {
                    unreachable!()
                };

                if self
                    .registry
                    .get_property(&function.signature.name)
                    .unwrap()
                    .non_deterministic
                {
                    self.non_deterministic = true;
                    Ok(None)
                } else {
                    Self::visit_function_call(expr, self)
                }
            }

            fn enter_lambda_function_call(
                &mut self,
                expr: &Expr<I>,
            ) -> Result<Option<Expr<I>>, Self::Error> {
                if self.non_deterministic {
                    Ok(None)
                } else {
                    Self::visit_lambda_function_call(expr, self)
                }
            }
        }

        let mut visitor = Deterministic {
            non_deterministic: false,
            registry,
        };
        visit_expr(self, &mut visitor).unwrap();
        !visitor.non_deterministic
    }

    pub fn contains_column_ref(&self) -> bool {
        struct AnyColumnRef(bool);
        impl<I: ColumnIndex> ExprVisitor<I> for AnyColumnRef {
            fn enter_column_ref(&mut self, expr: &Expr<I>) -> Result<Option<Expr<I>>, Self::Error> {
                debug_assert!(expr.is_column_ref());
                self.0 = true;
                Ok(None)
            }

            fn enter_function_call(
                &mut self,
                expr: &Expr<I>,
            ) -> Result<Option<Expr<I>>, Self::Error> {
                if self.0 {
                    Ok(None)
                } else {
                    Self::visit_function_call(expr, self)
                }
            }

            fn enter_lambda_function_call(
                &mut self,
                expr: &Expr<I>,
            ) -> Result<Option<Expr<I>>, Self::Error> {
                if self.0 {
                    Ok(None)
                } else {
                    Self::visit_lambda_function_call(expr, self)
                }
            }
        }

        let mut visitor = AnyColumnRef(false);
        visit_expr(self, &mut visitor).unwrap();
        visitor.0
    }
}

impl<Index: ColumnIndex> RemoteExpr<Index> {
    pub fn as_expr(&self, fn_registry: &FunctionRegistry) -> Expr<Index> {
        match self {
            RemoteExpr::Constant {
                span,
                scalar,
                data_type,
            } => Expr::Constant {
                span: *span,
                scalar: scalar.clone(),
                data_type: data_type.clone(),
            },
            RemoteExpr::ColumnRef {
                span,
                id,
                data_type,
                display_name,
            } => Expr::ColumnRef {
                span: *span,
                id: id.clone(),
                data_type: data_type.clone(),
                display_name: display_name.clone(),
            },
            RemoteExpr::Cast {
                span,
                is_try,
                expr,
                dest_type,
            } => Expr::Cast {
                span: *span,
                is_try: *is_try,
                expr: Box::new(expr.as_expr(fn_registry)),
                dest_type: dest_type.clone(),
            },
            RemoteExpr::FunctionCall {
                span,
                id,
                generics,
                args,
                return_type,
            } => {
                let function = fn_registry.get(id).expect("function id not found");
                Expr::FunctionCall {
                    span: *span,
                    id: id.clone(),
                    function,
                    generics: generics.clone(),
                    args: args.iter().map(|arg| arg.as_expr(fn_registry)).collect(),
                    return_type: return_type.clone(),
                }
            }
            RemoteExpr::LambdaFunctionCall {
                span,
                name,
                args,
                lambda_expr,
                lambda_display,
                return_type,
            } => Expr::LambdaFunctionCall {
                span: *span,
                name: name.clone(),
                args: args.iter().map(|arg| arg.as_expr(fn_registry)).collect(),
                lambda_expr: lambda_expr.clone(),
                lambda_display: lambda_display.clone(),
                return_type: return_type.clone(),
            },
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, EnumAsInner)]
pub enum RemoteDefaultExpr {
    RemoteExpr(RemoteExpr),
    Sequence(String),
}
