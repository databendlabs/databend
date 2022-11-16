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
use std::collections::HashSet;
use std::sync::Arc;

use educe::Educe;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

use crate::function::Function;
use crate::function::FunctionID;
use crate::function::FunctionRegistry;
use crate::types::DataType;
use crate::values::Scalar;

pub type Span = Option<std::ops::Range<usize>>;

#[derive(Debug, Clone)]
pub enum RawExpr {
    Literal {
        span: Span,
        lit: Literal,
    },
    ColumnRef {
        span: Span,
        id: usize,
        data_type: DataType,
    },
    Cast {
        span: Span,
        is_try: bool,
        expr: Box<RawExpr>,
        dest_type: DataType,
    },
    FunctionCall {
        span: Span,
        name: String,
        params: Vec<usize>,
        args: Vec<RawExpr>,
    },
}

#[derive(Debug, Clone, Educe, EnumAsInner)]
#[educe(PartialEq)]
pub enum Expr {
    Constant {
        span: Span,
        scalar: Scalar,
        data_type: DataType,
    },
    ColumnRef {
        span: Span,
        id: usize,
        data_type: DataType,
    },
    Cast {
        span: Span,
        is_try: bool,
        expr: Box<Expr>,
        dest_type: DataType,
    },
    FunctionCall {
        span: Span,
        id: FunctionID,
        #[educe(PartialEq(ignore))]
        function: Arc<Function>,
        generics: Vec<DataType>,
        args: Vec<Expr>,
        return_type: DataType,
    },
}

/// Serializable expression used to share executable expression between nodes.
///
/// The remote node will recover the `Arc` pointer within `FunctionCall` by looking
/// up the funciton registry with the `FunctionID`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RemoteExpr {
    Constant {
        span: Span,
        scalar: Scalar,
        data_type: DataType,
    },
    ColumnRef {
        span: Span,
        id: usize,
        data_type: DataType,
    },
    Cast {
        span: Span,
        is_try: bool,
        expr: Box<RemoteExpr>,
        dest_type: DataType,
    },
    FunctionCall {
        span: Span,
        id: FunctionID,
        generics: Vec<DataType>,
        args: Vec<RemoteExpr>,
        return_type: DataType,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Literal {
    Null,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Boolean(bool),
    String(Vec<u8>),
}

impl RawExpr {
    pub fn column_refs(&self) -> HashSet<usize> {
        fn walk(expr: &RawExpr, buf: &mut HashSet<usize>) {
            match expr {
                RawExpr::ColumnRef { id, .. } => {
                    buf.insert(*id);
                }
                RawExpr::Cast { expr, .. } => walk(expr, buf),
                RawExpr::FunctionCall { args, .. } => args.iter().for_each(|expr| walk(expr, buf)),
                RawExpr::Literal { .. } => (),
            }
        }

        let mut buf = HashSet::new();
        walk(self, &mut buf);
        buf
    }
}

impl Expr {
    pub fn data_type(&self) -> &DataType {
        match self {
            Expr::Constant { data_type, .. } => data_type,
            Expr::ColumnRef { data_type, .. } => data_type,
            Expr::Cast { dest_type, .. } => dest_type,
            Expr::FunctionCall { return_type, .. } => return_type,
        }
    }

    pub fn column_refs(&self) -> HashSet<usize> {
        fn walk(expr: &Expr, buf: &mut HashSet<usize>) {
            match expr {
                Expr::ColumnRef { id, .. } => {
                    buf.insert(*id);
                }
                Expr::Cast { expr, .. } => walk(expr, buf),
                Expr::FunctionCall { args, .. } => args.iter().for_each(|expr| walk(expr, buf)),
                Expr::Constant { .. } => (),
            }
        }

        let mut buf = HashSet::new();
        walk(self, &mut buf);
        buf
    }

    pub fn project_column_ref(&self, map: &HashMap<usize, usize>) -> Expr {
        match self {
            Expr::Constant { .. } => self.clone(),
            Expr::ColumnRef {
                span,
                id,
                data_type,
            } => Expr::ColumnRef {
                span: span.clone(),
                id: map[id],
                data_type: data_type.clone(),
            },
            Expr::Cast {
                span,
                is_try,
                expr,
                dest_type,
            } => Expr::Cast {
                span: span.clone(),
                is_try: *is_try,
                expr: Box::new(expr.project_column_ref(map)),
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
                span: span.clone(),
                id: id.clone(),
                function: function.clone(),
                generics: generics.clone(),
                args: args
                    .iter()
                    .map(|expr| expr.project_column_ref(map))
                    .collect(),
                return_type: return_type.clone(),
            },
        }
    }
}

impl RemoteExpr {
    pub fn from_expr(expr: Expr) -> Self {
        match expr {
            Expr::Constant {
                span,
                scalar,
                data_type,
            } => RemoteExpr::Constant {
                span,
                scalar,
                data_type,
            },
            Expr::ColumnRef {
                span,
                id,
                data_type,
            } => RemoteExpr::ColumnRef {
                span,
                id,
                data_type,
            },
            Expr::Cast {
                span,
                is_try,
                expr,
                dest_type,
            } => RemoteExpr::Cast {
                span,
                is_try,
                expr: Box::new(RemoteExpr::from_expr(*expr)),
                dest_type,
            },
            Expr::FunctionCall {
                span,
                id,
                function: _,
                generics,
                args,
                return_type,
            } => RemoteExpr::FunctionCall {
                span,
                id,
                generics,
                args: args.into_iter().map(RemoteExpr::from_expr).collect(),
                return_type,
            },
        }
    }

    pub fn into_expr(self, fn_registry: &FunctionRegistry) -> Option<Expr> {
        Some(match self {
            RemoteExpr::Constant {
                span,
                scalar,
                data_type,
            } => Expr::Constant {
                span,
                scalar,
                data_type,
            },
            RemoteExpr::ColumnRef {
                span,
                id,
                data_type,
            } => Expr::ColumnRef {
                span,
                id,
                data_type,
            },
            RemoteExpr::Cast {
                span,
                is_try,
                expr,
                dest_type,
            } => Expr::Cast {
                span,
                is_try,
                expr: Box::new(expr.into_expr(fn_registry)?),
                dest_type,
            },
            RemoteExpr::FunctionCall {
                span,
                id,
                generics,
                args,
                return_type,
            } => {
                let function = fn_registry.get(&id)?;
                Expr::FunctionCall {
                    span,
                    id,
                    function,
                    generics,
                    args: args
                        .into_iter()
                        .map(|arg| arg.into_expr(fn_registry))
                        .collect::<Option<_>>()?,
                    return_type,
                }
            }
        })
    }
}
