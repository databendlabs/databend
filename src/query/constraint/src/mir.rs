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

use z3::ast::Ast;
use z3::ast::Dynamic;
use z3::Context;
use z3::DeclKind;

use crate::declare::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MirExpr {
    Constant(MirConstant),
    Variable {
        name: String,
        data_type: MirDataType,
    },
    UnaryOperator {
        op: MirUnaryOperator,
        arg: Box<MirExpr>,
    },
    BinaryOperator {
        op: MirBinaryOperator,
        left: Box<MirExpr>,
        right: Box<MirExpr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirConstant {
    Bool(bool),
    Int(i64),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirDataType {
    Bool,
    Int,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirUnaryOperator {
    Minus,
    Not,
    IsNull,
    RemoveNullable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirBinaryOperator {
    Plus,
    Minus,
    Multiply,
    And,
    Or,
    Eq,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug, Clone, Copy)]
pub struct WrongType;

impl MirExpr {
    pub fn variables(&self) -> HashMap<String, MirDataType> {
        fn walk(expr: &MirExpr, buf: &mut HashMap<String, MirDataType>) {
            match expr {
                MirExpr::Constant { .. } => {}
                MirExpr::Variable { name, data_type } => {
                    buf.insert(name.clone(), *data_type);
                }
                MirExpr::UnaryOperator { arg, .. } => {
                    walk(arg, buf);
                }
                MirExpr::BinaryOperator { left, right, .. } => {
                    walk(left, buf);
                    walk(right, buf);
                }
            }
        }

        let mut buf = HashMap::new();
        walk(self, &mut buf);
        buf
    }

    pub fn as_z3_ast<'ctx>(
        &self,
        ctx: &'ctx Context,
        required_type: MirDataType,
    ) -> Result<Dynamic<'ctx>, WrongType> {
        let (ast, ty) = match self {
            MirExpr::Constant(c) => match c {
                MirConstant::Bool(x) => Ok((const_bool(ctx, *x), MirDataType::Bool)),
                MirConstant::Int(x) => Ok((const_int(ctx, *x), MirDataType::Int)),
                MirConstant::Null => match required_type {
                    MirDataType::Bool => Ok((null_bool(ctx), MirDataType::Bool)),
                    MirDataType::Int => Ok((null_int(ctx), MirDataType::Int)),
                },
            },
            MirExpr::Variable { name, data_type } => match data_type {
                MirDataType::Bool => Ok((var_bool(ctx, name), MirDataType::Bool)),
                MirDataType::Int => Ok((var_int(ctx, name), MirDataType::Int)),
            },
            MirExpr::UnaryOperator { op, arg } => match op {
                MirUnaryOperator::Minus => Ok((
                    unary_minus_int(ctx, &arg.as_z3_ast(ctx, MirDataType::Int)?),
                    MirDataType::Int,
                )),
                MirUnaryOperator::Not => Ok((
                    not_bool(ctx, &arg.as_z3_ast(ctx, MirDataType::Bool)?),
                    MirDataType::Bool,
                )),
                MirUnaryOperator::IsNull => match arg.as_z3_ast(ctx, MirDataType::Bool) {
                    Ok(arg) => Ok((from_bool(ctx, &is_null_bool(ctx, &arg)), MirDataType::Bool)),
                    Err(_) => match arg.as_z3_ast(ctx, MirDataType::Int) {
                        Ok(arg) => Ok((from_bool(ctx, &is_null_int(ctx, &arg)), MirDataType::Bool)),
                        Err(_) => Err(WrongType),
                    },
                },
                MirUnaryOperator::RemoveNullable => unreachable!(
                    "RemoveNullable is a hint returned by the solver, \
                        therefore it should not be used as an input"
                ),
            },
            MirExpr::BinaryOperator { op, left, right } => match op {
                MirBinaryOperator::Plus => Ok((
                    plus_int(
                        ctx,
                        &left.as_z3_ast(ctx, MirDataType::Int)?,
                        &right.as_z3_ast(ctx, MirDataType::Int)?,
                    ),
                    MirDataType::Int,
                )),
                MirBinaryOperator::Minus => Ok((
                    minus_int(
                        ctx,
                        &left.as_z3_ast(ctx, MirDataType::Int)?,
                        &right.as_z3_ast(ctx, MirDataType::Int)?,
                    ),
                    MirDataType::Int,
                )),
                MirBinaryOperator::Multiply => Ok((
                    multiply_int(
                        ctx,
                        &left.as_z3_ast(ctx, MirDataType::Int)?,
                        &right.as_z3_ast(ctx, MirDataType::Int)?,
                    ),
                    MirDataType::Int,
                )),
                MirBinaryOperator::And => Ok((
                    and_bool(
                        ctx,
                        &left.as_z3_ast(ctx, MirDataType::Bool)?,
                        &right.as_z3_ast(ctx, MirDataType::Bool)?,
                    ),
                    MirDataType::Bool,
                )),
                MirBinaryOperator::Or => Ok((
                    or_bool(
                        ctx,
                        &left.as_z3_ast(ctx, MirDataType::Bool)?,
                        &right.as_z3_ast(ctx, MirDataType::Bool)?,
                    ),
                    MirDataType::Bool,
                )),
                MirBinaryOperator::Lt => Ok((
                    lt_int(
                        ctx,
                        &left.as_z3_ast(ctx, MirDataType::Int)?,
                        &right.as_z3_ast(ctx, MirDataType::Int)?,
                    ),
                    MirDataType::Bool,
                )),
                MirBinaryOperator::Lte => Ok((
                    le_int(
                        ctx,
                        &left.as_z3_ast(ctx, MirDataType::Int)?,
                        &right.as_z3_ast(ctx, MirDataType::Int)?,
                    ),
                    MirDataType::Bool,
                )),
                MirBinaryOperator::Gt => Ok((
                    gt_int(
                        ctx,
                        &left.as_z3_ast(ctx, MirDataType::Int)?,
                        &right.as_z3_ast(ctx, MirDataType::Int)?,
                    ),
                    MirDataType::Bool,
                )),
                MirBinaryOperator::Gte => Ok((
                    ge_int(
                        ctx,
                        &left.as_z3_ast(ctx, MirDataType::Int)?,
                        &right.as_z3_ast(ctx, MirDataType::Int)?,
                    ),
                    MirDataType::Bool,
                )),
                MirBinaryOperator::Eq => match (
                    left.as_z3_ast(ctx, MirDataType::Bool),
                    right.as_z3_ast(ctx, MirDataType::Bool),
                ) {
                    (Ok(left), Ok(right)) => Ok((eq_bool(ctx, &left, &right), MirDataType::Bool)),
                    _ => match (
                        left.as_z3_ast(ctx, MirDataType::Int),
                        right.as_z3_ast(ctx, MirDataType::Int),
                    ) {
                        (Ok(left), Ok(right)) => {
                            Ok((eq_int(ctx, &left, &right), MirDataType::Bool))
                        }
                        _ => Err(WrongType),
                    },
                },
            },
        }?;

        if ty != required_type {
            return Err(WrongType);
        }

        Ok(ast)
    }

    pub fn from_z3_ast(ast: &Dynamic, variables: &HashMap<String, MirDataType>) -> Option<MirExpr> {
        let decl = ast.safe_decl().ok()?;
        match (decl.kind(), decl.name().as_str()) {
            (DeclKind::DT_CONSTRUCTOR, "TRUE_BOOL") | (DeclKind::TRUE, _) => {
                Some(MirExpr::Constant(MirConstant::Bool(true)))
            }
            (DeclKind::DT_CONSTRUCTOR, "FALSE_BOOL") | (DeclKind::FALSE, _) => {
                Some(MirExpr::Constant(MirConstant::Bool(false)))
            }
            (DeclKind::DT_CONSTRUCTOR, "NULL_BOOL") => Some(MirExpr::Constant(MirConstant::Null)),
            (DeclKind::DT_CONSTRUCTOR, "NULL_INT") => Some(MirExpr::Constant(MirConstant::Null)),
            (DeclKind::DT_CONSTRUCTOR, "JUST_INT") => {
                Self::from_z3_ast(&ast.children()[0], variables)
            }
            (DeclKind::AND, _) => Some(MirExpr::BinaryOperator {
                op: MirBinaryOperator::And,
                left: Box::new(Self::from_z3_ast(&ast.children()[0], variables)?),
                right: Box::new(Self::from_z3_ast(&ast.children()[1], variables)?),
            }),
            (DeclKind::OR, _) => Some(MirExpr::BinaryOperator {
                op: MirBinaryOperator::Or,
                left: Box::new(Self::from_z3_ast(&ast.children()[0], variables)?),
                right: Box::new(Self::from_z3_ast(&ast.children()[1], variables)?),
            }),
            (DeclKind::NOT, _) => {
                let arg = Self::from_z3_ast(&ast.children()[0], variables)?;
                match arg.clone() {
                    MirExpr::BinaryOperator { op, left, right } => {
                        let op = match op {
                            MirBinaryOperator::Gt => MirBinaryOperator::Lte,
                            MirBinaryOperator::Gte => MirBinaryOperator::Lt,
                            MirBinaryOperator::Lt => MirBinaryOperator::Gte,
                            MirBinaryOperator::Lte => MirBinaryOperator::Gt,
                            _ => {
                                return Some(MirExpr::UnaryOperator {
                                    op: MirUnaryOperator::Not,
                                    arg: Box::new(arg),
                                });
                            }
                        };
                        Some(MirExpr::BinaryOperator { op, left, right })
                    }
                    _ => Some(MirExpr::UnaryOperator {
                        op: MirUnaryOperator::Not,
                        arg: Box::new(arg),
                    }),
                }
            }
            (DeclKind::GT, _) => Some(MirExpr::BinaryOperator {
                op: MirBinaryOperator::Gt,
                left: Box::new(Self::from_z3_ast(&ast.children()[0], variables)?),
                right: Box::new(Self::from_z3_ast(&ast.children()[1], variables)?),
            }),
            (DeclKind::GE, _) => Some(MirExpr::BinaryOperator {
                op: MirBinaryOperator::Gte,
                left: Box::new(Self::from_z3_ast(&ast.children()[0], variables)?),
                right: Box::new(Self::from_z3_ast(&ast.children()[1], variables)?),
            }),
            (DeclKind::LT, _) => Some(MirExpr::BinaryOperator {
                op: MirBinaryOperator::Lt,
                left: Box::new(Self::from_z3_ast(&ast.children()[0], variables)?),
                right: Box::new(Self::from_z3_ast(&ast.children()[1], variables)?),
            }),
            (DeclKind::LE, _) => Some(MirExpr::BinaryOperator {
                op: MirBinaryOperator::Lte,
                left: Box::new(Self::from_z3_ast(&ast.children()[0], variables)?),
                right: Box::new(Self::from_z3_ast(&ast.children()[1], variables)?),
            }),
            (DeclKind::ADD, _) => {
                let left = Self::from_z3_ast(&ast.children()[0], variables)?;
                let right = Self::from_z3_ast(&ast.children()[1], variables)?;
                match right {
                    MirExpr::UnaryOperator {
                        op: MirUnaryOperator::Minus,
                        arg,
                    } => Some(MirExpr::BinaryOperator {
                        op: MirBinaryOperator::Minus,
                        left: Box::new(left),
                        right: arg,
                    }),
                    _ => Some(MirExpr::BinaryOperator {
                        op: MirBinaryOperator::Plus,
                        left: Box::new(Self::from_z3_ast(&ast.children()[0], variables)?),
                        right: Box::new(Self::from_z3_ast(&ast.children()[1], variables)?),
                    }),
                }
            }
            (DeclKind::SUB, _) => Some(MirExpr::BinaryOperator {
                op: MirBinaryOperator::Minus,
                left: Box::new(Self::from_z3_ast(&ast.children()[0], variables)?),
                right: Box::new(Self::from_z3_ast(&ast.children()[1], variables)?),
            }),
            (DeclKind::UMINUS, _) => {
                let arg = Self::from_z3_ast(&ast.children()[0], variables)?;
                match arg {
                    MirExpr::Constant(MirConstant::Int(x)) => {
                        Some(MirExpr::Constant(MirConstant::Int(-x)))
                    }
                    _ => Some(MirExpr::UnaryOperator {
                        op: MirUnaryOperator::Minus,
                        arg: Box::new(arg),
                    }),
                }
            }
            (DeclKind::MUL, _) => {
                let left = Self::from_z3_ast(&ast.children()[0], variables)?;
                let right = Self::from_z3_ast(&ast.children()[1], variables)?;
                match left {
                    MirExpr::Constant(MirConstant::Int(1)) => Some(right),
                    MirExpr::Constant(MirConstant::Int(-1)) => Some(MirExpr::UnaryOperator {
                        op: MirUnaryOperator::Minus,
                        arg: Box::new(right),
                    }),
                    _ => Some(MirExpr::BinaryOperator {
                        op: MirBinaryOperator::Multiply,
                        left: Box::new(left),
                        right: Box::new(right),
                    }),
                }
            }
            (DeclKind::EQ, _) => {
                let left = Self::from_z3_ast(&ast.children()[0], variables)?;
                let right = Self::from_z3_ast(&ast.children()[1], variables)?;
                match (left, right) {
                    (MirExpr::Constant(MirConstant::Null), other)
                    | (other, MirExpr::Constant(MirConstant::Null)) => {
                        Some(MirExpr::UnaryOperator {
                            op: MirUnaryOperator::IsNull,
                            arg: Box::new(other),
                        })
                    }
                    (left, right) => Some(MirExpr::BinaryOperator {
                        op: MirBinaryOperator::Eq,
                        left: Box::new(left),
                        right: Box::new(right),
                    }),
                }
            }
            (DeclKind::ANUM, _) => Some(MirExpr::Constant(MirConstant::Int(
                ast.as_int().unwrap().as_i64()?,
            ))),
            (DeclKind::DT_ACCESSOR, "unwrap-int") => Some(MirExpr::UnaryOperator {
                op: MirUnaryOperator::RemoveNullable,
                arg: Box::new(Self::from_z3_ast(&ast.children()[0], variables)?),
            }),
            (DeclKind::UNINTERPRETED, name) => Some(MirExpr::Variable {
                name: name.to_string(),
                data_type: variables[name],
            }),
            _ => None,
        }
    }
}
