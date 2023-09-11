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

use common_ast::ast::BinaryOperator;
use common_ast::ast::ColumnID;
use common_ast::ast::ColumnPosition;
use common_ast::ast::Expr;
use common_ast::ast::Identifier;
use common_ast::ast::Literal;
use common_expression::types::DataType;
use common_expression::types::DecimalDataType;
use common_expression::types::NumberDataType;
use ethnum::I256;
use rand::distributions::Alphanumeric;
use rand::Rng;

use crate::sql_gen::SqlGenerator;

impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn gen_expr(&mut self, ty: &DataType) -> Expr {
        match self.rng.gen_range(0..=9) {
            0..=3 => self.gen_column(ty),
            4..=6 => self.gen_scalar_value(ty),
            7..=8 => self.gen_scalar_func(ty),
            9 => self.gen_binary_op(ty),
            // TODO other exprs
            _ => unreachable!(),
        }
    }

    fn gen_column(&mut self, ty: &DataType) -> Expr {
        for bound_column in &self.bound_columns {
            if bound_column.data_type == *ty {
                let column = if self.rng.gen_bool(0.8) {
                    let name = Identifier::from_name(bound_column.name.clone());
                    ColumnID::Name(name)
                } else {
                    ColumnID::Position(ColumnPosition::create(bound_column.index, None))
                };
                let table = if self.is_join || self.rng.gen_bool(0.2) {
                    Some(Identifier::from_name(bound_column.table_name.clone()))
                } else {
                    None
                };
                return Expr::ColumnRef {
                    span: None,
                    // TODO
                    database: None,
                    table,
                    column,
                };
            }
        }
        // column does not exist, generate a scalar value instead
        self.gen_scalar_value(ty)
    }

    fn gen_scalar_value(&mut self, ty: &DataType) -> Expr {
        match ty {
            DataType::Null => Expr::Literal {
                span: None,
                lit: Literal::Null,
            },
            DataType::EmptyArray => Expr::Array {
                span: None,
                exprs: vec![],
            },
            DataType::EmptyMap => Expr::Map {
                span: None,
                kvs: vec![],
            },
            DataType::Boolean => Expr::Literal {
                span: None,
                lit: Literal::Boolean(self.rng.gen_bool(0.5)),
            },
            DataType::String => {
                let v = self
                    .rng
                    .sample_iter(&Alphanumeric)
                    .take(5)
                    .map(u8::from)
                    .collect::<Vec<_>>();
                Expr::Literal {
                    span: None,
                    lit: Literal::String(unsafe { String::from_utf8_unchecked(v) }),
                }
            }
            DataType::Number(num_type) => match num_type {
                NumberDataType::UInt8 => Expr::Literal {
                    span: None,
                    lit: Literal::UInt64(self.rng.gen_range(0..=255)),
                },
                NumberDataType::UInt16 => Expr::Literal {
                    span: None,
                    lit: Literal::UInt64(self.rng.gen_range(0..=65535)),
                },
                NumberDataType::UInt32 => Expr::Literal {
                    span: None,
                    lit: Literal::UInt64(self.rng.gen_range(0..=4294967295)),
                },
                NumberDataType::UInt64 => Expr::Literal {
                    span: None,
                    lit: Literal::UInt64(self.rng.gen_range(0..=18446744073709551615)),
                },
                NumberDataType::Int8 => Expr::Literal {
                    span: None,
                    lit: Literal::UInt64(self.rng.gen_range(0..=127)),
                },
                NumberDataType::Int16 => Expr::Literal {
                    span: None,
                    lit: Literal::UInt64(self.rng.gen_range(0..=32767)),
                },
                NumberDataType::Int32 => Expr::Literal {
                    span: None,
                    lit: Literal::UInt64(self.rng.gen_range(0..=2147483647)),
                },
                NumberDataType::Int64 => Expr::Literal {
                    span: None,
                    lit: Literal::UInt64(self.rng.gen_range(0..=9223372036854775807)),
                },
                NumberDataType::Float32 => Expr::Literal {
                    span: None,
                    lit: Literal::Float64(self.rng.gen_range(-3.4e5..=3.4e5)),
                },
                NumberDataType::Float64 => Expr::Literal {
                    span: None,
                    lit: Literal::Float64(self.rng.gen_range(-1.7e10..=1.7e10)),
                },
            },
            DataType::Decimal(decimal_type) => match decimal_type {
                DecimalDataType::Decimal128(size) => Expr::Literal {
                    span: None,
                    lit: Literal::Decimal256 {
                        value: I256::from(self.rng.gen_range(-2147483648..=2147483647)),
                        precision: size.precision,
                        scale: size.scale,
                    },
                },
                DecimalDataType::Decimal256(size) => Expr::Literal {
                    span: None,
                    lit: Literal::Decimal256 {
                        value: I256::from(self.rng.gen_range(-2147483648..=2147483647)),
                        precision: size.precision,
                        scale: size.scale,
                    },
                },
            },
            DataType::Date => {
                let arg = Expr::Literal {
                    span: None,
                    lit: Literal::UInt64(self.rng.gen_range(0..=1000000)),
                };
                Expr::FunctionCall {
                    span: None,
                    distinct: false,
                    name: Identifier::from_name("to_date".to_string()),
                    args: vec![arg],
                    params: vec![],
                    window: None,
                    lambda: None,
                }
            }
            DataType::Timestamp => {
                let arg = Expr::Literal {
                    span: None,
                    lit: Literal::UInt64(self.rng.gen_range(0..=10000000000000)),
                };
                Expr::FunctionCall {
                    span: None,
                    distinct: false,
                    name: Identifier::from_name("to_timestamp".to_string()),
                    args: vec![arg],
                    params: vec![],
                    window: None,
                    lambda: None,
                }
            }
            DataType::Nullable(box inner_ty) => {
                if self.rng.gen_bool(0.5) {
                    Expr::Literal {
                        span: None,
                        lit: Literal::Null,
                    }
                } else {
                    self.gen_scalar_value(inner_ty)
                }
            }
            DataType::Array(box inner_ty) => {
                let len = self.rng.gen_range(1..=3);
                let mut exprs = Vec::with_capacity(len);
                for _ in 0..len {
                    exprs.push(self.gen_expr(inner_ty));
                }
                Expr::Array { span: None, exprs }
            }
            DataType::Map(box inner_ty) => {
                if let DataType::Tuple(inner_tys) = inner_ty {
                    let key_ty = &inner_tys[0];
                    let val_ty = &inner_tys[1];
                    let len = self.rng.gen_range(1..=3);
                    let mut kvs = Vec::with_capacity(len);
                    for _ in 0..len {
                        let key = self.gen_expr(key_ty);
                        let val = self.gen_expr(val_ty);
                        kvs.push((key, val));
                    }
                    Expr::Map { span: None, kvs }
                } else {
                    unreachable!()
                }
            }
            DataType::Tuple(inner_tys) => {
                let mut exprs = Vec::with_capacity(inner_tys.len());
                for inner_ty in inner_tys {
                    let expr = self.gen_expr(inner_ty);
                    exprs.push(expr);
                }
                Expr::Tuple { span: None, exprs }
            }
            DataType::Bitmap => {
                let arg = Expr::Literal {
                    span: None,
                    lit: Literal::UInt64(self.rng.gen_range(0..=1024)),
                };
                Expr::FunctionCall {
                    span: None,
                    distinct: false,
                    name: Identifier::from_name("to_bitmap".to_string()),
                    args: vec![arg],
                    params: vec![],
                    window: None,
                    lambda: None,
                }
            }
            DataType::Variant => {
                let val = jsonb::rand_value();
                let arg = Expr::Literal {
                    span: None,
                    lit: Literal::String(format!("{}", val)),
                };
                Expr::FunctionCall {
                    span: None,
                    distinct: false,
                    name: Identifier::from_name("parse_json".to_string()),
                    args: vec![arg],
                    params: vec![],
                    window: None,
                    lambda: None,
                }
            }
            _ => Expr::Literal {
                span: None,
                lit: Literal::Null,
            },
        }
    }

    fn gen_binary_op(&mut self, ty: &DataType) -> Expr {
        if ty.remove_nullable() != DataType::Boolean {
            return self.gen_scalar_value(ty);
        }
        let (op, left, right) = match self.rng.gen_range(0..=3) {
            0..=1 => {
                let inner_ty = self.gen_simple_data_type();
                let left = self.gen_expr(&inner_ty);
                let right = self.gen_expr(&inner_ty);
                let op = match self.rng.gen_range(0..=5) {
                    0 => BinaryOperator::Gt,
                    1 => BinaryOperator::Lt,
                    2 => BinaryOperator::Gte,
                    3 => BinaryOperator::Lte,
                    4 => BinaryOperator::Eq,
                    5 => BinaryOperator::NotEq,
                    _ => unreachable!(),
                };
                (op, left, right)
            }
            2..=3 => {
                let left = self.gen_expr(ty);
                let right = self.gen_expr(ty);
                let op = match self.rng.gen_range(0..=2) {
                    0 => BinaryOperator::And,
                    1 => BinaryOperator::Or,
                    2 => BinaryOperator::Xor,
                    _ => unreachable!(),
                };
                (op, left, right)
            }
            // TODO other binary operators
            _ => unreachable!(),
        };
        Expr::BinaryOp {
            span: None,
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    fn gen_scalar_func(&mut self, ty: &DataType) -> Expr {
        let mut indices = Vec::new();
        for (i, func_sig) in self.scalar_func_sigs.iter().enumerate() {
            if ty == &func_sig.return_type {
                indices.push(i);
            }
        }
        if indices.is_empty() {
            return self.gen_scalar_value(ty);
        }
        let idx = self.rng.gen_range(0..indices.len());
        let func_sig = unsafe { self.scalar_func_sigs.get_unchecked(idx) }.clone();

        let name = Identifier::from_name(func_sig.name.clone());
        let args = func_sig
            .args_type
            .iter()
            .map(|ty| self.gen_expr(ty))
            .collect::<Vec<_>>();
        let mut params = vec![];
        for _ in 0..self.rng.gen_range(1..4) {
            params.push(Literal::UInt64(self.rng.gen_range(0..=9223372036854775807)));
        }
        Expr::FunctionCall {
            span: None,
            distinct: false,
            name,
            args,
            params,
            window: None,
            lambda: None,
        }
    }

    pub(crate) fn gen_agg_func(&mut self, ty: &DataType) -> Expr {
        let idx = self.rng.gen_range(0..self.agg_func_names.len() - 1);
        let name = self.agg_func_names.get(idx).unwrap().clone();
        let mut args = Vec::with_capacity(1);
        let arg = self.gen_expr(ty).clone();
        args.push(arg);
        // can ignore ErrorCode::BadDataValueType
        Expr::FunctionCall {
            span: None,
            distinct: self.rng.gen_bool(0.2),
            name: Identifier::from_name(name),
            args,
            params: vec![],
            window: None,
            lambda: None,
        }
    }
}
