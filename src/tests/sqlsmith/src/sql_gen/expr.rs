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
use common_ast::ast::IntervalKind;
use common_ast::ast::Literal;
use common_ast::ast::MapAccessor;
use common_ast::ast::TrimWhere;
use common_expression::types::DataType;
use common_expression::types::DecimalDataType;
use common_expression::types::NumberDataType;
use ethnum::I256;
use rand::distributions::Alphanumeric;
use rand::Rng;

use crate::sql_gen::SqlGenerator;

impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn gen_expr(&mut self, ty: &DataType) -> Expr {
        match self.rng.gen_range(0..=11) {
            0..=3 => self.gen_column(ty),
            4..=6 => self.gen_scalar_value(ty),
            7..=8 => self.gen_scalar_func(ty),
            9 => self.gen_factory_scalar_func(ty),
            10 => self.gen_inner_expr(ty),
            11 => self.gen_window_func(ty),
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

    pub(crate) fn gen_scalar_value(&mut self, ty: &DataType) -> Expr {
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
                    let val_ty = &inner_tys[1];
                    let len = self.rng.gen_range(1..=3);
                    let mut kvs = Vec::with_capacity(len);
                    for _ in 0..len {
                        let key = self.gen_literal();
                        let val = self.gen_scalar_value(val_ty);
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

    fn gen_literal(&mut self) -> Literal {
        let n = self.rng.gen_range(1..=7);
        match n {
            1 => Literal::Null,
            2 => Literal::String(
                rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(7)
                    .map(char::from)
                    .collect::<String>(),
            ),
            3 => Literal::Boolean(self.rng.gen_bool(0.5)),
            4 => Literal::Decimal256 {
                value: I256::from(self.rng.gen_range(-2147483648..=2147483647)),
                precision: 39,
                scale: self.rng.gen_range(0..39),
            },
            5 => Literal::UInt64(self.rng.gen_range(0..=9223372036854775807)),
            6 => Literal::CurrentTimestamp,
            7 => Literal::Float64(self.rng.gen_range(-1.7e10..=1.7e10)),
            _ => unreachable!(),
        }
    }

    fn gen_identifier(&mut self) -> Identifier {
        Identifier::from_name(
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(5)
                .map(char::from)
                .collect::<String>(),
        )
    }

    fn gen_inner_expr(&mut self, ty: &DataType) -> Expr {
        match ty.remove_nullable() {
            DataType::Boolean => {
                match self.rng.gen_range(0..=6) {
                    0 => {
                        let inner_ty = self.gen_data_type();
                        Expr::IsNull {
                            span: None,
                            expr: Box::new(self.gen_expr(&inner_ty)),
                            not: self.rng.gen_bool(0.5),
                        }
                    }
                    1 => {
                        let left_ty = self.gen_data_type();
                        let right_ty = self.gen_data_type();
                        Expr::IsDistinctFrom {
                            span: None,
                            left: Box::new(self.gen_expr(&left_ty)),
                            right: Box::new(self.gen_expr(&right_ty)),
                            not: self.rng.gen_bool(0.5),
                        }
                    }
                    2 => {
                        let expr_ty = self.gen_data_type();
                        let len = self.rng.gen_range(1..=5);
                        let list = (0..len)
                            .map(|_| self.gen_expr(&expr_ty))
                            .collect::<Vec<_>>();
                        Expr::InList {
                            span: None,
                            expr: Box::new(self.gen_expr(&expr_ty)),
                            list,
                            not: self.rng.gen_bool(0.5),
                        }
                    }
                    3 => {
                        let expr_ty = self.gen_data_type();
                        Expr::Between {
                            span: None,
                            expr: Box::new(self.gen_expr(&expr_ty)),
                            low: Box::new(self.gen_expr(&expr_ty)),
                            high: Box::new(self.gen_expr(&expr_ty)),
                            not: self.rng.gen_bool(0.5),
                        }
                    }
                    4..=6 => {
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
                    _ => unreachable!(),
                }
            }
            DataType::String => {
                if self.rng.gen_bool(0.5) {
                    let expr_ty = DataType::String;
                    let from_ty = DataType::Number(NumberDataType::Int64);

                    let expr = self.gen_expr(&expr_ty);
                    let from_expr = self.gen_expr(&from_ty);
                    let for_expr = if self.rng.gen_bool(0.5) {
                        Some(Box::new(self.gen_expr(&from_ty)))
                    } else {
                        None
                    };
                    Expr::Substring {
                        span: None,
                        expr: Box::new(expr),
                        substring_from: Box::new(from_expr),
                        substring_for: for_expr,
                    }
                } else {
                    let expr_ty = DataType::String;
                    let expr = self.gen_expr(&expr_ty);
                    let trim_where_expr = if self.rng.gen_bool(0.5) {
                        let trim_where = match self.rng.gen_range(0..=2) {
                            0 => TrimWhere::Both,
                            1 => TrimWhere::Leading,
                            2 => TrimWhere::Trailing,
                            _ => unreachable!(),
                        };
                        let where_expr = self.gen_expr(&expr_ty);
                        Some((trim_where, Box::new(where_expr)))
                    } else {
                        None
                    };
                    Expr::Trim {
                        span: None,
                        expr: Box::new(expr),
                        trim_where: trim_where_expr,
                    }
                }
            }
            DataType::Date | DataType::Timestamp => {
                let unit = match self.rng.gen_range(0..=6) {
                    0 => IntervalKind::Year,
                    1 => IntervalKind::Quarter,
                    2 => IntervalKind::Month,
                    3 => IntervalKind::Day,
                    4 => IntervalKind::Hour,
                    5 => IntervalKind::Minute,
                    6 => IntervalKind::Second,
                    _ => unreachable!(),
                };
                let interval_ty = DataType::Number(NumberDataType::Int64);
                let date_ty = if self.rng.gen_bool(0.5) {
                    DataType::Date
                } else {
                    DataType::Timestamp
                };
                let interval_expr = self.gen_expr(&interval_ty);
                let date_expr = self.gen_expr(&date_ty);

                match self.rng.gen_range(0..2) {
                    0 => Expr::DateAdd {
                        span: None,
                        unit,
                        interval: Box::new(interval_expr),
                        date: Box::new(date_expr),
                    },
                    1 => Expr::DateSub {
                        span: None,
                        unit,
                        interval: Box::new(interval_expr),
                        date: Box::new(date_expr),
                    },
                    2 => Expr::DateTrunc {
                        span: None,
                        unit,
                        date: Box::new(date_expr),
                    },
                    _ => unreachable!(),
                }
            }
            DataType::Variant => {
                let mut expr = self.gen_expr(ty);
                let len = self.rng.gen_range(1..=3);
                for _ in 0..len {
                    let accessor = match self.rng.gen_range(0..=3) {
                        0 => MapAccessor::Bracket {
                            key: Box::new(self.gen_expr(&DataType::Number(NumberDataType::UInt8))),
                        },
                        1 => {
                            let key = self.gen_identifier();
                            MapAccessor::Dot { key }
                        }
                        2 => {
                            let key = self.rng.gen_range(0..=10);
                            MapAccessor::DotNumber { key }
                        }
                        3 => {
                            let key = self.gen_identifier();
                            MapAccessor::Colon { key }
                        }
                        _ => unreachable!(),
                    };
                    expr = Expr::MapAccess {
                        span: None,
                        expr: Box::new(expr.clone()),
                        accessor,
                    };
                }
                expr
            }
            _ => {
                // no suitable expr exist, generate scalar value instead
                self.gen_scalar_value(ty)
            }
        }
    }
}
