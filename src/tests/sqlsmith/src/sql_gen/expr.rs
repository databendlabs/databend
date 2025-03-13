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

use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnPosition;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::IntervalKind;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::MapAccessor;
use databend_common_ast::ast::SubqueryModifier;
use databend_common_ast::ast::TrimWhere;
use databend_common_ast::ast::TypeName;
use databend_common_ast::ast::UnaryOperator;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::NumberDataType;
use ethnum::I256;
use rand::distributions::Alphanumeric;
use rand::Rng;

use crate::sql_gen::SqlGenerator;

impl<R: Rng> SqlGenerator<'_, R> {
    pub(crate) fn gen_expr(&mut self, ty: &DataType) -> Expr {
        // avoid generate too complex expression
        if self.expr_depth == 0 {
            // reset `expr_depth` for generate next expression
            self.expr_depth = 2;
            return self.gen_simple_expr(ty);
        }
        self.expr_depth -= 1;

        // only column, scalar value and scalar functions
        // not generate aggregate, window and lambda functions
        if self.only_scalar_expr {
            let expr = match self.rng.gen_range(0..=8) {
                0..=5 => self.gen_simple_expr(ty),
                6 => self.gen_scalar_func(ty),
                7 => self.gen_factory_scalar_func(ty),
                8 => self.gen_other_expr(ty),
                _ => unreachable!(),
            };
            self.only_scalar_expr = false;
            return expr;
        }

        match self.rng.gen_range(0..=9) {
            0..=3 => self.gen_simple_expr(ty),
            4 => self.gen_scalar_func(ty),
            5 => self.gen_factory_scalar_func(ty),
            6 => self.gen_window_func(ty),
            7 => self.gen_lambda_func(ty),
            8 => self.gen_other_expr(ty),
            9 => self.gen_cast_expr(ty),
            _ => unreachable!(),
        }
    }

    pub(crate) fn gen_simple_expr(&mut self, ty: &DataType) -> Expr {
        if self.rng.gen_bool(0.6) {
            self.gen_column(ty)
        } else {
            self.gen_scalar_value(ty)
        }
    }

    fn gen_column(&mut self, ty: &DataType) -> Expr {
        for bound_column in &self.bound_columns {
            if bound_column.data_type == *ty {
                let column = if bound_column.table_name.is_some() && self.rng.gen_bool(0.2) {
                    ColumnID::Position(ColumnPosition::create(None, bound_column.index))
                } else {
                    let name = Identifier::from_name(None, bound_column.name.clone());
                    ColumnID::Name(name)
                };
                let table = if self.is_join
                    || (bound_column.table_name.is_some() && self.rng.gen_bool(0.2))
                {
                    bound_column.table_name.clone()
                } else {
                    None
                };
                return Expr::ColumnRef {
                    span: None,
                    // TODO
                    column: ColumnRef {
                        database: None,
                        table,
                        column,
                    },
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
                value: self.gen_literal(&DataType::Null),
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
                value: self.gen_literal(&DataType::Boolean),
            },
            DataType::String => Expr::Literal {
                span: None,
                value: self.gen_literal(&DataType::String),
            },
            DataType::Number(_) => Expr::Literal {
                span: None,
                value: self.gen_literal(ty),
            },
            DataType::Decimal(_) => Expr::Literal {
                span: None,
                value: self.gen_literal(ty),
            },
            DataType::Date => {
                let arg = Expr::Literal {
                    span: None,
                    value: Literal::UInt64(self.rng.gen_range(0..=1000000)),
                };
                Expr::FunctionCall {
                    span: None,
                    func: FunctionCall {
                        distinct: false,
                        name: Identifier::from_name(None, "to_date".to_string()),
                        args: vec![arg],
                        params: vec![],
                        order_by: vec![],
                        window: None,
                        lambda: None,
                    },
                }
            }
            DataType::Timestamp => {
                let arg = Expr::Literal {
                    span: None,
                    value: Literal::UInt64(self.rng.gen_range(0..=10000000000000)),
                };
                Expr::FunctionCall {
                    span: None,
                    func: FunctionCall {
                        distinct: false,
                        name: Identifier::from_name(None, "to_timestamp".to_string()),
                        args: vec![arg],
                        params: vec![],
                        order_by: vec![],
                        window: None,
                        lambda: None,
                    },
                }
            }
            DataType::Nullable(box inner_ty) => {
                if self.rng.gen_bool(0.5) {
                    Expr::Literal {
                        span: None,
                        value: Literal::Null,
                    }
                } else {
                    self.gen_scalar_value(inner_ty)
                }
            }
            DataType::Array(box inner_ty) => {
                let len = self.rng.gen_range(1..=3);
                let mut exprs = Vec::with_capacity(len);
                for _ in 0..len {
                    exprs.push(self.gen_scalar_value(inner_ty));
                }
                Expr::Array { span: None, exprs }
            }
            DataType::Map(box inner_ty) => {
                if let DataType::Tuple(fields) = inner_ty {
                    let len = self.rng.gen_range(1..=3);
                    let mut kvs = Vec::with_capacity(len);
                    for _ in 0..len {
                        let key = self.gen_literal(&fields[0]);
                        let val = self.gen_scalar_value(&fields[1]);
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
                    let expr = self.gen_scalar_value(inner_ty);
                    exprs.push(expr);
                }
                Expr::Tuple { span: None, exprs }
            }
            DataType::Bitmap => {
                let arg = Expr::Literal {
                    span: None,
                    value: Literal::UInt64(self.rng.gen_range(0..=1024)),
                };
                Expr::FunctionCall {
                    span: None,
                    func: FunctionCall {
                        distinct: false,
                        name: Identifier::from_name(None, "to_bitmap".to_string()),
                        args: vec![arg],
                        params: vec![],
                        order_by: vec![],
                        window: None,
                        lambda: None,
                    },
                }
            }
            DataType::Variant => {
                let val = jsonb::rand_value();
                let arg = Expr::Literal {
                    span: None,
                    value: Literal::String(format!("{}", val)),
                };
                Expr::FunctionCall {
                    span: None,
                    func: FunctionCall {
                        distinct: false,
                        name: Identifier::from_name(None, "parse_json".to_string()),
                        args: vec![arg],
                        params: vec![],
                        order_by: vec![],
                        window: None,
                        lambda: None,
                    },
                }
            }
            DataType::Binary => {
                let arg = Expr::Literal {
                    span: None,
                    value: self.gen_literal(&DataType::String),
                };
                Expr::FunctionCall {
                    span: None,
                    func: FunctionCall {
                        distinct: false,
                        name: Identifier::from_name(None, "to_binary".to_string()),
                        args: vec![arg],
                        params: vec![],
                        order_by: vec![],
                        window: None,
                        lambda: None,
                    },
                }
            }
            DataType::Geometry => {
                let geo = self.gen_geometry();
                let arg = Expr::Literal {
                    span: None,
                    value: Literal::String(geo),
                };
                Expr::FunctionCall {
                    span: None,
                    func: FunctionCall {
                        distinct: false,
                        name: Identifier::from_name(None, "to_geometry".to_string()),
                        args: vec![arg],
                        params: vec![],
                        order_by: vec![],
                        window: None,
                        lambda: None,
                    },
                }
            }
            DataType::Geography => {
                let lng: f64 = self.rng.gen_range(-180.0..=180.0);
                let lat: f64 = self.rng.gen_range(-90.0..=90.0);
                let arg = Expr::Literal {
                    span: None,
                    value: Literal::String(format!("POINT({} {})", lng, lat)),
                };
                Expr::FunctionCall {
                    span: None,
                    func: FunctionCall {
                        distinct: false,
                        name: Identifier::from_name(None, "st_geographyfromewkt".to_string()),
                        args: vec![arg],
                        params: vec![],
                        order_by: vec![],
                        window: None,
                        lambda: None,
                    },
                }
            }
            _ => Expr::Literal {
                span: None,
                value: Literal::Null,
            },
        }
    }

    fn gen_literal(&mut self, ty: &DataType) -> Literal {
        match ty {
            DataType::Null => Literal::Null,
            DataType::Boolean => Literal::Boolean(self.rng.gen_bool(0.5)),
            DataType::String => Literal::String(
                rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(7)
                    .map(char::from)
                    .collect::<String>(),
            ),
            DataType::Number(num_type) => match num_type {
                NumberDataType::UInt8 => Literal::UInt64(self.rng.gen_range(0..=255)),
                NumberDataType::UInt16 => Literal::UInt64(self.rng.gen_range(0..=65535)),
                NumberDataType::UInt32 => Literal::UInt64(self.rng.gen_range(0..=4294967295)),
                NumberDataType::UInt64 => {
                    Literal::UInt64(self.rng.gen_range(0..=18446744073709551615))
                }
                NumberDataType::Int8 => Literal::UInt64(self.rng.gen_range(0..=127)),
                NumberDataType::Int16 => Literal::UInt64(self.rng.gen_range(0..=32767)),
                NumberDataType::Int32 => Literal::UInt64(self.rng.gen_range(0..=2147483647)),
                NumberDataType::Int64 => {
                    Literal::UInt64(self.rng.gen_range(0..=9223372036854775807))
                }
                NumberDataType::Float32 => Literal::Float64(self.rng.gen_range(-3.4e5..=3.4e5)),
                NumberDataType::Float64 => Literal::Float64(self.rng.gen_range(-1.7e10..=1.7e10)),
            },
            DataType::Decimal(decimal_type) => match decimal_type {
                DecimalDataType::Decimal128(size) => Literal::Decimal256 {
                    value: I256::from(self.rng.gen_range(-2147483648..=2147483647)),
                    precision: size.precision,
                    scale: size.scale,
                },
                DecimalDataType::Decimal256(size) => Literal::Decimal256 {
                    value: I256::from(self.rng.gen_range(-2147483648..=2147483647)),
                    precision: size.precision,
                    scale: size.scale,
                },
            },
            _ => Literal::Null,
        }
    }

    fn gen_identifier(&mut self) -> Identifier {
        Identifier::from_name(
            None,
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(5)
                .map(char::from)
                .collect::<String>(),
        )
    }

    fn gen_other_expr(&mut self, ty: &DataType) -> Expr {
        match ty.remove_nullable() {
            DataType::Boolean => match self.rng.gen_range(0..=9) {
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
                4..=6 => self.gen_binary_expr(),
                7 => {
                    let not = self.rng.gen_bool(0.5);
                    let (subquery, _) = self.gen_subquery(false);
                    Expr::Exists {
                        span: None,
                        not,
                        subquery: Box::new(subquery),
                    }
                }
                8 => {
                    let modifier = match self.rng.gen_range(0..=3) {
                        0 => None,
                        1 => Some(SubqueryModifier::Any),
                        2 => Some(SubqueryModifier::All),
                        3 => Some(SubqueryModifier::Some),
                        _ => unreachable!(),
                    };
                    let (subquery, _) = self.gen_subquery(true);
                    Expr::Subquery {
                        span: None,
                        modifier,
                        subquery: Box::new(subquery),
                    }
                }
                9 => {
                    let expr_ty = self.gen_simple_data_type();
                    let expr = self.gen_expr(&expr_ty);
                    let not = self.rng.gen_bool(0.5);
                    let (subquery, _) = self.gen_subquery(true);
                    Expr::InSubquery {
                        span: None,
                        expr: Box::new(expr),
                        subquery: Box::new(subquery),
                        not,
                    }
                }
                _ => unreachable!(),
            },
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
            DataType::Number(_) => match self.rng.gen_range(0..=3) {
                0 => {
                    let expr_ty = if self.rng.gen_bool(0.5) {
                        DataType::Date
                    } else {
                        DataType::Timestamp
                    };
                    let expr = self.gen_expr(&expr_ty);
                    let kind = match self.rng.gen_range(0..=10) {
                        0 => IntervalKind::Year,
                        1 => IntervalKind::Quarter,
                        2 => IntervalKind::Month,
                        3 => IntervalKind::Day,
                        4 => IntervalKind::Hour,
                        5 => IntervalKind::Minute,
                        6 => IntervalKind::Second,
                        7 => IntervalKind::Doy,
                        8 => IntervalKind::Dow,
                        9 => IntervalKind::Week,
                        10 => IntervalKind::Epoch,
                        _ => unreachable!(),
                    };
                    Expr::Extract {
                        span: None,
                        kind,
                        expr: Box::new(expr),
                    }
                }
                1 => {
                    let expr_ty = DataType::String;
                    let substr_expr = self.gen_expr(&expr_ty);
                    let str_expr = self.gen_expr(&expr_ty);
                    Expr::Position {
                        span: None,
                        substr_expr: Box::new(substr_expr),
                        str_expr: Box::new(str_expr),
                    }
                }
                2 => Expr::CountAll {
                    span: None,
                    window: None,
                },
                3 => {
                    let expr_ty = self.gen_all_number_data_type();
                    let expr = self.gen_expr(&expr_ty);
                    let op = match self.rng.gen_range(0..=7) {
                        0 => UnaryOperator::Plus,
                        1 => UnaryOperator::Minus,
                        2 => UnaryOperator::Not,
                        3 => UnaryOperator::Factorial,
                        4 => UnaryOperator::SquareRoot,
                        5 => UnaryOperator::CubeRoot,
                        6 => UnaryOperator::Abs,
                        7 => UnaryOperator::BitwiseNot,
                        _ => unreachable!(),
                    };
                    Expr::UnaryOp {
                        span: None,
                        op,
                        expr: Box::new(expr),
                    }
                }
                _ => unreachable!(),
            },
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

                match self.rng.gen_range(0..=2) {
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
                    let accessor = match self.rng.gen_range(0..=2) {
                        0 => MapAccessor::Bracket {
                            key: Box::new(self.gen_expr(&DataType::Number(NumberDataType::UInt8))),
                        },
                        1 => {
                            let key = self.rng.gen_range(0..=10);
                            MapAccessor::DotNumber { key }
                        }
                        2 => {
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
            DataType::Binary => {
                let arg_ty = DataType::String;
                let arg = self.gen_expr(&arg_ty);
                Expr::FunctionCall {
                    span: None,
                    func: FunctionCall {
                        distinct: false,
                        name: Identifier::from_name(None, "to_binary"),
                        args: vec![arg],
                        params: vec![],
                        order_by: vec![],
                        window: None,
                        lambda: None,
                    },
                }
            }
            DataType::Geometry => {
                let (func_name, args) = match self.rng.gen_range(0..=10) {
                    0 => {
                        let arg_ty = DataType::Number(NumberDataType::Float64);
                        let x = self.gen_expr(&arg_ty);
                        let y = self.gen_expr(&arg_ty);
                        ("st_makegeompoint", vec![x, y])
                    }
                    _ => {
                        let geo = self.gen_geometry();
                        let arg0 = Expr::Literal {
                            span: None,
                            value: Literal::String(geo),
                        };
                        let args = if self.rng.gen_bool(0.5) {
                            let arg1_ty = DataType::Number(NumberDataType::Int32);
                            let arg1 = self.gen_expr(&arg1_ty);
                            vec![arg0, arg1]
                        } else {
                            vec![arg0]
                        };
                        ("st_geometryfromwkt", args)
                    }
                };
                Expr::FunctionCall {
                    span: None,
                    func: FunctionCall {
                        distinct: false,
                        name: Identifier::from_name(None, func_name),
                        args,
                        params: vec![],
                        order_by: vec![],
                        window: None,
                        lambda: None,
                    },
                }
            }
            _ => {
                if self.rng.gen_bool(0.3) {
                    let cond_ty = DataType::Boolean;
                    let len = self.rng.gen_range(1..=3);
                    let mut conditions = Vec::with_capacity(len);
                    let mut results = Vec::with_capacity(len);
                    for _ in 0..len {
                        conditions.push(self.gen_expr(&cond_ty));
                        results.push(self.gen_expr(ty));
                    }
                    let else_result = if self.rng.gen_bool(0.5) {
                        Some(Box::new(self.gen_expr(ty)))
                    } else {
                        None
                    };
                    Expr::Case {
                        span: None,
                        operand: None,
                        conditions,
                        results,
                        else_result,
                    }
                } else {
                    // no suitable expr exist, generate scalar value instead
                    self.gen_scalar_value(ty)
                }
            }
        }
    }

    pub(crate) fn gen_point(&mut self) -> String {
        let x: f64 = self.rng.gen_range(-1.7e10..=1.7e10);
        let y: f64 = self.rng.gen_range(-1.7e10..=1.7e10);
        format!("{} {}", x, y)
    }

    pub(crate) fn gen_points(&mut self) -> String {
        let mut points = vec![];
        for _ in 0..self.rng.gen_range(1..=6) {
            let point = format!("{}", self.gen_point());
            points.push(point);
        }
        points.join(", ")
    }

    pub(crate) fn gen_simple_geometry(&mut self) -> String {
        match self.rng.gen_range(0..=5) {
            0 => match self.rng.gen_range(0..=10) {
                0 => "POINT EMPTY".to_string(),
                _ => {
                    format!("POINT({})", self.gen_point())
                }
            },
            1 => match self.rng.gen_range(0..=10) {
                0 => "MULTIPOINT EMPTY".to_string(),
                _ => {
                    let mut points = vec![];
                    for _ in 0..self.rng.gen_range(1..=6) {
                        let point = format!("({})", self.gen_point());
                        points.push(point);
                    }
                    format!("MULTIPOINT({})", points.join(", "))
                }
            },
            2 => match self.rng.gen_range(0..=10) {
                0 => "LINESTRING EMPTY".to_string(),
                _ => {
                    let points = self.gen_points();
                    format!("LINESTRING({})", points)
                }
            },
            3 => match self.rng.gen_range(0..=10) {
                0 => "MULTILINESTRING EMPTY".to_string(),
                _ => {
                    let mut lines = vec![];
                    for _ in 0..self.rng.gen_range(1..=6) {
                        let points = self.gen_points();
                        let line = format!("({})", points);
                        lines.push(line);
                    }
                    format!("MULTILINESTRING({})", lines.join(", "))
                }
            },
            4 => match self.rng.gen_range(0..=10) {
                0 => "POLYGON EMPTY".to_string(),
                _ => {
                    let mut polygons = vec![];
                    for _ in 0..self.rng.gen_range(1..=6) {
                        let points = self.gen_points();
                        let polygon = format!("({})", points);
                        polygons.push(polygon);
                    }
                    format!("POLYGON({})", polygons.join(", "))
                }
            },
            5 => match self.rng.gen_range(0..=10) {
                0 => "MULTIPOLYGON EMPTY".to_string(),
                _ => {
                    let mut polygons = vec![];
                    for _ in 0..self.rng.gen_range(1..=6) {
                        let points = self.gen_points();
                        let polygon = format!("(({}))", points);
                        polygons.push(polygon);
                    }
                    format!("MULTIPOLYGON({})", polygons.join(", "))
                }
            },
            _ => unreachable!(),
        }
    }

    pub(crate) fn gen_geometry(&mut self) -> String {
        let geo = match self.rng.gen_range(0..=8) {
            0 => {
                let mut geos = vec![];
                for _ in 0..self.rng.gen_range(1..=4) {
                    geos.push(self.gen_simple_geometry());
                }
                format!("GEOMETRYCOLLECTION({})", geos.join(", "))
            }
            _ => self.gen_simple_geometry(),
        };
        if self.rng.gen_bool(0.4) {
            let srid = self.rng.gen_range(1..=10000);
            format!("SRID={};{}", srid, geo)
        } else {
            geo
        }
    }

    pub(crate) fn gen_binary_expr(&mut self) -> Expr {
        let (op, left, right) = match self.rng.gen_range(0..=3) {
            0..=1 => {
                let inner_ty = self.gen_simple_data_type();
                self.expr_depth = 0;
                let left = self.gen_expr(&inner_ty);
                self.expr_depth = 1;
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
                self.expr_depth = 0;
                let left = self.gen_expr(&DataType::Boolean);
                self.expr_depth = 1;
                let right = self.gen_expr(&DataType::Boolean);
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

    fn gen_cast_expr(&mut self, ty: &DataType) -> Expr {
        // can't cast to nested types
        if matches!(
            ty.remove_nullable(),
            DataType::Null
                | DataType::EmptyArray
                | DataType::EmptyMap
                | DataType::Array(_)
                | DataType::Map(_)
                | DataType::Tuple(_)
                | DataType::Generic(_)
        ) {
            return self.gen_other_expr(ty);
        }

        let source_type = self.gen_data_type();
        let source_expr = self.gen_expr(&source_type);
        let target_type = convert_to_type_name(ty);

        if self.rng.gen_bool(0.5) {
            Expr::Cast {
                span: None,
                expr: Box::new(source_expr),
                target_type,
                pg_style: self.rng.gen_bool(0.5),
            }
        } else {
            Expr::TryCast {
                span: None,
                expr: Box::new(source_expr),
                target_type,
            }
        }
    }
}

fn convert_to_type_name(ty: &DataType) -> TypeName {
    match ty {
        DataType::Boolean => TypeName::Boolean,
        DataType::Number(NumberDataType::UInt8) => TypeName::UInt8,
        DataType::Number(NumberDataType::UInt16) => TypeName::UInt16,
        DataType::Number(NumberDataType::UInt32) => TypeName::UInt32,
        DataType::Number(NumberDataType::UInt64) => TypeName::UInt64,
        DataType::Number(NumberDataType::Int8) => TypeName::Int8,
        DataType::Number(NumberDataType::Int16) => TypeName::Int16,
        DataType::Number(NumberDataType::Int32) => TypeName::Int32,
        DataType::Number(NumberDataType::Int64) => TypeName::Int64,
        DataType::Number(NumberDataType::Float32) => TypeName::Float32,
        DataType::Number(NumberDataType::Float64) => TypeName::Float64,
        DataType::Decimal(DecimalDataType::Decimal128(size)) => TypeName::Decimal {
            precision: size.precision,
            scale: size.scale,
        },
        DataType::Decimal(DecimalDataType::Decimal256(size)) => TypeName::Decimal {
            precision: size.precision,
            scale: size.scale,
        },
        DataType::Date => TypeName::Date,
        DataType::Timestamp => TypeName::Timestamp,
        DataType::String => TypeName::String,
        DataType::Bitmap => TypeName::Bitmap,
        DataType::Variant => TypeName::Variant,
        DataType::Binary => TypeName::Binary,
        DataType::Geometry => TypeName::Geometry,
        DataType::Nullable(box inner_ty) => {
            TypeName::Nullable(Box::new(convert_to_type_name(inner_ty)))
        }
        DataType::Array(box inner_ty) => TypeName::Array(Box::new(convert_to_type_name(inner_ty))),
        DataType::Map(box inner_ty) => match inner_ty {
            DataType::Tuple(inner_tys) => TypeName::Map {
                key_type: Box::new(convert_to_type_name(&inner_tys[0])),
                val_type: Box::new(convert_to_type_name(&inner_tys[1])),
            },
            _ => unreachable!(),
        },
        DataType::Tuple(inner_tys) => TypeName::Tuple {
            fields_name: None,
            fields_type: inner_tys
                .iter()
                .map(convert_to_type_name)
                .collect::<Vec<_>>(),
        },
        _ => TypeName::String,
    }
}
