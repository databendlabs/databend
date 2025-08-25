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

use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::GroupBy;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::OrderByExpr;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SelectStmt;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::UnaryOperator;
use databend_common_ast::ast::Window;
use databend_common_ast::ast::WindowDesc;
use databend_common_ast::ast::WindowFrame;
use databend_common_ast::ast::WindowFrameBound;
use databend_common_ast::ast::WindowFrameUnits;
use databend_common_ast::Span;
use derive_visitor::Drive;
use derive_visitor::DriveMut;
use derive_visitor::Visitor;
use derive_visitor::VisitorMut;
use ethnum::I256;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use Expr::TimeSlice;

const BAD_INT64_VALUES: [i64; 31] = [
    -2,
    -1,
    0,
    1,
    2,
    3,
    7,
    10,
    100,
    255,
    256,
    257,
    1023,
    1024,
    1025,
    65535,
    65536,
    65537,
    1024 * 1024 - 1,
    1024 * 1024,
    1024 * 1024 + 1,
    i32::MIN as i64 - 1,
    i32::MIN as i64,
    i32::MIN as i64 + 1,
    i32::MAX as i64 - 1,
    i32::MAX as i64,
    i32::MAX as i64 + 1,
    i64::MIN,
    i64::MIN + 1,
    i64::MAX - 1,
    i64::MAX,
];

const BAD_FLOAT64_VALUES: [f64; 20] = [
    f64::NAN,
    f64::INFINITY,
    f64::NEG_INFINITY,
    0.,
    -0.,
    0.0001,
    0.5,
    0.9999,
    1.,
    1.0001,
    2.,
    10.0001,
    100.0001,
    1000.0001,
    1e10,
    1e20,
    f64::MIN,
    f64::MIN + f64::EPSILON,
    f64::MAX,
    f64::MAX + f64::EPSILON,
];

const SIMPLE_CAST_FUNCTIONS: [&str; 42] = [
    "to_binary",
    "to_string",
    "to_uint8",
    "to_uint16",
    "to_uint32",
    "to_uint64",
    "to_int8",
    "to_int16",
    "to_int32",
    "to_int64",
    "to_float32",
    "to_float64",
    "to_timestamp",
    "to_interval",
    "to_date",
    "to_variant",
    "to_boolean",
    "to_decimal",
    "to_bitmap",
    "to_geometry",
    "parse_json",
    "try_to_binary",
    "try_to_string",
    "try_to_uint8",
    "try_to_uint16",
    "try_to_uint32",
    "try_to_uint64",
    "try_to_int8",
    "try_to_int16",
    "try_to_int32",
    "try_to_int64",
    "try_to_float32",
    "try_to_float64",
    "try_to_timestamp",
    "try_to_interval",
    "try_to_date",
    "try_to_variant",
    "try_to_boolean",
    "try_to_decimal",
    "try_to_bitmap",
    "try_to_geometry",
    "try_parse_json",
];

#[derive(Visitor)]
#[visitor(Expr(enter), TableReference(enter))]
struct CollectorVisitor {
    column_like_map: HashMap<String, Expr>,
    column_like: Vec<(String, Expr)>,
    table_like_map: HashMap<String, TableReference>,
    table_like: Vec<(String, TableReference)>,
}

impl CollectorVisitor {
    fn new() -> Self {
        Self {
            column_like_map: HashMap::new(),
            column_like: Vec::new(),
            table_like_map: HashMap::new(),
            table_like: Vec::new(),
        }
    }

    fn enter_expr(&mut self, expr: &Expr) {
        let name = format!("{}", expr);
        if !self.column_like_map.contains_key(&name) {
            self.column_like_map.insert(name.clone(), expr.clone());
            self.column_like.push((name, expr.clone()));
        }
    }

    fn enter_table_reference(&mut self, table_ref: &TableReference) {
        let name = format!("{}", table_ref);
        if !self.table_like_map.contains_key(&name) {
            self.table_like_map.insert(name.clone(), table_ref.clone());
            self.table_like.push((name, table_ref.clone()));
        }
    }
}

#[derive(VisitorMut)]
#[visitor(Query(enter), SelectStmt(enter))]
struct QueryVisitor {
    rng: SmallRng,
    collector_visitor: CollectorVisitor,
}

impl QueryVisitor {
    fn new(seed: Option<u64>) -> Self {
        let rng = if let Some(seed) = seed {
            SmallRng::seed_from_u64(seed)
        } else {
            SmallRng::from_entropy()
        };
        Self {
            rng,
            collector_visitor: CollectorVisitor::new(),
        }
    }

    fn enter_query(&mut self, query: &mut Query) {
        // fuzz order by
        self.fuzz_order_by(&mut query.order_by);
    }

    fn enter_select_stmt(&mut self, select_stmt: &mut SelectStmt) {
        // fuzz select list
        self.fuzz_select_list(&mut select_stmt.select_list);
        // fuzz where selection
        self.fuzz_expr_opt(&mut select_stmt.selection);
        // fuzz group by
        self.fuzz_group_by(&mut select_stmt.group_by);
        // fuzz having
        self.fuzz_expr_opt(&mut select_stmt.having);
        // fuzz qualify
        self.fuzz_expr_opt(&mut select_stmt.qualify);
    }

    fn fuzz_select_list(&mut self, select_list: &mut Vec<SelectTarget>) {
        for target in select_list.iter_mut() {
            if let SelectTarget::AliasedExpr { expr, alias } = target {
                let fuzzed_expr = self.fuzz_expr(expr);
                *target = SelectTarget::AliasedExpr {
                    expr: Box::new(fuzzed_expr),
                    alias: alias.clone(),
                };
            }
        }
        // add random expr column
        if self.rng.gen_bool(0.3) {
            let len = self.rng.gen_range(1..5);
            for _ in 0..len {
                let expr = self.get_random_column_like();
                let new_target = SelectTarget::AliasedExpr {
                    expr: Box::new(expr),
                    alias: None,
                };
                if select_list.len() > 1 {
                    let idx = self.rng.gen_range(0..select_list.len());
                    select_list.insert(idx, new_target);
                } else {
                    select_list.push(new_target);
                }
            }
        }
    }

    fn fuzz_expr(&mut self, expr: &Expr) -> Expr {
        // random replace with a column
        if self.rng.gen_bool(0.3) {
            return self.get_random_column_like();
        }
        match expr {
            Expr::ColumnRef { span, column } => self.fuzz_column(span, column),
            Expr::Literal { span, value } => self.fuzz_literal(span, value),
            Expr::FunctionCall { span, func } => self.fuzz_func(span, func),
            Expr::IsNull { span, expr, .. } => Expr::IsNull {
                span: *span,
                expr: Box::new(self.fuzz_expr(expr)),
                not: self.rng.gen_bool(0.5),
            },
            Expr::IsDistinctFrom {
                span, left, right, ..
            } => Expr::IsDistinctFrom {
                span: *span,
                left: Box::new(self.fuzz_expr(left)),
                right: Box::new(self.fuzz_expr(right)),
                not: self.rng.gen_bool(0.5),
            },
            Expr::InList {
                span, expr, list, ..
            } => Expr::InList {
                span: *span,
                expr: Box::new(self.fuzz_expr(expr)),
                list: list.iter().map(|expr| self.fuzz_expr(expr)).collect(),
                not: self.rng.gen_bool(0.5),
            },
            Expr::InSubquery {
                span,
                expr,
                subquery,
                ..
            } => Expr::InSubquery {
                span: *span,
                expr: Box::new(self.fuzz_expr(expr)),
                subquery: subquery.clone(),
                not: self.rng.gen_bool(0.5),
            },
            Expr::Between {
                span,
                expr,
                low,
                high,
                ..
            } => Expr::Between {
                span: *span,
                expr: Box::new(self.fuzz_expr(expr)),
                low: Box::new(self.fuzz_expr(low)),
                high: Box::new(self.fuzz_expr(high)),
                not: self.rng.gen_bool(0.5),
            },
            Expr::BinaryOp {
                span,
                op,
                left,
                right,
            } => Expr::BinaryOp {
                span: *span,
                op: op.clone(),
                left: Box::new(self.fuzz_expr(left)),
                right: Box::new(self.fuzz_expr(right)),
            },
            Expr::JsonOp {
                span,
                op,
                left,
                right,
            } => Expr::JsonOp {
                span: *span,
                op: op.clone(),
                left: Box::new(self.fuzz_expr(left)),
                right: Box::new(self.fuzz_expr(right)),
            },
            Expr::UnaryOp { span, op, expr } => Expr::UnaryOp {
                span: *span,
                op: op.clone(),
                expr: Box::new(self.fuzz_expr(expr)),
            },
            Expr::Cast {
                span,
                expr,
                target_type,
                pg_style,
            } => {
                if self.rng.gen_bool(0.2) {
                    self.fuzz_expr(expr)
                } else {
                    Expr::Cast {
                        span: *span,
                        expr: Box::new(self.fuzz_expr(expr)),
                        target_type: target_type.clone(),
                        pg_style: *pg_style,
                    }
                }
            }
            Expr::TryCast {
                span,
                expr,
                target_type,
            } => {
                if self.rng.gen_bool(0.2) {
                    self.fuzz_expr(expr)
                } else {
                    Expr::TryCast {
                        span: *span,
                        expr: Box::new(self.fuzz_expr(expr)),
                        target_type: target_type.clone(),
                    }
                }
            }
            Expr::Extract { span, kind, expr } => Expr::Extract {
                span: *span,
                kind: *kind,
                expr: Box::new(self.fuzz_expr(expr)),
            },
            Expr::DatePart { span, kind, expr } => Expr::DatePart {
                span: *span,
                kind: *kind,
                expr: Box::new(self.fuzz_expr(expr)),
            },
            Expr::Position {
                span,
                substr_expr,
                str_expr,
            } => Expr::Position {
                span: *span,
                substr_expr: Box::new(self.fuzz_expr(substr_expr)),
                str_expr: Box::new(self.fuzz_expr(str_expr)),
            },
            Expr::Substring {
                span,
                expr,
                substring_from,
                substring_for,
            } => Expr::Substring {
                span: *span,
                expr: Box::new(self.fuzz_expr(expr)),
                substring_from: Box::new(self.fuzz_expr(substring_from)),
                substring_for: substring_for
                    .as_ref()
                    .map(|substring_for| Box::new(self.fuzz_expr(substring_for))),
            },
            Expr::Trim {
                span,
                expr,
                trim_where,
            } => Expr::Trim {
                span: *span,
                expr: Box::new(self.fuzz_expr(expr)),
                trim_where: trim_where
                    .as_ref()
                    .map(|(trim_where, expr)| (trim_where.clone(), Box::new(self.fuzz_expr(expr)))),
            },
            Expr::CountAll {
                span,
                window,
                qualified,
            } => Expr::CountAll {
                span: *span,
                window: window.as_ref().map(|window| self.fuzz_window(window)),
                qualified: qualified.clone(),
            },
            Expr::Tuple { span, exprs } => Expr::Tuple {
                span: *span,
                exprs: exprs.iter().map(|expr| self.fuzz_expr(expr)).collect(),
            },
            Expr::Case {
                span,
                operand,
                conditions,
                results,
                else_result,
            } => Expr::Case {
                span: *span,
                operand: operand
                    .as_ref()
                    .map(|operand| Box::new(self.fuzz_expr(operand))),
                conditions: conditions.iter().map(|expr| self.fuzz_expr(expr)).collect(),
                results: results.iter().map(|expr| self.fuzz_expr(expr)).collect(),
                else_result: else_result
                    .as_ref()
                    .map(|expr| Box::new(self.fuzz_expr(expr))),
            },
            Expr::Exists { span, subquery, .. } => Expr::Exists {
                span: *span,
                subquery: subquery.clone(),
                not: self.rng.gen_bool(0.5),
            },
            Expr::Subquery {
                span,
                modifier,
                subquery,
            } => Expr::Subquery {
                span: *span,
                modifier: modifier.clone(),
                subquery: subquery.clone(),
            },
            Expr::MapAccess {
                span,
                expr,
                accessor,
            } => Expr::MapAccess {
                span: *span,
                expr: Box::new(self.fuzz_expr(expr)),
                accessor: accessor.clone(),
            },
            Expr::Array { span, exprs } => Expr::Array {
                span: *span,
                exprs: exprs.iter().map(|expr| self.fuzz_expr(expr)).collect(),
            },
            Expr::Map { span, kvs } => Expr::Map {
                span: *span,
                kvs: kvs
                    .iter()
                    .map(|(k, expr)| (k.clone(), self.fuzz_expr(expr)))
                    .collect(),
            },
            Expr::Interval { span, expr, unit } => Expr::Interval {
                span: *span,
                expr: Box::new(self.fuzz_expr(expr)),
                unit: *unit,
            },
            Expr::DateAdd {
                span,
                unit,
                interval,
                date,
            } => Expr::DateAdd {
                span: *span,
                unit: *unit,
                interval: Box::new(self.fuzz_expr(interval)),
                date: Box::new(self.fuzz_expr(date)),
            },
            Expr::DateDiff {
                span,
                unit,
                date_start,
                date_end,
            } => Expr::DateDiff {
                span: *span,
                unit: *unit,
                date_start: Box::new(self.fuzz_expr(date_start)),
                date_end: Box::new(self.fuzz_expr(date_end)),
            },
            Expr::DateBetween {
                span,
                unit,
                date_start,
                date_end,
            } => Expr::DateBetween {
                span: *span,
                unit: *unit,
                date_start: Box::new(self.fuzz_expr(date_start)),
                date_end: Box::new(self.fuzz_expr(date_end)),
            },
            Expr::DateSub {
                span,
                unit,
                interval,
                date,
            } => Expr::DateSub {
                span: *span,
                unit: *unit,
                interval: Box::new(self.fuzz_expr(interval)),
                date: Box::new(self.fuzz_expr(date)),
            },
            Expr::DateTrunc { span, unit, date } => Expr::DateTrunc {
                span: *span,
                unit: *unit,
                date: Box::new(self.fuzz_expr(date)),
            },
            TimeSlice {
                span,
                slice_length,
                unit,
                date,
                start_or_end,
            } => TimeSlice {
                span: *span,
                unit: *unit,
                date: Box::new(self.fuzz_expr(date)),
                slice_length: *slice_length,
                start_or_end: start_or_end.to_string(),
            },
            Expr::PreviousDay { span, unit, date } => Expr::PreviousDay {
                span: *span,
                unit: *unit,
                date: Box::new(self.fuzz_expr(date)),
            },
            Expr::NextDay { span, unit, date } => Expr::NextDay {
                span: *span,
                unit: *unit,
                date: Box::new(self.fuzz_expr(date)),
            },
            _ => expr.clone(),
        }
    }

    fn fuzz_literal_bad_value(&mut self, lit: &Literal) -> Expr {
        match lit {
            Literal::UInt64(_) => {
                let val = BAD_INT64_VALUES[self.rng.gen_range(0..BAD_INT64_VALUES.len())];
                if val < 0 {
                    Expr::UnaryOp {
                        span: None,
                        op: UnaryOperator::Minus,
                        expr: Box::new(Expr::Literal {
                            span: None,
                            value: Literal::UInt64(val.unsigned_abs()),
                        }),
                    }
                } else {
                    Expr::Literal {
                        span: None,
                        value: Literal::UInt64(val as u64),
                    }
                }
            }
            Literal::Float64(_) => {
                let val = BAD_FLOAT64_VALUES[self.rng.gen_range(0..BAD_FLOAT64_VALUES.len())];
                Expr::Literal {
                    span: None,
                    value: Literal::Float64(val),
                }
            }
            Literal::Decimal256 { .. } => {
                let val = BAD_INT64_VALUES[self.rng.gen_range(0..BAD_INT64_VALUES.len())];
                let precision = self.rng.gen_range(1..76);
                let scale = self.rng.gen_range(0..precision);
                Expr::Literal {
                    span: None,
                    value: Literal::Decimal256 {
                        value: I256::from(val),
                        precision,
                        scale,
                    },
                }
            }
            Literal::String(s) => {
                let fuzz_str = match self.rng.gen_range(0..10) {
                    0 => "".to_string(),
                    1 => {
                        let mut owned_str = s.clone();
                        owned_str.push_str(s);
                        owned_str
                    }
                    2 => {
                        let mut owned_str = s.clone();
                        owned_str.push_str(s);
                        owned_str.push_str(s);
                        owned_str.push_str(s);
                        owned_str
                    }
                    3 => {
                        let mut owned_str = s.clone();
                        if !owned_str.is_empty() {
                            unsafe {
                                let bytes = owned_str.as_bytes_mut();
                                let idx = self.rng.gen_range(0..bytes.len());
                                bytes[idx] = b'\0';
                                owned_str = String::from_utf8_unchecked(bytes.to_vec());
                            }
                        }
                        owned_str
                    }
                    _ => s.clone(),
                };
                Expr::Literal {
                    span: None,
                    value: Literal::String(fuzz_str),
                }
            }
            Literal::Boolean(val) => Expr::Literal {
                span: None,
                value: Literal::Boolean(*val),
            },
            Literal::Null => Expr::Literal {
                span: None,
                value: Literal::Null,
            },
        }
    }

    fn get_random_column_like(&mut self) -> Expr {
        if self.collector_visitor.column_like.is_empty() {
            Expr::Literal {
                span: None,
                value: Literal::Null,
            }
        } else {
            let (_, fuzz_expr) = &self.collector_visitor.column_like[self
                .rng
                .gen_range(0..self.collector_visitor.column_like.len())];
            fuzz_expr.clone()
        }
    }

    fn gen_expr_func(&mut self, func_name: &str, args: Vec<Expr>, params: Vec<Expr>) -> Expr {
        Expr::FunctionCall {
            span: None,
            func: FunctionCall {
                distinct: false,
                name: Identifier::from_name(None, func_name),
                args,
                params,
                order_by: vec![],
                window: None,
                lambda: None,
            },
        }
    }

    fn gen_expr_binary_op(&mut self, op: BinaryOperator, arg0: Expr, arg1: Expr) -> Expr {
        Expr::BinaryOp {
            span: None,
            op,
            left: Box::new(arg0),
            right: Box::new(arg1),
        }
    }

    fn fuzz_expr_opt(&mut self, expr_opt: &mut Option<Expr>) {
        if let Some(expr) = expr_opt {
            if self.rng.gen_bool(0.2) {
                let new_expr = self.fuzz_expr(expr);
                *expr_opt = Some(new_expr);
            }
        } else if self.rng.gen_bool(0.1) {
            let new_expr = self.get_random_column_like();
            *expr_opt = Some(new_expr);
        }
    }

    fn fuzz_column(&mut self, span: &Span, column: &ColumnRef) -> Expr {
        if self.rng.gen_bool(0.1) {
            self.get_random_column_like()
        } else {
            Expr::ColumnRef {
                span: *span,
                column: column.clone(),
            }
        }
    }

    fn fuzz_literal(&mut self, span: &Span, value: &Literal) -> Expr {
        let is_number = matches!(
            value,
            Literal::UInt64(_) | Literal::Float64(_) | Literal::Decimal256 { .. }
        );
        let is_string = matches!(value, Literal::String(_));

        // A bad value may be generated
        let mut expr = if self.rng.gen_bool(0.3) {
            self.fuzz_literal_bad_value(value)
        } else {
            Expr::Literal {
                span: *span,
                value: value.clone(),
            }
        };

        if is_number && self.rng.gen_bool(0.3) {
            let precision = self.rng.gen_range(1..76);
            let scale = self.rng.gen_range(0..precision);
            let params = vec![
                Expr::Literal {
                    span: *span,
                    value: Literal::UInt64(precision),
                },
                Expr::Literal {
                    span: *span,
                    value: Literal::UInt64(scale),
                },
            ];
            expr = self.gen_expr_func("to_decimal", vec![expr], params);
        }
        if is_number && self.rng.gen_bool(0.3) {
            let other = Expr::Literal {
                span: *span,
                value: Literal::UInt64(self.rng.gen_range(0..1000)),
            };
            let op = match self.rng.gen_range(0..5) {
                0 => BinaryOperator::Plus,
                1 => BinaryOperator::Minus,
                2 => BinaryOperator::Multiply,
                3 => BinaryOperator::Div,
                _ => BinaryOperator::Divide,
            };
            expr = if self.rng.gen_bool(0.5) {
                self.gen_expr_binary_op(op, expr, other)
            } else {
                self.gen_expr_binary_op(op, other, expr)
            };
        }

        if is_string && self.rng.gen_bool(0.3) {
            let func_name = match self.rng.gen_range(0..7) {
                0 => "lower",
                1 => "upper",
                2 => "bit_length",
                3 => "octet_length",
                4 => "length",
                5 => "quote",
                _ => "trim",
            };
            expr = self.gen_expr_func(func_name, vec![expr.clone()], vec![]);
        }
        if self.rng.gen_bool(0.3) {
            expr = self.gen_expr_func("to_nullable", vec![expr], vec![]);
        }

        expr
    }

    fn fuzz_func(&mut self, span: &Span, func: &FunctionCall) -> Expr {
        // try remove cast function
        if self.rng.gen_bool(0.2)
            && !func.args.is_empty()
            && SIMPLE_CAST_FUNCTIONS.contains(&func.name.name.to_lowercase().as_str())
        {
            return func.args[0].clone();
        }
        let mut func = func.clone();
        // try remove argument
        if self.rng.gen_bool(0.05) && !func.args.is_empty() {
            let idx = self.rng.gen_range(0..func.args.len());
            func.args.remove(idx);
        }
        // try replace arguemnts with fuzzed arguments
        if self.rng.gen_bool(0.05) {
            func.args = func.args.iter().map(|arg| self.fuzz_expr(arg)).collect();
        }
        if let Some(window) = func.window {
            func.window = Some(self.fuzz_window_desc(&window));
        }
        Expr::FunctionCall { span: *span, func }
    }

    fn fuzz_window_desc(&mut self, window_desc: &WindowDesc) -> WindowDesc {
        let mut window_desc = window_desc.clone();
        // fuzz ignore_nulls
        if window_desc.ignore_nulls.is_none() && self.rng.gen_bool(0.2) {
            window_desc.ignore_nulls = Some(self.rng.gen_bool(0.5));
        } else if self.rng.gen_bool(0.2) {
            match self.rng.gen_range(0..3) {
                0 => window_desc.ignore_nulls = Some(true),
                1 => window_desc.ignore_nulls = Some(false),
                _ => window_desc.ignore_nulls = None,
            }
        }
        window_desc.window = self.fuzz_window(&window_desc.window);
        window_desc
    }

    fn fuzz_window(&mut self, window: &Window) -> Window {
        let mut window = window.clone();
        if let Window::WindowSpec(ref mut window_spec) = window {
            // fuzz window order by
            self.fuzz_order_by(&mut window_spec.order_by);
            // fuzz window frame
            if self.rng.gen_bool(0.4) {
                let units = if self.rng.gen_bool(0.5) {
                    WindowFrameUnits::Rows
                } else {
                    WindowFrameUnits::Range
                };
                let start_bound = self.fuzz_bound();
                let end_bound = self.fuzz_bound();
                let window_frame = WindowFrame {
                    units,
                    start_bound,
                    end_bound,
                };
                window_spec.window_frame = Some(window_frame);
            }
        }
        window
    }

    fn fuzz_bound(&mut self) -> WindowFrameBound {
        let bound = Literal::UInt64(0);
        match self.rng.gen_range(0..5) {
            0 => WindowFrameBound::Preceding(None),
            1 => {
                let fuzz_bound = self.fuzz_literal_bad_value(&bound);
                WindowFrameBound::Preceding(Some(Box::new(fuzz_bound)))
            }
            2 => WindowFrameBound::Following(None),
            3 => {
                let fuzz_bound = self.fuzz_literal_bad_value(&bound);
                WindowFrameBound::Following(Some(Box::new(fuzz_bound)))
            }
            _ => WindowFrameBound::CurrentRow,
        }
    }

    fn fuzz_order_by(&mut self, order_by: &mut Vec<OrderByExpr>) {
        // remove element
        if self.rng.gen_bool(0.3) && order_by.len() > 1 {
            let idx = self.rng.gen_range(0..order_by.len());
            order_by.remove(idx);
        }
        // add random column
        if self.rng.gen_bool(0.1) {
            let expr = self.get_random_column_like();
            let order_by_expr = OrderByExpr {
                expr,
                asc: Some(self.rng.gen_bool(0.5)),
                nulls_first: Some(self.rng.gen_bool(0.5)),
            };
            if order_by.len() > 1 {
                let idx = self.rng.gen_range(0..order_by.len());
                order_by.insert(idx, order_by_expr);
            } else {
                order_by.push(order_by_expr);
            }
        }
    }

    fn fuzz_group_by(&mut self, group_by: &mut Option<GroupBy>) {
        if let Some(group_by) = group_by {
            if self.rng.gen_bool(0.2) {
                let expr = self.get_random_column_like();
                match self.rng.gen_range(0..6) {
                    0 => {
                        *group_by = GroupBy::Normal(vec![expr]);
                    }
                    1 => *group_by = GroupBy::All,
                    2 => {
                        *group_by = GroupBy::GroupingSets(vec![vec![expr]]);
                    }
                    3 => {
                        *group_by = GroupBy::Cube(vec![expr]);
                    }
                    4 => {
                        *group_by = GroupBy::Rollup(vec![expr]);
                    }
                    _ => {
                        *group_by = GroupBy::Combined(vec![GroupBy::Normal(vec![expr])]);
                    }
                }
            }
        } else if self.rng.gen_bool(0.2) {
            let expr = self.get_random_column_like();
            *group_by = Some(GroupBy::Normal(vec![expr]));
        }
    }
}

pub struct QueryFuzzer {
    query_visitor: QueryVisitor,
}

impl QueryFuzzer {
    pub fn new() -> Self {
        Self {
            query_visitor: QueryVisitor::new(None),
        }
    }

    pub fn fuzz(&mut self, stmt: Statement) -> Statement {
        self.collect_fuzz_info(stmt.clone());
        let mut fuzzed_stmt = stmt.clone();
        fuzzed_stmt.drive_mut(&mut self.query_visitor);
        fuzzed_stmt
    }

    fn collect_fuzz_info(&mut self, stmt: Statement) {
        match stmt {
            Statement::Query(_) => {
                stmt.drive(&mut self.query_visitor.collector_visitor);
            }
            _ => {
                // todo
            }
        }
    }
}
