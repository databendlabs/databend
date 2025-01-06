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
use databend_common_ast::ast::WindowFrame;
use databend_common_ast::ast::WindowFrameBound;
use databend_common_ast::ast::WindowFrameUnits;
use derive_visitor::Drive;
use derive_visitor::DriveMut;
use derive_visitor::Visitor;
use derive_visitor::VisitorMut;
use ethnum::I256;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;

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

const DECIMAL_SCALES: [u8; 4] = [0, 1, 2, 10];

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
            Literal::Decimal256 { precision, .. } => {
                let val = BAD_INT64_VALUES[self.rng.gen_range(0..BAD_INT64_VALUES.len())];
                let scale = DECIMAL_SCALES[self.rng.gen_range(0..DECIMAL_SCALES.len())];
                Expr::Literal {
                    span: None,
                    value: Literal::Decimal256 {
                        value: I256::from(val),
                        precision: *precision,
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
                window: None,
                lambda: None,
            },
        }
    }

    fn fuzz_literal(&mut self, value: &Literal) -> Expr {
        let is_number = matches!(value, Literal::UInt64(_) | Literal::Float64(_));

        let mut expr = if self.rng.gen_bool(0.15) {
            self.fuzz_literal_bad_value(value)
        } else {
            Expr::Literal {
                span: None,
                value: value.clone(),
            }
        };
        if is_number && self.rng.gen_bool(0.15) {
            let precision = self.rng.gen_range(1..76);
            let scale = self.rng.gen_range(0..precision);
            let params = vec![
                Expr::Literal {
                    span: None,
                    value: Literal::UInt64(precision),
                },
                Expr::Literal {
                    span: None,
                    value: Literal::UInt64(scale),
                },
            ];
            expr = self.gen_expr_func("to_decimal", vec![expr], params);
        }

        if self.rng.gen_bool(0.15) {
            expr = self.gen_expr_func("to_nullable", vec![expr], vec![]);
        }
        expr
    }

    fn fuzz_func(&mut self, func: &FunctionCall) -> Expr {
        // try remove cast function
        if self.rng.gen_bool(0.15) && SIMPLE_CAST_FUNCTIONS.contains(&func.name.name.as_str()) {
            return func.args[0].clone();
        }
        let mut func = func.clone();
        // try remove argument
        if self.rng.gen_bool(0.02) && !func.args.is_empty() {
            let idx = self.rng.gen_range(0..func.args.len());
            func.args.remove(idx);
        }
        // try remove params
        if self.rng.gen_bool(0.02) && !func.params.is_empty() {
            let idx = self.rng.gen_range(0..func.params.len());
            func.params.remove(idx);
        }
        if func.window.is_some() {
            let mut window = func.window.clone().unwrap();
            // fuzz ignore_nulls
            if window.ignore_nulls.is_none() && self.rng.gen_bool(0.01) {
                window.ignore_nulls = Some(self.rng.gen_bool(0.5));
            } else if self.rng.gen_bool(0.05) {
                match self.rng.gen_range(0..3) {
                    0 => window.ignore_nulls = Some(true),
                    1 => window.ignore_nulls = Some(false),
                    _ => window.ignore_nulls = None,
                }
            }
            if let Window::WindowSpec(mut window_spec) = window.window {
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
        }
        Expr::FunctionCall { span: None, func }
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
        if self.rng.gen_bool(0.02) && order_by.len() > 1 {
            let idx = self.rng.gen_range(0..order_by.len());
            order_by.remove(idx);
        }
        // add random column
        if self.rng.gen_bool(0.02) {
            let expr = self.get_random_column_like();
            let order_by_expr = OrderByExpr {
                expr,
                asc: Some(self.rng.gen_bool(0.5)),
                nulls_first: Some(self.rng.gen_bool(0.5)),
            };
            let idx = self.rng.gen_range(0..order_by.len());
            order_by.insert(idx, order_by_expr);
        }
    }

    fn fuzz_expr(&mut self, expr: &mut Option<Expr>) {
        if expr.is_some() {
            if self.rng.gen_bool(0.1) {
                let new_expr = self.get_random_column_like();
                *expr = Some(new_expr);
            }
        } else if self.rng.gen_bool(0.02) {
            let new_expr = self.get_random_column_like();
            *expr = Some(new_expr);
        }
    }

    fn fuzz_group_by(&mut self, group_by: &mut Option<GroupBy>) {
        if let Some(group_by) = group_by {
            if self.rng.gen_bool(0.1) {
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
        } else if self.rng.gen_bool(0.02) {
            let expr = self.get_random_column_like();
            *group_by = Some(GroupBy::Normal(vec![expr]));
        }
    }

    fn fuzz_select_list(&mut self, select_list: &mut Vec<SelectTarget>) {
        for target in select_list {
            if let SelectTarget::AliasedExpr { expr, alias } = target {
                let fuzzed_expr = match &**expr {
                    Expr::Literal { value, .. } => self.fuzz_literal(value),
                    Expr::FunctionCall { func, .. } => self.fuzz_func(func),
                    _ => {
                        // todo
                        *expr.clone()
                    }
                };
                *target = SelectTarget::AliasedExpr {
                    expr: Box::new(fuzzed_expr),
                    alias: alias.clone(),
                };
            }
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
        self.fuzz_expr(&mut select_stmt.selection);
        // fuzz group by
        self.fuzz_group_by(&mut select_stmt.group_by);
        // fuzz having
        self.fuzz_expr(&mut select_stmt.having);
        // fuzz qualify
        self.fuzz_expr(&mut select_stmt.qualify);
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
