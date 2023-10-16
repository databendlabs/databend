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

use std::mem;

use common_ast::ast::Expr;
use common_ast::ast::Identifier;
use common_ast::ast::Lambda;
use common_ast::ast::Literal;
use common_ast::ast::OrderByExpr;
use common_ast::ast::Window;
use common_ast::ast::WindowFrame;
use common_ast::ast::WindowFrameBound;
use common_ast::ast::WindowFrameUnits;
use common_ast::ast::WindowRef;
use common_ast::ast::WindowSpec;
use common_expression::types::DataType;
use common_expression::types::DecimalDataType::Decimal128;
use common_expression::types::DecimalDataType::Decimal256;
use common_expression::types::DecimalSize;
use common_expression::types::NumberDataType;
use common_expression::types::ALL_FLOAT_TYPES;
use common_expression::types::ALL_INTEGER_TYPES;
use rand::Rng;

use crate::sql_gen::Column;
use crate::sql_gen::SqlGenerator;

impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn gen_scalar_func(&mut self, ty: &DataType) -> Expr {
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
        let func_sig = unsafe { self.scalar_func_sigs.get_unchecked(indices[idx]) }.clone();

        self.gen_func(
            func_sig.name.clone(),
            vec![],
            func_sig.args_type,
            None,
            None,
        )
    }

    pub(crate) fn gen_factory_scalar_func(&mut self, ty: &DataType) -> Expr {
        let (name, params, args_type) = match ty.remove_nullable() {
            DataType::String => {
                let idx = self.rng.gen_range(0..=5);
                let name = match idx {
                    0 => "char".to_string(),
                    1 => "concat".to_string(),
                    2 => "concat_ws".to_string(),
                    3 => "regexp_replace".to_string(),
                    4 => "regexp_substr".to_string(),
                    5 => "to_string".to_string(),
                    _ => unreachable!(),
                };
                let args_type = if idx == 0 {
                    let len = self.rng.gen_range(1..=6);
                    vec![DataType::Number(NumberDataType::UInt8); len]
                } else if idx == 3 {
                    match self.rng.gen_range(3..=6) {
                        3 => vec![DataType::String; 3],
                        4 => vec![
                            DataType::String,
                            DataType::String,
                            DataType::String,
                            DataType::Number(NumberDataType::Int64),
                        ],
                        5 => vec![
                            DataType::String,
                            DataType::String,
                            DataType::String,
                            DataType::Number(NumberDataType::Int64),
                            DataType::Number(NumberDataType::Int64),
                        ],
                        6 => vec![
                            DataType::String,
                            DataType::String,
                            DataType::String,
                            DataType::Number(NumberDataType::Int64),
                            DataType::Number(NumberDataType::Int64),
                            DataType::String,
                        ],
                        _ => unreachable!(),
                    }
                } else if idx == 4 {
                    match self.rng.gen_range(2..=5) {
                        2 => vec![DataType::String; 2],
                        3 => vec![
                            DataType::String,
                            DataType::String,
                            DataType::Number(NumberDataType::Int64),
                        ],
                        4 => vec![
                            DataType::String,
                            DataType::String,
                            DataType::Number(NumberDataType::Int64),
                            DataType::Number(NumberDataType::Int64),
                        ],
                        5 => vec![
                            DataType::String,
                            DataType::String,
                            DataType::Number(NumberDataType::Int64),
                            DataType::Number(NumberDataType::Int64),
                            DataType::String,
                        ],
                        _ => unreachable!(),
                    }
                } else if idx == 5 {
                    if self.rng.gen_bool(0.5) {
                        vec![
                            DataType::Decimal(Decimal128(DecimalSize {
                                precision: 20,
                                scale: 0
                            }));
                            1
                        ]
                    } else {
                        vec![
                            DataType::Decimal(Decimal256(DecimalSize {
                                precision: 39,
                                scale: 0
                            }));
                            1
                        ]
                    }
                } else {
                    let len = self.rng.gen_range(2..=6);
                    vec![DataType::String; len]
                };
                (name, vec![], args_type)
            }
            DataType::Boolean => {
                let idx = self.rng.gen_range(0..=3);
                let name = match idx {
                    0 => "and_filters".to_string(),
                    1 => "regexp_like".to_string(),
                    2 => {
                        let comp_func = vec!["eq", "gt", "gte", "lt", "lte", "ne", "noteq"];
                        comp_func[self.rng.gen_range(0..=6)].to_string()
                    }
                    3 => "ignore".to_string(),

                    _ => unreachable!(),
                };
                let args_type = match idx {
                    0 => vec![DataType::Boolean; 2],
                    1 => match self.rng.gen_range(2..=3) {
                        2 => vec![DataType::String; 2],
                        3 => vec![DataType::String; 3],
                        _ => unreachable!(),
                    },
                    2 => {
                        let ty = self.gen_data_type();
                        vec![ty; 2]
                    }
                    3 => {
                        let ty1 = self.gen_data_type();
                        let ty2 = self.gen_data_type();
                        let ty3 = self.gen_data_type();
                        vec![ty1, ty2, ty3]
                    }
                    _ => unreachable!(),
                };
                (name, vec![], args_type)
            }
            DataType::Number(_) => {
                let idx = self.rng.gen_range(0..=4);
                let name = match idx {
                    0 => "point_in_ellipses".to_string(),
                    1 => "point_in_polygon".to_string(),
                    2 => "regexp_instr".to_string(),
                    3 => {
                        let arithmetic_func = vec!["plus", "minus", "multiply", "divide"];
                        arithmetic_func[self.rng.gen_range(0..=3)].to_string()
                    }
                    4 => {
                        let array_func = vec![
                            "array_approx_count_distinct",
                            "array_avg",
                            "array_kurtosis",
                            "array_median",
                            "array_skewness",
                            "array_std",
                            "array_stddev",
                            "array_stddev_pop",
                            "array_stddev_samp",
                            "array_sum",
                        ];
                        array_func[self.rng.gen_range(0..=9)].to_string()
                    }
                    _ => unreachable!(),
                };

                let args_type = match idx {
                    0 => vec![DataType::Number(NumberDataType::Float64); 7],
                    1 => {
                        let mut args_type = vec![];
                        let arg1 =
                            DataType::Tuple(vec![DataType::Number(NumberDataType::Float64); 3]);
                        let arg2 =
                            DataType::Array(Box::from(DataType::Number(NumberDataType::Float64)));
                        let arg3 =
                            DataType::Array(Box::from(DataType::Number(NumberDataType::Int64)));
                        args_type.push(arg1);
                        args_type.push(arg2);
                        args_type.push(arg3);
                        args_type
                    }
                    2 => match self.rng.gen_range(2..=6) {
                        2 => vec![DataType::String; 2],
                        3 => vec![
                            DataType::String,
                            DataType::String,
                            DataType::Number(NumberDataType::Int64),
                        ],
                        4 => vec![
                            DataType::String,
                            DataType::String,
                            DataType::Number(NumberDataType::Int64),
                            DataType::Number(NumberDataType::Int64),
                        ],
                        5 => vec![
                            DataType::String,
                            DataType::String,
                            DataType::Number(NumberDataType::Int64),
                            DataType::Number(NumberDataType::Int64),
                            DataType::Number(NumberDataType::Int64),
                        ],
                        6 => vec![
                            DataType::String,
                            DataType::String,
                            DataType::Number(NumberDataType::Int64),
                            DataType::Number(NumberDataType::Int64),
                            DataType::Number(NumberDataType::Int64),
                            DataType::String,
                        ],
                        _ => unreachable!(),
                    },
                    3 => {
                        let mut args_type = vec![];
                        let int_num = ALL_INTEGER_TYPES.len();
                        let float_num = ALL_FLOAT_TYPES.len();
                        let left = ALL_INTEGER_TYPES[self.rng.gen_range(0..=int_num - 1)];
                        let right = ALL_FLOAT_TYPES[self.rng.gen_range(0..=float_num - 1)];
                        if self.rng.gen_bool(0.5) {
                            args_type.push(DataType::Number(left));
                            args_type.push(DataType::Number(right));
                        } else {
                            args_type.push(DataType::Number(right));
                            args_type.push(DataType::Number(left));
                        }
                        args_type
                    }
                    4 => {
                        let inner_ty = self.gen_number_data_type();
                        vec![DataType::Array(Box::new(inner_ty))]
                    }
                    _ => unreachable!(),
                };

                (name, vec![], args_type)
            }
            DataType::Array(box inner_ty) => {
                let name = "array".to_string();
                let len = self.rng.gen_range(0..=4);
                let args_type = vec![inner_ty; len];
                (name, vec![], args_type)
            }
            DataType::Decimal(_) => {
                let decimal = vec!["to_float64", "to_float32", "to_decimal", "try_to_decimal"];
                let name = decimal[self.rng.gen_range(0..=3)].to_string();
                if name == "to_decimal" || name == "try_to_decimal" {
                    let args_type = vec![self.gen_data_type(); 1];
                    let params = vec![Literal::UInt64(20), Literal::UInt64(19)];
                    (name, params, args_type)
                } else {
                    let ty = if self.rng.gen_bool(0.5) {
                        DataType::Decimal(Decimal128(DecimalSize {
                            precision: 28,
                            scale: 0,
                        }))
                    } else {
                        DataType::Decimal(Decimal256(DecimalSize {
                            precision: 39,
                            scale: 0,
                        }))
                    };
                    let args_type = vec![ty; 1];
                    let params = vec![];
                    (name, params, args_type)
                }
            }
            DataType::Tuple(inner_tys) => {
                let name = "tuple".to_string();
                (name, vec![], inner_tys)
            }
            DataType::Variant => {
                if self.rng.gen_bool(0.5) {
                    let json_func = vec!["json_array", "json_object", "json_object_keep_null"];
                    let name = json_func[self.rng.gen_range(0..=2)].to_string();
                    let len = self.rng.gen_range(0..=2);
                    let mut args_type = Vec::with_capacity(len * 2);
                    for _ in 0..len {
                        args_type.push(DataType::String);
                        args_type.push(self.gen_data_type());
                    }
                    (name, vec![], args_type)
                } else {
                    let json_func = vec!["unnest", "json_path_query"];
                    let name = json_func[self.rng.gen_range(0..=1)].to_string();
                    let args_type = vec![ty.clone()];
                    (name, vec![], args_type)
                }
            }
            _ => {
                if self.rng.gen_bool(0.3) {
                    let name = "if".to_string();
                    let len = self.rng.gen_range(1..=3) * 2 + 1;
                    let mut args_type = Vec::with_capacity(len);
                    for i in 0..len {
                        if i % 2 == 0 && i != len - 1 {
                            args_type.push(DataType::Boolean);
                        } else {
                            args_type.push(ty.clone());
                        }
                    }
                    (name, vec![], args_type)
                } else {
                    let array_func = vec![
                        "unnest",
                        "array_any",
                        "array_count",
                        "array_max",
                        "array_min",
                    ];
                    let name = array_func[self.rng.gen_range(0..=4)].to_string();
                    let args_type = vec![DataType::Array(Box::new(ty.clone()))];
                    (name, vec![], args_type)
                }
            }
        };

        self.gen_func(name, params, args_type, None, None)
    }

    pub(crate) fn gen_agg_func(&mut self, ty: &DataType) -> Expr {
        let (name, params, mut args_type) = match ty.remove_nullable() {
            DataType::Number(NumberDataType::UInt8) => {
                let name = "window_funnel".to_string();
                let other_type = vec![DataType::Boolean; 6];
                let mut args_type = Vec::with_capacity(7);

                match self.rng.gen_range(0..=2) {
                    0 => args_type.push(self.gen_number_data_type()),
                    1 => args_type.push(DataType::Date),
                    2 => args_type.push(DataType::Timestamp),
                    _ => unreachable!(),
                };
                args_type.extend_from_slice(&other_type);
                let params = vec![Literal::UInt64(self.rng.gen_range(1..=10))];
                (name, params, args_type)
            }
            DataType::Number(NumberDataType::UInt64) => {
                let idx = self.rng.gen_range(0..=7);
                let name = match idx {
                    0 => "approx_count_distinct".to_string(),
                    1 => "count".to_string(),
                    2 => "bitmap_and_count".to_string(),
                    3 => "bitmap_or_count".to_string(),
                    4 => "bitmap_xor_count".to_string(),
                    5 => "bitmap_not_count".to_string(),
                    6 => "intersect_count".to_string(),
                    7 => "sum".to_string(),
                    _ => unreachable!(),
                };
                let args_type = if (2..=5).contains(&idx) {
                    if self.rng.gen_bool(0.5) {
                        vec![DataType::Bitmap]
                    } else {
                        vec![DataType::Nullable(Box::new(DataType::Bitmap))]
                    }
                } else if idx == 6 {
                    if self.rng.gen_bool(0.5) {
                        vec![DataType::Bitmap; 2]
                    } else {
                        vec![DataType::Nullable(Box::new(DataType::Bitmap)); 2]
                    }
                } else if idx == 7 {
                    vec![self.gen_all_number_data_type()]
                } else {
                    vec![self.gen_data_type()]
                };
                let params = if idx == 6 {
                    vec![
                        Literal::UInt64(self.rng.gen_range(1..=10)),
                        Literal::UInt64(self.rng.gen_range(1..=10)),
                    ]
                } else {
                    vec![]
                };
                (name, params, args_type)
            }
            DataType::Array(_) => {
                let idx = self.rng.gen_range(0..=2);
                let name = match idx {
                    0 => {
                        if self.rng.gen_bool(0.5) {
                            "array_agg".to_string()
                        } else {
                            "list".to_string()
                        }
                    }
                    1 => "retention".to_string(),
                    2 => {
                        if self.rng.gen_bool(0.5) {
                            "group_array_moving_sum".to_string()
                        } else {
                            "group_array_moving_avg".to_string()
                        }
                    }
                    _ => unreachable!(),
                };
                let args_type = if idx == 0 {
                    vec![self.gen_data_type()]
                } else if idx == 1 {
                    if self.rng.gen_bool(0.9) {
                        vec![DataType::Boolean; 6]
                    } else {
                        vec![self.gen_data_type(); 6]
                    }
                } else {
                    vec![self.gen_all_number_data_type()]
                };

                let params = if idx == 2 {
                    if self.rng.gen_bool(0.5) {
                        vec![Literal::UInt64(self.rng.gen_range(1..=3))]
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                };
                (name, params, args_type)
            }
            DataType::Decimal(_) => {
                let name = "sum".to_string();
                let params = vec![];
                let args_type = vec![self.gen_decimal_data_type()];
                (name, params, args_type)
            }
            DataType::Number(NumberDataType::Float64) => {
                let idx = self.rng.gen_range(0..=14);
                let name = match idx {
                    0 => "avg".to_string(),
                    1 => "covar_pop".to_string(),
                    2 => "covar_samp".to_string(),
                    3 => "kurtosis".to_string(),
                    4 => "median_tdigest".to_string(),
                    5 => "median".to_string(),
                    6 => "skewness".to_string(),
                    7 => "stddev_pop".to_string(),
                    8 => "stddev".to_string(),
                    9 => "std".to_string(),
                    10 => "stddev_samp".to_string(),
                    11 => "quantile".to_string(),
                    12 => "quantile_cont".to_string(),
                    13 => "quantile_tdigest".to_string(),
                    14 => "quantile_disc".to_string(),
                    _ => unreachable!(),
                };

                let args_type = if idx == 1 || idx == 2 {
                    vec![
                        self.gen_all_number_data_type(),
                        self.gen_all_number_data_type(),
                    ]
                } else {
                    vec![self.gen_all_number_data_type()]
                };

                let params = if idx >= 11 {
                    if self.rng.gen_bool(0.5) {
                        vec![Literal::Float64(self.rng.gen_range(0.01..=0.99))]
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                };
                (name, params, args_type)
            }
            DataType::Bitmap => {
                let idx = self.rng.gen_range(0..=1);
                let name = match idx {
                    0 => "bitmap_intersect".to_string(),
                    1 => "bitmap_union".to_string(),
                    _ => unreachable!(),
                };
                let params = vec![];
                let args_type = vec![DataType::Bitmap];
                (name, params, args_type)
            }
            DataType::String => {
                let name = "string_agg".to_string();
                let args_type = if self.rng.gen_bool(0.6) {
                    vec![DataType::String]
                } else {
                    vec![DataType::String; 2]
                };
                let params = vec![];
                (name, params, args_type)
            }
            _ => {
                // TODO: other aggreate functions
                let idx = self.rng.gen_range(0..=4);
                let name = match idx {
                    0 => "any".to_string(),
                    1 => "min".to_string(),
                    2 => "max".to_string(),
                    3 => "arg_min".to_string(),
                    4 => "arg_max".to_string(),
                    _ => unreachable!(),
                };
                let params = vec![];
                let args_type = if idx == 3 || idx == 4 {
                    vec![ty.clone(), self.gen_simple_data_type()]
                } else {
                    vec![ty.clone()]
                };
                (name, params, args_type)
            }
        };
        // test combinator, only need test _if and _distinct
        let idx = self.rng.gen_range(0..=2);
        let (name, params, args_type) = match idx {
            0 => (name, params, args_type),
            1 => {
                let name = name + "_if";
                args_type.push(DataType::Boolean);
                (name, params, args_type)
            }
            2 => {
                let name = name + "_distinct";
                (name, params, args_type)
            }
            _ => unreachable!(),
        };

        let window = if self.rng.gen_bool(0.8) {
            None
        } else {
            self.gen_window()
        };

        self.gen_func(name, params, args_type, window, None)
    }

    pub(crate) fn gen_window_func(&mut self, ty: &DataType) -> Expr {
        let window = self.gen_window();
        let ty = ty.clone();
        match ty {
            DataType::Number(NumberDataType::UInt64) => {
                let number = vec!["row_number", "rank", "dense_rank", "ntile"];
                let name = number[self.rng.gen_range(0..=2)];
                let args_type = if name == "ntile" {
                    vec![DataType::Number(NumberDataType::UInt64)]
                } else {
                    vec![]
                };
                self.gen_func(name.to_string(), vec![], args_type, window, None)
            }
            DataType::Number(NumberDataType::Float64) => {
                let float = vec!["percent_rank", "cume_dist"];
                let name = float[self.rng.gen_range(0..=1)].to_string();
                self.gen_func(name, vec![], vec![], window, None)
            }
            _ => {
                let name = vec![
                    "lag",
                    "lead",
                    "first_value",
                    "first",
                    "last_value",
                    "last",
                    "nth_value",
                ];
                let name = name[self.rng.gen_range(0..=6)];
                let args_type = if name == "lag" || name == "lead" {
                    vec![ty; 3]
                } else if name == "nth_value" {
                    vec![ty, DataType::Number(NumberDataType::UInt64)]
                } else {
                    vec![ty]
                };
                self.gen_func(name.to_string(), vec![], args_type, window, None)
            }
        }
    }

    fn gen_window(&mut self) -> Option<Window> {
        if self.rng.gen_bool(0.2) && !self.windows_name.is_empty() {
            let len = self.windows_name.len();
            let name = if len == 1 {
                self.windows_name[0].to_string()
            } else {
                self.windows_name[self.rng.gen_range(0..=len - 1)].to_string()
            };
            Some(Window::WindowReference(WindowRef {
                window_name: Identifier {
                    name,
                    quote: None,
                    span: None,
                },
            }))
        } else {
            let window_spec = self.gen_window_spec();
            Some(Window::WindowSpec(window_spec))
        }
    }

    pub(crate) fn gen_window_spec(&mut self) -> WindowSpec {
        let ty = self.gen_data_type();
        let expr1 = self.gen_scalar_value(&ty);
        let expr2 = self.gen_scalar_value(&ty);
        let expr3 = self.gen_scalar_value(&ty);
        let expr4 = self.gen_scalar_value(&ty);

        let order_by = vec![
            OrderByExpr {
                expr: expr1,
                asc: None,
                nulls_first: None,
            },
            OrderByExpr {
                expr: expr2,
                asc: Some(true),
                nulls_first: Some(true),
            },
        ];
        WindowSpec {
            existing_window_name: None,
            partition_by: vec![expr3, expr4],
            order_by,
            window_frame: if self.rng.gen_bool(0.8) {
                None
            } else {
                Some(WindowFrame {
                    units: WindowFrameUnits::Rows,
                    start_bound: WindowFrameBound::Preceding(None),
                    end_bound: WindowFrameBound::CurrentRow,
                })
            },
        }
    }

    pub(crate) fn gen_lambda_func(&mut self, ty: &DataType) -> Expr {
        // return value of lambda function must be an array type
        if !matches!(ty, &DataType::Array(_)) {
            return self.gen_simple_expr(ty);
        }
        let inner_ty = ty.as_array().unwrap();

        let current_cte_tables = mem::take(&mut self.cte_tables);
        let current_bound_tables = mem::take(&mut self.bound_tables);
        let current_bound_columns = mem::take(&mut self.bound_columns);
        let current_is_join = self.is_join;

        self.cte_tables = vec![];
        self.bound_tables = vec![];
        self.bound_columns = vec![];
        self.is_join = false;

        let name = if inner_ty.remove_nullable() == DataType::Boolean {
            "array_filter".to_string()
        } else if self.rng.gen_bool(0.5) {
            "array_transform".to_string()
        } else {
            "array_apply".to_string()
        };

        let args_type = vec![ty.clone()];
        let lambda_name = format!("l{}", self.gen_random_name());
        let lambda_column = Column {
            table_name: "".to_string(),
            name: lambda_name.clone(),
            index: 0,
            data_type: ty.clone(),
        };
        self.bound_columns.push(lambda_column);

        let lambda_expr = self.gen_expr(inner_ty);

        let lambda = Lambda {
            params: vec![Identifier::from_name(lambda_name)],
            expr: Box::new(lambda_expr),
        };

        self.cte_tables = current_cte_tables;
        self.bound_tables = current_bound_tables;
        self.bound_columns = current_bound_columns;
        self.is_join = current_is_join;

        self.gen_func(name, vec![], args_type, None, Some(lambda))
    }

    fn gen_func(
        &mut self,
        name: String,
        params: Vec<Literal>,
        args_type: Vec<DataType>,
        window: Option<Window>,
        lambda: Option<Lambda>,
    ) -> Expr {
        let distinct = if name == *"count" {
            self.rng.gen_bool(0.5)
        } else {
            false
        };

        let mut args = vec![];
        for (i, ty) in args_type.iter().enumerate() {
            if name == *"lead" || name == *"lag" || name == *"nth_value" {
                if i == 1 {
                    args.push(Expr::Literal {
                        span: None,
                        lit: Literal::UInt64(self.rng.gen_range(1..=10)),
                    })
                } else {
                    args.push(self.gen_expr(ty))
                }
            } else if name == "factorial" {
                args.push(Expr::Literal {
                    span: None,
                    lit: Literal::UInt64(self.rng.gen_range(0..=20)),
                })
            } else {
                args.push(self.gen_expr(ty))
            }
        }

        let name = Identifier::from_name(name);
        Expr::FunctionCall {
            span: None,
            distinct,
            name,
            args,
            params,
            window,
            lambda,
        }
    }
}
