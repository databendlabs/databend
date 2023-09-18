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

use common_ast::ast::Expr;
use common_ast::ast::Identifier;
use common_ast::ast::Literal;
use common_expression::types::DataType;
use common_expression::types::DecimalDataType::Decimal128;
use common_expression::types::DecimalDataType::Decimal256;
use common_expression::types::DecimalSize;
use common_expression::types::NumberDataType;
use common_expression::types::ALL_FLOAT_TYPES;
use common_expression::types::ALL_INTEGER_TYPES;
use rand::Rng;

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

        self.gen_func(func_sig.name.clone(), vec![], func_sig.args_type)
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
                    5 => "to_sting".to_string(),
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
                let params = vec![];
                (name, params, args_type)
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
                let params = vec![];
                (name, params, args_type)
            }
            DataType::Number(_) => {
                let arithmetic = vec![
                    "plus",
                    "minus",
                    "multiply",
                    "divide",
                    "point_in_ellipses",
                    "point_in_polygon",
                    "regexp_instr",
                ];
                let name = arithmetic
                    .get(self.rng.gen_range(0..=3))
                    .unwrap()
                    .to_string();
                let args_type = if name == "point_in_ellipses" {
                    vec![DataType::Number(NumberDataType::Float64); 7]
                } else if name == "point_in_polygon" {
                    let mut args_type = vec![];
                    let arg1 = DataType::Tuple(vec![DataType::Number(NumberDataType::Float64); 3]);
                    let arg2 =
                        DataType::Array(Box::from(DataType::Number(NumberDataType::Float64)));
                    let arg3 = DataType::Array(Box::from(DataType::Number(NumberDataType::Int64)));
                    args_type.push(arg1);
                    args_type.push(arg2);
                    args_type.push(arg3);
                    args_type
                } else if name == "regexp_instr" {
                    match self.rng.gen_range(2..=6) {
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
                    }
                } else {
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
                };

                let params = vec![];
                (name, params, args_type)
            }
            DataType::Array(nested) => {
                let name = "array".to_string();
                let args_type = vec![DataType::Array(nested)];
                let params = vec![];
                (name, params, args_type)
            }
            DataType::Decimal(_) => {
                let decimal = vec!["to_float64", "to_folat32", "to_decimal", "try_to_decimal"];
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
            DataType::Tuple(tuple) => {
                let tuple_func = ["json_path_query", "tuple"];
                let name = tuple_func[self.rng.gen_range(0..=2)].to_string();
                let params = vec![];
                if name == "tuple" {
                    let args_type = vec![DataType::Tuple(tuple)];
                    (name, params, args_type)
                } else {
                    let args_type = vec![DataType::Variant, DataType::String];
                    (name, params, args_type)
                }
            }
            DataType::Variant => {
                let json = vec!["json_array", "json_object", "json_object_keep_null"];
                let name = json[self.rng.gen_range(0..=2)].to_string();
                let ty1 = self.gen_data_type();
                let ty2 = self.gen_data_type();
                let ty3 = self.gen_data_type();
                let args_type = vec![ty1, ty2, ty3];
                let params = vec![];
                (name, params, args_type)
            }
            _ => {
                // TODO: other factory functions
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
                    let params = vec![];
                    (name, params, args_type)
                } else {
                    return self.gen_scalar_value(ty);
                }
            }
        };

        self.gen_func(name, params, args_type)
    }

    pub(crate) fn gen_agg_func(&mut self, ty: &DataType) -> Expr {
        let (name, params, args_type) = match ty.remove_nullable() {
            DataType::Number(NumberDataType::UInt64) => {
                let idx = self.rng.gen_range(0..=7);
                let name = match idx {
                    0 => "approx_count_distinct".to_string(),
                    1 => "count".to_string(),
                    2 => "count_if".to_string(),
                    3 => "bitmap_and_count".to_string(),
                    4 => "bitmap_or_count".to_string(),
                    5 => "bitmap_xor_count".to_string(),
                    6 => "bitmap_not_count".to_string(),
                    7 => "intersect_count".to_string(),
                    _ => unreachable!(),
                };
                let args_type = if idx == 2 {
                    vec![self.gen_data_type(), DataType::Boolean]
                } else if (3..=6).contains(&idx) {
                    if self.rng.gen_bool(0.5) {
                        vec![DataType::Bitmap]
                    } else {
                        vec![DataType::Nullable(Box::new(DataType::Bitmap))]
                    }
                } else if idx == 7 {
                    if self.rng.gen_bool(0.5) {
                        vec![DataType::Bitmap; 2]
                    } else {
                        vec![DataType::Nullable(Box::new(DataType::Bitmap)); 2]
                    }
                } else {
                    vec![self.gen_data_type()]
                };
                let params = if idx == 7 {
                    vec![
                        Literal::UInt64(self.rng.gen_range(1..=10)),
                        Literal::UInt64(self.rng.gen_range(1..=10)),
                    ]
                } else {
                    vec![]
                };
                (name, params, args_type)
            }
            DataType::Number(NumberDataType::Float64) => {
                let idx = self.rng.gen_range(0..=15);
                let name = match idx {
                    0 => "avg".to_string(),
                    1 => "avg_if".to_string(),
                    2 => "covar_pop".to_string(),
                    3 => "covar_samp".to_string(),
                    4 => "kurtosis".to_string(),
                    5 => "median_tdigest".to_string(),
                    6 => "median".to_string(),
                    7 => "skewness".to_string(),
                    8 => "stddev_pop".to_string(),
                    9 => "stddev".to_string(),
                    10 => "std".to_string(),
                    11 => "stddev_samp".to_string(),
                    12 => "quantile".to_string(),
                    13 => "quantile_cont".to_string(),
                    14 => "quantile_tdigest".to_string(),
                    15 => "quantile_disc".to_string(),
                    _ => unreachable!(),
                };

                let args_type = if idx == 1 {
                    vec![self.gen_number_data_type(), DataType::Boolean]
                } else if idx == 2 || idx == 3 {
                    vec![self.gen_number_data_type(), self.gen_number_data_type()]
                } else {
                    vec![self.gen_number_data_type()]
                };

                let params = if idx >= 12 {
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
                let idx = self.rng.gen_range(0..=6);
                let name = match idx {
                    0 => "any".to_string(),
                    1 => "min".to_string(),
                    2 => "max".to_string(),
                    3 => "min_if".to_string(),
                    4 => "max_if".to_string(),
                    5 => "arg_min".to_string(),
                    6 => "arg_max".to_string(),
                    _ => unreachable!(),
                };
                let params = vec![];
                let args_type = if idx == 3 || idx == 4 {
                    vec![ty.clone(), DataType::Boolean]
                } else if idx == 5 || idx == 6 {
                    vec![ty.clone(), self.gen_simple_data_type()]
                } else {
                    vec![ty.clone()]
                };
                (name, params, args_type)
            }
        };

        self.gen_func(name, params, args_type)
    }

    fn gen_func(&mut self, name: String, params: Vec<Literal>, args_type: Vec<DataType>) -> Expr {
        let distinct = if name == *"count" {
            self.rng.gen_bool(0.5)
        } else {
            false
        };
        let name = Identifier::from_name(name);
        let args = args_type
            .iter()
            .map(|ty| self.gen_expr(ty))
            .collect::<Vec<_>>();

        Expr::FunctionCall {
            span: None,
            distinct,
            name,
            args,
            params,
            window: None,
            lambda: None,
        }
    }
}
