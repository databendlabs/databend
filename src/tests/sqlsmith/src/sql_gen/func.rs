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
use common_expression::types::NumberDataType;
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
        let func_sig = unsafe { self.scalar_func_sigs.get_unchecked(idx) }.clone();

        self.gen_func(func_sig.name.clone(), vec![], func_sig.args_type)
    }

    pub(crate) fn gen_factory_scalar_func(&mut self, ty: &DataType) -> Expr {
        let (name, params, args_type) = match ty.remove_nullable() {
            DataType::String => {
                let idx = self.rng.gen_range(0..=4);
                let name = match idx {
                    0 => "char".to_string(),
                    1 => "concat".to_string(),
                    2 => "concat_ws".to_string(),
                    3 => "regexp_replace".to_string(),
                    4 => "regexp_substr".to_string(),
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
                } else {
                    let len = self.rng.gen_range(1..=6);
                    vec![DataType::String; len]
                };
                let params = vec![];
                (name, params, args_type)
            }
            DataType::Boolean => {
                let name = "regexp_like".to_string();
                let args_type = match self.rng.gen_range(2..=3) {
                    2 => vec![DataType::String; 2],
                    3 => vec![DataType::String; 3],
                    _ => unreachable!(),
                };
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
                        if i % 2 == 0 {
                            args_type.push(ty.clone());
                        } else {
                            args_type.push(DataType::Boolean);
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
