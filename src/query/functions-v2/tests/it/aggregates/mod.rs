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

mod agg;

use std::io::Write;

use comfy_table::Table;
use common_expression::type_check;
use common_expression::types::number::NumberScalar;
use common_expression::types::AnyType;
use common_expression::types::DataType;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::Evaluator;
use common_expression::RawExpr;
use common_expression::Scalar;
use common_expression::Value;
use common_functions_v2::aggregates::eval_aggr;
use common_functions_v2::scalars::builtin_functions;
use itertools::Itertools;

use super::scalars::parser;

/// run ast which is agg expr
pub fn run_agg_ast(file: &mut impl Write, text: &str, columns: &[(&str, DataType, Column)]) {
    let raw_expr = parser::parse_raw_expr(
        text,
        &columns
            .iter()
            .map(|(name, ty, _)| (*name, ty.clone()))
            .collect::<Vec<_>>(),
    );

    let num_rows = columns.iter().map(|col| col.2.len()).max().unwrap_or(0);
    let chunk = Chunk::new(
        columns
            .iter()
            .map(|(_, _, col)| Value::Column(col.clone()))
            .collect::<Vec<_>>(),
        num_rows,
    );

    let used_columns = raw_expr
        .column_refs()
        .into_iter()
        .sorted()
        .collect::<Vec<_>>();

    // For test only, we just support agg function call here
    let result: common_exception::Result<(Column, DataType)> = try {
        match raw_expr {
            common_expression::RawExpr::FunctionCall {
                name, params, args, ..
            } => {
                let args: Vec<(Value<AnyType>, DataType)> = args
                    .iter()
                    .map(|raw_expr| run_scalar_expr(raw_expr, &chunk))
                    .collect::<common_expression::Result<_>>()
                    .unwrap();

                let params = params
                    .iter()
                    .map(|p| Scalar::Number(NumberScalar::UInt64(*p as u64)))
                    .collect();

                let arg_types: Vec<DataType> = args.iter().map(|arg| arg.1.clone()).collect();
                let arg_columns: Vec<Column> = args
                    .iter()
                    .map(|arg| match &arg.0 {
                        Value::Scalar(s) => {
                            let builder = s.as_ref().repeat(chunk.num_rows());
                            builder.build()
                        }
                        Value::Column(c) => c.clone(),
                    })
                    .collect();

                eval_aggr(
                    name.as_str(),
                    params,
                    &arg_columns,
                    &arg_types,
                    chunk.num_rows(),
                )?
            }
            _ => unimplemented!(),
        }
    };

    match result {
        Ok((column, _)) => {
            writeln!(file, "ast: {text}").unwrap();
            {
                let mut table = Table::new();
                table.load_preset("||--+-++|    ++++++");
                table.set_header(["Column", "Data"]);

                let ids = match used_columns.is_empty() {
                    true => {
                        if columns.is_empty() {
                            vec![]
                        } else {
                            vec![0]
                        }
                    }
                    false => used_columns,
                };

                for id in ids.iter() {
                    let (name, _, col) = &columns[*id];
                    table.add_row(&[name.to_string(), format!("{col:?}")]);
                }
                table.add_row(["Output".to_string(), format!("{column:?}")]);
                writeln!(file, "evaluation (internal):\n{table}").unwrap();
            }
            write!(file, "\n\n").unwrap();
        }
        Err(e) => {
            writeln!(file, "error: {}\n", e.message()).unwrap();
        }
    }
}

pub fn run_scalar_expr(
    raw_expr: &RawExpr,
    chunk: &Chunk,
) -> common_expression::Result<(Value<AnyType>, DataType)> {
    let fn_registry = builtin_functions();
    let (expr, output_ty) = type_check::check(raw_expr, &fn_registry)?;
    let evaluator = Evaluator::new(chunk, chrono_tz::UTC);
    let result = evaluator.run(&expr)?;
    Ok((result, output_ty))
}
