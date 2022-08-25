// Copyright 2021 Datafuse Labs.
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

use std::io::Write;

use comfy_table::Table;
use common_ast::DisplayError;
use common_expression::type_check;
use common_expression::types::DataType;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::ConstantFolder;
use common_expression::Evaluator;
use common_expression::FunctionContext;
use common_expression::RemoteExpr;
use common_expression::Value;
use common_functions_v2::scalars::builtin_functions;

mod arithmetic;
mod boolean;
mod control;
mod math;
mod parser;
mod string;

pub fn run_ast(file: &mut impl Write, text: &str, columns: &[(&str, DataType, Column)]) {
    let result = try {
        let raw_expr = parser::parse_raw_expr(
            text,
            &columns
                .iter()
                .map(|(name, ty, _)| (*name, ty.clone()))
                .collect::<Vec<_>>(),
        );

        let fn_registry = builtin_functions();
        let (expr, output_ty) = type_check::check(&raw_expr, &fn_registry)?;

        // Converting to and then back from `RemoteExpr` should not change anything.
        let remote_expr = RemoteExpr::from_expr(expr);
        let expr = remote_expr.into_expr(&fn_registry).unwrap();

        let input_domains = columns
            .iter()
            .map(|(_, _, col)| col.domain())
            .collect::<Vec<_>>();

        let constant_folder = ConstantFolder::new(&input_domains, FunctionContext::default());
        let (optimized_expr, output_domain) = constant_folder.fold(&expr);

        let num_rows = columns.iter().map(|col| col.2.len()).max().unwrap_or(0);
        let chunk = Chunk::new(
            columns
                .iter()
                .map(|(_, _, col)| Value::Column(col.clone()))
                .collect::<Vec<_>>(),
            num_rows,
        );

        columns.iter().for_each(|(_, _, col)| {
            test_arrow_conversion(col);
        });

        let evaluator = Evaluator::new(&chunk, FunctionContext::default());
        let result = evaluator.run(&expr);
        let optimized_result = evaluator.run(&optimized_expr);
        match &result {
            Ok(result) => assert!(
                result
                    .as_ref()
                    .sematically_eq(&optimized_result.unwrap().as_ref())
            ),
            Err(e) => assert_eq!(e, &optimized_result.unwrap_err()),
        }

        (
            raw_expr,
            expr,
            input_domains,
            output_ty,
            optimized_expr,
            output_domain
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "Unknown".to_string()),
            result?,
        )
    };

    match result {
        Ok((raw_expr, expr, input_domains, output_ty, optimized_expr, output_domain, result)) => {
            writeln!(file, "ast            : {text}").unwrap();
            writeln!(file, "raw expr       : {raw_expr}").unwrap();
            writeln!(file, "checked expr   : {expr}").unwrap();
            if optimized_expr != expr {
                writeln!(file, "optimized expr : {optimized_expr}").unwrap();
            }

            match result {
                Value::Scalar(output_scalar) => {
                    writeln!(file, "output type    : {output_ty}").unwrap();
                    writeln!(file, "output domain  : {output_domain}").unwrap();
                    writeln!(file, "output         : {}", output_scalar.as_ref()).unwrap();
                }
                Value::Column(output_col) => {
                    test_arrow_conversion(&output_col);

                    let mut table = Table::new();
                    table.load_preset("||--+-++|    ++++++");

                    let mut header = vec!["".to_string()];
                    header.extend(columns.iter().map(|(name, _, _)| name.to_string()));
                    header.push("Output".to_string());
                    table.set_header(header);

                    let mut type_row = vec!["Type".to_string()];
                    type_row.extend(columns.iter().map(|(_, ty, _)| ty.to_string()));
                    type_row.push(output_ty.to_string());
                    table.add_row(type_row);

                    let mut domain_row = vec!["Domain".to_string()];
                    domain_row.extend(input_domains.iter().map(|domain| domain.to_string()));
                    domain_row.push(output_domain.to_string());
                    table.add_row(domain_row);

                    for i in 0..output_col.len() {
                        let mut row = vec![format!("Row {i}")];
                        for (_, _, col) in columns.iter() {
                            let value = col.index(i).unwrap();
                            row.push(format!("{}", value));
                        }
                        row.push(format!("{}", output_col.index(i).unwrap()));
                        table.add_row(row);
                    }

                    writeln!(file, "evaluation:\n{table}").unwrap();

                    let mut table = Table::new();
                    table.load_preset("||--+-++|    ++++++");

                    table.set_header(&["Column", "Data"]);

                    for (name, _, col) in columns.iter() {
                        table.add_row(&[name.to_string(), format!("{col:?}")]);
                    }

                    table.add_row(["Output".to_string(), format!("{output_col:?}")]);

                    writeln!(file, "evaluation (internal):\n{table}").unwrap();
                }
            }
            write!(file, "\n\n").unwrap();
        }
        Err((Some(span), msg)) => {
            writeln!(file, "{}\n", span.display_error((text.to_string(), msg))).unwrap();
        }
        Err((None, msg)) => {
            writeln!(file, "error: {}\n", msg).unwrap();
        }
    }
}

fn test_arrow_conversion(col: &Column) {
    let arrow_col = col.as_arrow();
    let new_col = Column::from_arrow(&*arrow_col);
    assert_eq!(col, &new_col, "arrow conversion went wrong");
}
