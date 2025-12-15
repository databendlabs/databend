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

use std::collections::HashMap;
use std::io::Write;

use comfy_table::Table;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::Domain;
use databend_common_expression::Evaluator;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionFactory;
use databend_common_expression::Value;
use databend_common_expression::type_check;
use databend_common_expression::types::NullableColumn;
use databend_common_functions::BUILTIN_FUNCTIONS;
use goldenfile::Mint;
use itertools::Itertools;

mod arithmetic;
mod array;
mod binary;
mod bitmap;
mod boolean;
mod cast;
mod comparison;
mod control;
mod datetime;
mod geo;
// NOTE:(everpcpc) result different on macos
// TODO: fix this in running on linux
#[cfg(not(target_os = "macos"))]
mod geo_h3;
mod geography;
mod geometry;
mod hash;
mod map;
mod math;
mod misc;
mod obfuscator;
mod other;
mod regexp;
mod string;
mod tuple;
mod variant;
mod vector;

pub use databend_common_functions::test_utils as parser;

#[derive(Clone)]
pub struct TestContext<'a> {
    pub entries: &'a [(&'a str, BlockEntry)],
    pub input_domains: Option<&'a [(&'a str, Domain)]>,
    pub func_ctx: FunctionContext,
    pub strict_eval: bool,
}

impl Default for TestContext<'_> {
    fn default() -> Self {
        Self {
            entries: &[],
            input_domains: None,
            func_ctx: FunctionContext::default(),
            strict_eval: true,
        }
    }
}

impl<'a> TestContext<'a> {
    pub fn input_domains(&mut self) -> HashMap<usize, Domain> {
        self.entries
            .iter()
            .map(|(name, entry)| {
                self.input_domains
                    .and_then(|domains| {
                        domains
                            .iter()
                            .find(|(n, _)| n == name)
                            .map(|(_, domain)| domain.clone())
                    })
                    .unwrap_or_else(|| match entry {
                        BlockEntry::Const(scalar, data_type, _) => {
                            scalar.as_ref().domain(data_type)
                        }
                        BlockEntry::Column(column) => column.domain(),
                    })
            })
            .enumerate()
            .collect()
    }
}

pub fn run_ast(file: &mut impl Write, text: impl AsRef<str>, columns: &[(&str, Column)]) {
    let entries = &columns
        .iter()
        .map(|(name, column)| (*name, column.clone().into()))
        .collect::<Vec<_>>();

    run_ast_with_context(file, text, TestContext {
        entries,
        func_ctx: FunctionContext::default(),
        input_domains: None,
        strict_eval: true,
    })
}

pub fn run_ast_with_context(file: &mut impl Write, text: impl AsRef<str>, mut ctx: TestContext) {
    let text = text.as_ref();

    let result: Result<_> = try {
        let raw_expr = parser::parse_raw_expr(
            text,
            &ctx.entries
                .iter()
                .map(|(name, entry)| (*name, entry.data_type()))
                .collect::<Vec<_>>(),
        );

        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS)?;
        let expr = type_check::rewrite_function_to_cast(expr);

        let input_domains = ctx.input_domains();

        let (optimized_expr, output_domain) = ConstantFolder::fold_with_domain(
            &expr,
            &input_domains,
            &ctx.func_ctx,
            &BUILTIN_FUNCTIONS,
        );

        let remote_expr = optimized_expr.as_remote_expr();
        let optimized_expr = remote_expr.as_expr(&BUILTIN_FUNCTIONS);

        let num_rows = ctx
            .entries
            .iter()
            .map(|(_, entry)| entry.len())
            .max()
            .unwrap_or(1);
        let block = DataBlock::new(
            ctx.entries.iter().map(|(_, entry)| entry.clone()).collect(),
            num_rows,
        );

        ctx.entries.iter().for_each(|(_, entry)| {
            if let BlockEntry::Column(col) = entry {
                test_arrow_conversion(col, false);
            }
        });

        let evaluator = Evaluator::new(&block, &ctx.func_ctx, &BUILTIN_FUNCTIONS);
        let result = evaluator.run(&expr);
        let optimized_result = evaluator.run(&expr);
        match &result {
            Ok(result) => assert!(
                result.semantically_eq(&optimized_result.clone().unwrap()),
                "{} should eq {}, expr: {}, optimized_expr: {}",
                result,
                optimized_result.unwrap(),
                expr.sql_display(),
                optimized_expr.sql_display()
            ),
            Err(e) => {
                let optimized_err = optimized_result.unwrap_err();
                // assert_eq!(e.message(), optimized_err.message());
                assert_eq!(e.span(), optimized_err.span());
            }
        }

        (
            raw_expr,
            expr,
            input_domains,
            optimized_expr,
            output_domain
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "Unknown".to_string()),
            result?,
        )
    };

    match result {
        Ok((raw_expr, expr, input_domains, optimized_expr, output_domain, result)) => {
            writeln!(file, "ast            : {text}").unwrap();
            writeln!(file, "raw expr       : {raw_expr}").unwrap();
            writeln!(file, "checked expr   : {expr}").unwrap();
            if optimized_expr != expr {
                writeln!(file, "optimized expr : {optimized_expr}").unwrap();
            }
            if ctx.func_ctx != FunctionContext::default() {
                writeln!(file, "func ctx       : (modified)").unwrap();
            }

            match result {
                Value::Scalar(output_scalar) => {
                    writeln!(file, "output type    : {}", expr.data_type()).unwrap();
                    writeln!(file, "output domain  : {output_domain}").unwrap();
                    writeln!(file, "output         : {}", output_scalar.as_ref()).unwrap();
                }
                Value::Column(output_col) => {
                    test_arrow_conversion(&output_col, ctx.strict_eval);

                    // Only display the used input columns
                    let used_columns = raw_expr
                        .column_refs()
                        .keys()
                        .cloned()
                        .sorted()
                        .collect::<Vec<_>>();
                    let input_domains = used_columns
                        .iter()
                        .cloned()
                        .map(|i| input_domains[&i].clone())
                        .collect::<Vec<_>>();
                    let columns = used_columns
                        .into_iter()
                        .map(|i| ctx.entries[i].clone())
                        .collect::<Vec<_>>();

                    let mut table = Table::new();
                    table.load_preset("||--+-++|    ++++++");

                    let mut header = vec!["".to_string()];
                    header.extend(columns.iter().map(|(name, _)| name.to_string()));
                    header.push("Output".to_string());
                    table.set_header(header);

                    let mut type_row = vec!["Type".to_string()];
                    type_row.extend(columns.iter().map(|(_, c)| c.data_type().to_string()));
                    type_row.push(expr.data_type().to_string());
                    table.add_row(type_row);

                    let mut domain_row = vec!["Domain".to_string()];
                    domain_row.extend(input_domains.iter().map(|domain| domain.to_string()));
                    domain_row.push(output_domain.to_string());
                    table.add_row(domain_row);

                    for i in 0..output_col.len() {
                        let mut row = vec![format!("Row {i}")];
                        for (_, col) in columns.iter() {
                            let value = col.index(i).unwrap();
                            row.push(format!("{}", value));
                        }
                        row.push(format!("{}", output_col.index(i).unwrap()));
                        table.add_row(row);
                    }

                    writeln!(file, "evaluation:\n{table}").unwrap();

                    let mut table = Table::new();
                    table.load_preset("||--+-++|    ++++++");

                    table.set_header(["Column", "Data"]);

                    for (name, col) in columns.iter() {
                        table.add_row(&[name.to_string(), format!("{col:?}")]);
                    }

                    table.add_row(["Output".to_string(), format!("{output_col:?}")]);

                    writeln!(file, "evaluation (internal):\n{table}").unwrap();
                }
            }
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "{}\n", err.display_with_sql(text).message()).unwrap();
        }
    }
}

fn test_arrow_conversion(col: &Column, strict: bool) {
    let data_type = col.data_type();
    let col = if !strict {
        match col {
            Column::Decimal(decimal) => Column::Decimal(decimal.clone().strict_decimal()),
            col @ Column::Nullable(nullable) => match &nullable.column {
                Column::Decimal(decimal) => Column::Nullable(Box::new(NullableColumn {
                    column: Column::Decimal(decimal.clone().strict_decimal()),
                    validity: nullable.validity.clone(),
                })),
                _ => col.clone(),
            },
            col => col.clone(),
        }
    } else {
        col.clone()
    };

    let arrow_col = col.clone().into_arrow_rs();
    let new_col = Column::from_arrow_rs(arrow_col, &data_type).unwrap();
    assert_eq!(col, new_col, "arrow conversion went wrong");
}

#[test]
fn list_all_builtin_functions() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("function_list.txt").unwrap();

    let fn_registry = &BUILTIN_FUNCTIONS;

    writeln!(file, "Function aliases (alias to origin):").unwrap();
    for (alias_name, original_name) in fn_registry
        .aliases
        .iter()
        .sorted_by_key(|(alias_name, _)| alias_name.to_string())
    {
        writeln!(file, "{alias_name} -> {original_name}").unwrap();
    }
    writeln!(file, "\nFunctions overloads:").unwrap();
    let mut funcs = fn_registry
        .funcs
        .values()
        .flatten()
        .map(|(func, id)| {
            (
                (func.signature.name.clone(), *id),
                format!("{}", func.signature),
            )
        })
        .collect::<Vec<_>>();
    fn_registry
        .factories
        .iter()
        .flat_map(|(name, funcs)| {
            funcs.iter().map(move |(factor, id)| {
                let display = match factor {
                    FunctionFactory::Closure(_) => format!("{name} FACTORY"),
                    FunctionFactory::Helper(helper) => {
                        format!("{name} {helper:?}")
                    }
                };
                ((name.clone(), *id), display)
            })
        })
        .collect_into(&mut funcs);
    funcs.sort_by_key(|(key, _)| key.clone());
    for ((_, id), sig) in funcs {
        writeln!(file, "{id} {sig}").unwrap();
    }
}

#[test]
fn check_ambiguity() {
    BUILTIN_FUNCTIONS.check_ambiguity()
}

#[test]
fn test_if_function() -> Result<()> {
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::*;
    let raw_expr = parser::parse_raw_expr("if(eq(n,1), sum_sid + 1,100)", &[
        ("n", UInt8Type::data_type()),
        ("sum_sid", Int32Type::data_type().wrap_nullable()),
    ]);
    let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS)?;
    let block = DataBlock::new(
        vec![
            UInt8Type::from_data(vec![2_u8, 1]).into(),
            BlockEntry::new_const_column(
                Int32Type::data_type().wrap_nullable(),
                Scalar::Number(NumberScalar::Int32(2400)),
                2,
            ),
        ],
        2,
    );
    let func_ctx = FunctionContext::default();
    let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
    let result = evaluator.run(&expr).unwrap();
    let result = result
        .as_column()
        .unwrap()
        .clone()
        .as_nullable()
        .unwrap()
        .clone();

    let bm = Bitmap::from_iter([true, true]);
    assert_eq!(result.validity, bm);
    assert_eq!(result.column, Int64Type::from_data(vec![100, 2401]));
    Ok(())
}
