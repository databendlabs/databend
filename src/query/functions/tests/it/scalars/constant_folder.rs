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

use databend_common_exception::Result;
use databend_common_expression::type_check;
use databend_common_expression::types::*;
use databend_common_expression::Column;
use databend_common_expression::ConstantFolder;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_functions::BUILTIN_FUNCTIONS;
use goldenfile::Mint;

use super::parser;

fn run_ast_for_prune(file: &mut impl Write, text: impl AsRef<str>, columns: &[(&str, Column)]) {
    let text = text.as_ref();
    let result: Result<_> = try {
        let raw_expr = parser::parse_raw_expr(
            text,
            &columns
                .iter()
                .map(|(name, col)| (*name, col.data_type()))
                .collect::<Vec<_>>(),
        );

        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS)?;

        let input_domains = columns
            .iter()
            .map(|(_, col)| col.domain())
            .enumerate()
            .collect::<HashMap<_, _>>();

        let (optimized_expr, output_domain) = ConstantFolder::fold_for_prune(
            &expr,
            &input_domains,
            &FunctionContext::default(),
            &BUILTIN_FUNCTIONS,
        );

        (raw_expr, expr, input_domains, optimized_expr, output_domain)
    };

    match result {
        Ok((raw_expr, expr, input_domains, optimized_expr, output_domain)) => {
            writeln!(file, "ast            : {text}").unwrap();
            writeln!(file, "raw expr       : {raw_expr}").unwrap();
            writeln!(file, "checked expr   : {expr}").unwrap();
            writeln!(file, "input domain  : {input_domains:?}").unwrap();
            if optimized_expr != expr {
                writeln!(file, "optimized expr : {optimized_expr}").unwrap();
            }

            writeln!(file, "output type    : {}", expr.data_type()).unwrap();
            writeln!(file, "output domain  : {output_domain:?}").unwrap();

            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "{}\n", err.display_with_sql(text).message()).unwrap();
        }
    }
}

#[test]
fn test_fold_for_prune() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("fold_for_prune.txt").unwrap();

    let bool_domain = &[("x", BooleanType::from_data(vec![true, false]))];
    run_ast_for_prune(file, "to_int8(x) = 2", bool_domain);
    run_ast_for_prune(file, "x::decimal(1,0) = 2", bool_domain);
    run_ast_for_prune(file, "x::int8 = 1::int8", bool_domain);

    run_ast_for_prune(file, "x::decimal(5,0) = 1.2", &[(
        "x",
        Int8Type::from_data(vec![0, 2]),
    )]);
    run_ast_for_prune(file, "x = 1.2", &[("x", Int16Type::from_data(vec![0, 2]))]);
    run_ast_for_prune(file, "x::string = '+1'", &[(
        "x",
        Int16Type::from_data(vec![1, 100]),
    )]);
    run_ast_for_prune(file, "x::string = '+1'", &[(
        "x",
        Int16Type::from_data(vec![100, 200]),
    )]);
    run_ast_for_prune(file, "to_int32(to_int16(x)) = 1.2", &[(
        "x",
        Int8Type::from_data(vec![0, 2]),
    )]);
    run_ast_for_prune(file, "x::int8 = 10", &[(
        "x",
        Int32Type::from_data(vec![0, 6000]),
    )]);

    run_ast_for_prune(file, "x::datetime = '2021-03-05 01:01:01'", &[(
        "x",
        StringType::from_data(vec!["2021-03-05 01:01:01", "2021-03-05 01:01:02"]),
    )]);

    run_ast_for_prune(file, "x::datetime = '2021-03-05 01:01:03'", &[(
        "x",
        DateType::from_data(vec![18600, 19000]),
    )]);

    run_ast_for_prune(file, "to_int8(x) = 1.2", &[(
        "x",
        Int16Type::from_data(vec![0, 300]),
    )]);

    run_ast_for_prune(file, "x = 1::int8", &[(
        "x",
        Int16Type::from_data(vec![0, 100]).wrap_nullable(None),
    )]);

    run_ast_for_prune(file, "x::int8 null = 1::int8", &[(
        "x",
        Int16Type::from_data(vec![0, 100]).wrap_nullable(None),
    )]);
}
