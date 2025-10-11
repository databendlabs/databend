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

use std::io::Write;

use databend_common_expression::type_check;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::Domain;
use databend_common_expression::Expr;
use databend_common_expression::Scalar;
use databend_common_functions::test_utils::parse_raw_expr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_index::eliminate_cast;
use goldenfile::Mint;

pub fn parse_expr(text: &str, columns: &[(&str, DataType)]) -> Expr<String> {
    let raw_expr = parse_raw_expr(text, columns);
    let raw_expr = raw_expr.project_column_ref(|i| columns[*i].0.to_string());
    let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
    type_check::rewrite_function_to_cast(expr)
}

#[test]
fn test_eliminate_cast() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("test_eliminate_cast.txt").unwrap();

    fn n(n: i32) -> Scalar {
        Scalar::Number(n.into())
    }

    run_text(file, "a::string = '2'", &[(
        "a",
        Int32Type::data_type(),
        Domain::from_min_max(n(-2), n(3), &Int32Type::data_type()),
    )]);
}

fn run_text(file: &mut impl Write, text: &str, ctx: &[(&str, DataType, Domain)]) {
    writeln!(file, "text      : {text}").unwrap();
    writeln!(file, "ctx       : {ctx:?}").unwrap();

    let columns = ctx
        .iter()
        .map(|(name, data_type, _)| (*name, data_type.clone()))
        .collect::<Vec<_>>();
    let expr = parse_expr(text, &columns);
    writeln!(file, "expr      : {expr}").unwrap();

    let input_domains = ctx
        .iter()
        .map(|(name, _, domain)| (name.to_string(), domain.clone()))
        .collect();
    match eliminate_cast(&expr, input_domains) {
        Some(new_expr) => {
            writeln!(file, "rewrited  : {new_expr}").unwrap(); // typos:disable-line
        }
        None => {
            writeln!(file, "rewrited  : Unchange").unwrap(); // typos:disable-line
        }
    };
    writeln!(file).unwrap();
}
