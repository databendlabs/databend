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

use databend_common_expression::types::*;
use databend_common_expression::FromData;
use goldenfile::Mint;
use jsonb::OwnedJsonb;

use crate::scalars::run_ast;

#[test]
fn test_type_check() {
    let mut mint = Mint::new("tests/it/type_check/testdata");
    let json = "{}".parse::<OwnedJsonb>().unwrap().to_vec();

    let columns = [
        ("s", StringType::from_data(vec!["s"])),
        ("n_s", StringType::from_data(vec!["s"]).wrap_nullable(None)),
        ("i8", Int8Type::from_data(vec![0])),
        ("n_i8", Int8Type::from_data(vec![0]).wrap_nullable(None)),
        ("u8", UInt8Type::from_data(vec![0])),
        ("n_u8", UInt8Type::from_data(vec![0]).wrap_nullable(None)),
        ("i16", Int16Type::from_data(vec![0])),
        ("n_i16", Int16Type::from_data(vec![0]).wrap_nullable(None)),
        ("u16", UInt16Type::from_data(vec![0])),
        ("n_u16", UInt16Type::from_data(vec![0]).wrap_nullable(None)),
        ("i32", Int32Type::from_data(vec![0])),
        ("n_i32", Int32Type::from_data(vec![0]).wrap_nullable(None)),
        ("u32", UInt32Type::from_data(vec![0])),
        ("n_u32", UInt32Type::from_data(vec![0]).wrap_nullable(None)),
        ("i64", Int64Type::from_data(vec![0])),
        ("n_i64", Int64Type::from_data(vec![0]).wrap_nullable(None)),
        ("u64", UInt64Type::from_data(vec![0])),
        ("n_u64", UInt64Type::from_data(vec![0]).wrap_nullable(None)),
        ("f32", Float32Type::from_data(vec![0.0])),
        (
            "n_f32",
            Float32Type::from_data(vec![0.0]).wrap_nullable(None),
        ),
        ("f64", Float64Type::from_data(vec![0.0])),
        (
            "n_f64",
            Float64Type::from_data(vec![0.0]).wrap_nullable(None),
        ),
        ("b", BooleanType::from_data(vec![true])),
        (
            "n_b",
            BooleanType::from_data(vec![true]).wrap_nullable(None),
        ),
        ("d", DateType::from_data(vec![0])),
        ("n_d", DateType::from_data(vec![0]).wrap_nullable(None)),
        ("ts", TimestampType::from_data(vec![0])),
        (
            "n_ts",
            TimestampType::from_data(vec![0]).wrap_nullable(None),
        ),
        ("j", VariantType::from_data(vec![json.clone()])),
        (
            "n_j",
            VariantType::from_data(vec![json.clone()]).wrap_nullable(None),
        ),
        ("d128", Decimal128Type::from_data(vec![0_i128])),
        (
            "n_d128",
            Decimal128Type::from_data(vec![0_i128]).wrap_nullable(None),
        ),
        ("d256", Decimal256Type::from_data(vec![i256::ZERO])),
        (
            "n_d256",
            Decimal256Type::from_data(vec![i256::ZERO]).wrap_nullable(None),
        ),
    ];

    // 8 and 16 are just smaller 32.
    let size = ["32", "64"];

    let signed = size.iter().map(|s| format!("i{}", s)).collect::<Vec<_>>();
    let unsigned = size.iter().map(|s| format!("u{}", s)).collect::<Vec<_>>();
    let nullable_signed = size.iter().map(|s| format!("n_i{}", s)).collect::<Vec<_>>();
    let nullable_unsigned = size.iter().map(|s| format!("n_u{}", s)).collect::<Vec<_>>();
    let float = size
        .iter()
        .flat_map(|s| [format!("f{s}"), format!("n_f{s}")].into_iter())
        .collect::<Vec<_>>();
    let decimal = ["d128", "n_d128", "d256", "n_d256"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();

    let all_num = signed
        .iter()
        .chain(unsigned.iter())
        .chain(nullable_signed.iter())
        .chain(nullable_unsigned.iter())
        .chain(float.iter())
        .chain(decimal.iter())
        .collect::<Vec<_>>();

    for (name, types) in [
        ("signed", &signed),
        ("unsigned", &unsigned),
        ("nullable_signed", &nullable_signed),
        ("nullable_unsigned", &nullable_unsigned),
        ("float", &float),
        ("decimal", &decimal),
    ] {
        let file = &mut mint.new_goldenfile(format!("{name}.txt")).unwrap();
        let pair = types
            .iter()
            .flat_map(|lhs| all_num.iter().map(move |rhs| (lhs, *rhs)))
            .collect::<Vec<_>>();
        for (lhs, rhs) in pair {
            run_ast(file, format!("{lhs} > {rhs}"), &columns);
            run_ast(file, format!("{lhs} = {rhs}"), &columns);
        }

        for ty in types {
            run_ast(file, format!("{ty} > 1"), &columns);
            run_ast(file, format!("{ty} = 1"), &columns);
            run_ast(file, format!("1 > {ty}"), &columns);
            run_ast(file, format!("1 = {ty}"), &columns);

            run_ast(file, format!("{ty} > 1.0"), &columns);
            run_ast(file, format!("{ty} = 1.0"), &columns);
            run_ast(file, format!("1.0 > {ty}"), &columns);
            run_ast(file, format!("1.0 = {ty}"), &columns);

            run_ast(file, format!("{ty} > '1'"), &columns);
            run_ast(file, format!("{ty} = '1'"), &columns);
            run_ast(file, format!("'1' > {ty}"), &columns);
            run_ast(file, format!("'1' = {ty}"), &columns);

            run_ast(file, format!("{ty} > 1::uint64"), &columns);
            run_ast(file, format!("{ty} = 1::uint64"), &columns);
            run_ast(file, format!("1::uint64 > {ty}"), &columns);
            run_ast(file, format!("1::uint64 = {ty}"), &columns);
            run_ast(file, format!("{ty} = true"), &columns);
        }
    }
}
