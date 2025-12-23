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

use databend_common_expression::FromData;
use databend_common_expression::types::*;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_hash() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("hash.txt").unwrap();

    test_md5(file);
    test_sha(file);
    test_blake3(file);
    test_sha2(file);
    test_city64withseed(file);
    test_siphash64(file);
    test_xxhash64(file);
    test_xxhash32(file);
}

fn test_md5(file: &mut impl Write) {
    run_ast(file, "md5('Abc')", &[]);
    run_ast(file, "md5(NULL)", &[]);
    run_ast(file, "md5(a)", &[(
        "a",
        StringType::from_data(vec!["Abc", "Dobrý den", "ß😀山"]),
    )]);
}

fn test_sha(file: &mut impl Write) {
    run_ast(file, "sha('Abc')", &[]);
    run_ast(file, "sha(NULL)", &[]);
    run_ast(file, "sha1(a)", &[(
        "a",
        StringType::from_data(vec!["Abc", "Dobrý den", "ß😀山"]),
    )]);
}

fn test_blake3(file: &mut impl Write) {
    run_ast(file, "blake3('Abc')", &[]);
    run_ast(file, "blake3(NULL)", &[]);
    run_ast(file, "blake3(a)", &[(
        "a",
        StringType::from_data(vec!["Abc", "Dobrý den", "ß😀山"]),
    )]);
}

fn test_sha2(file: &mut impl Write) {
    run_ast(file, "sha2('Abc',0)", &[]);
    run_ast(file, "sha2('Abc',256)", &[]);
    run_ast(file, "sha2(NULL,0)", &[]);
    run_ast(file, "sha2(a,b)", &[
        (
            "a",
            StringType::from_data(vec!["Abc", "Dobrý den", "ß😀山"]),
        ),
        ("b", UInt16Type::from_data(vec![224u16, 384, 512])),
    ]);
}

fn test_city64withseed(file: &mut impl Write) {
    run_ast(file, "city64withseed('Abc',0)", &[]);
    run_ast(file, "city64withseed('Abc',256)", &[]);
    run_ast(file, "city64withseed('Abc',256.3)", &[]);
    run_ast(file, "city64withseed(to_datetime(100000), 1234)", &[]);
    run_ast(file, "city64withseed(1234567890, 12)", &[]);
    run_ast(file, "city64withseed(1.1, 12)", &[]);
    run_ast(file, "city64withseed(1.1::decimal(10,1),12)", &[]);
    run_ast(file, "city64withseed(1.1::decimal(40,1),12)", &[]);
    run_ast(file, "city64withseed(1.1::decimal(41,1),12)", &[]);
    run_ast(file, "city64withseed('1234567890', 12.12)", &[]);
    run_ast(file, "city64withseed(1234567890, 12.12)", &[]);
    run_ast(file, "city64withseed(to_date(100000), 1234)", &[]);
    run_ast(file, "city64withseed(NULL,0)", &[]);
    run_ast(file, "city64withseed(a,b)", &[
        (
            "a",
            StringType::from_data(vec!["Abc", "Dobrý den", "ß😀山"]),
        ),
        ("b", UInt16Type::from_data(vec![10u16, 11, 12])),
    ]);

    let size = DecimalSize::new(10, 0).unwrap();
    run_ast(file, "city64withseed(a,5)", &[(
        "a",
        Decimal128Type::from_data_with_size([0, 1, 2], Some(size)),
    )]);
    run_ast(file, "city64withseed(a,5)", &[(
        "a",
        Decimal256Type::from_data_with_size([i256::from(0), i256::from(20)], Some(size)),
    )]);
    run_ast(file, "city64withseed(a,5)", &[(
        "a",
        Decimal256Type::from_data_with_size(
            [i256::from(0), i256::from(20)],
            Some(DecimalSize::new(40, 0).unwrap()),
        ),
    )]);
}

fn test_siphash64(file: &mut impl Write) {
    run_ast(file, "siphash64('Abc')", &[]);
    run_ast(file, "siphash64(to_datetime(100000))", &[]);
    run_ast(file, "siphash64(1234567890)", &[]);
    run_ast(file, "siphash64([1, 2, 3, null])", &[]);
    run_ast(file, "siphash64(['ab', 'cd', null, 'ef'])", &[]);
    run_ast(file, "siphash64(1.1)", &[]);
    run_ast(file, "siphash64(to_date(100000))", &[]);
    run_ast(file, "siphash64(NULL)", &[]);
    run_ast(file, "siphash64(parse_json('{\"a\":1}'))", &[]);
    run_ast(file, "siphash(true)", &[]);
    run_ast(file, "siphash64(a)", &[(
        "a",
        StringType::from_data(vec!["Dobrý den", "ß😀山"]),
    )]);
}

fn test_xxhash64(file: &mut impl Write) {
    run_ast(file, "xxhash64('Abc')", &[]);
    run_ast(file, "xxhash64(to_datetime(100000))", &[]);
    run_ast(file, "xxhash64(1234567890)", &[]);
    run_ast(file, "xxhash64(1.1)", &[]);
    run_ast(file, "xxhash64(1.1::decimal(10,1))", &[]);
    run_ast(file, "xxhash64(1.1::decimal(40,1))", &[]);
    run_ast(file, "xxhash64(1.1::decimal(41,1))", &[]);
    run_ast(file, "xxhash64(to_date(100000))", &[]);
    run_ast(file, "xxhash64(NULL)", &[]);
    run_ast(file, "xxhash64(parse_json('{\"a\":1}'))", &[]);
    run_ast(file, "xxhash64(true)", &[]);
    run_ast(file, "xxhash64(a)", &[(
        "a",
        StringType::from_data(vec!["Dobrý den", "ß😀山"]),
    )]);

    let size = DecimalSize::new(10, 0).unwrap();
    run_ast(file, "xxhash64(a)", &[(
        "a",
        Decimal128Type::from_data_with_size([0, 1, 2], Some(size)),
    )]);
    run_ast(file, "xxhash64(a)", &[(
        "a",
        Decimal256Type::from_data_with_size([i256::from(0), i256::from(20)], Some(size)),
    )]);
    run_ast(file, "xxhash64(a)", &[(
        "a",
        Decimal256Type::from_data_with_size(
            [i256::from(0), i256::from(20)],
            Some(DecimalSize::new(40, 0).unwrap()),
        ),
    )]);
}

fn test_xxhash32(file: &mut impl Write) {
    run_ast(file, "xxhash32('Abc')", &[]);
    run_ast(file, "xxhash32(to_datetime(100000))", &[]);
    run_ast(file, "xxhash32(1234567890)", &[]);
    run_ast(file, "xxhash32(1.1)", &[]);
    run_ast(file, "xxhash32(1.1::decimal(10,1))", &[]);
    run_ast(file, "xxhash32(1.1::decimal(40,1))", &[]);
    run_ast(file, "xxhash32(1.1::decimal(41,1))", &[]);
    run_ast(file, "xxhash32(to_date(100000))", &[]);
    run_ast(file, "xxhash32(NULL)", &[]);
    run_ast(file, "xxhash32(parse_json('{\"a\":1}'))", &[]);
    run_ast(file, "xxhash32(true)", &[]);
    run_ast(file, "xxhash32(a)", &[(
        "a",
        StringType::from_data(vec!["Dobrý den", "ß😀山"]),
    )]);

    let size = DecimalSize::new(10, 0).unwrap();
    run_ast(file, "xxhash32(a)", &[(
        "a",
        Decimal128Type::from_data_with_size([0, 1, 2], Some(size)),
    )]);
    run_ast(file, "xxhash32(a)", &[(
        "a",
        Decimal256Type::from_data_with_size([i256::from(0), i256::from(20)], Some(size)),
    )]);
    run_ast(file, "xxhash32(a)", &[(
        "a",
        Decimal256Type::from_data_with_size(
            [i256::from(0), i256::from(20)],
            Some(DecimalSize::new(40, 0).unwrap()),
        ),
    )]);
}
