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

use databend_common_expression::FromData;
use databend_common_expression::types::*;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_feistel_obfuscate() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("obfuscator.txt").unwrap();

    run_ast(file, "feistel_obfuscate(a,0)", &[(
        "a",
        Int64Type::from_data(vec![1i64, -30, 1024, 10000, i64::MAX, i64::MIN, 0]),
    )]);
    run_ast(file, "feistel_obfuscate(a,0)", &[(
        "a",
        UInt64Type::from_data(vec![1u64, 30, 1024, u64::MAX, u64::MIN]),
    )]);
    run_ast(file, "feistel_obfuscate(a,0)", &[(
        "a",
        Float32Type::from_data(vec![0.0, -0.0, 30.0, f32::MIN, f32::MAX]),
    )]);
    run_ast(file, "feistel_obfuscate(a,0)", &[(
        "a",
        Float64Type::from_data(vec![0.0, -0.0, 30.0, f64::MIN, f64::MAX]),
    )]);
    run_ast(file, "feistel_obfuscate(null,0)", &[]);
}
