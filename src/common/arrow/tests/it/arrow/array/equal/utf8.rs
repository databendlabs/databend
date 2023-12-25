// Copyright 2020-2022 Jorge C. Leitão
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

use databend_common_arrow::arrow::array::*;
use databend_common_arrow::arrow::offset::Offset;

use super::binary_cases;
use super::test_equal;

fn test_generic_string_equal<O: Offset>() {
    let cases = binary_cases();

    for (lhs, rhs, expected) in cases {
        let lhs = lhs.iter().map(|x| x.as_deref());
        let rhs = rhs.iter().map(|x| x.as_deref());
        let lhs = Utf8Array::<O>::from_trusted_len_iter(lhs);
        let rhs = Utf8Array::<O>::from_trusted_len_iter(rhs);
        test_equal(&lhs, &rhs, expected);
    }
}

#[test]
fn utf8_equal() {
    test_generic_string_equal::<i32>()
}

#[test]
fn large_utf8_equal() {
    test_generic_string_equal::<i64>()
}
