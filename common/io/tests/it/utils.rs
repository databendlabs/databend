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

use common_io::prelude::*;

#[test]
fn convert_test() {
    assert_eq!(convert_byte_size(1_f64), "1 B");
    assert_eq!(convert_byte_size(1022_f64), "1.02 KB");
    assert_eq!(convert_byte_size(1022_f64 * 10000000f64), "10.22 GB");

    assert_eq!(convert_number_size(1_f64), "1");
    assert_eq!(convert_number_size(1022_f64), "1.02 thousand");
    assert_eq!(convert_number_size(10222_f64), "10.22 thousand");
}

#[test]
fn path_test() {
    assert_eq!(get_abs_path("ab/c", "d"), "ab/c/d".to_string());
    assert_eq!(get_abs_path("/ab/c", "d"), "/ab/c/d".to_string());
    assert_eq!(get_abs_path("/ab/c", "/d/e"), "/ab/c/d/e".to_string());
}

#[test]
fn parse_escape() {
    let cases = vec![
        vec!["a", "a"],
        vec!["abc", "abc"],
        vec!["\t\nabc", "\t\nabc"],
        vec!["\\t\\nabc", "\t\nabc"],
    ];

    for c in cases {
        assert_eq!(parse_escape_bytes(c[0].as_bytes()), c[1].as_bytes());
    }
}
