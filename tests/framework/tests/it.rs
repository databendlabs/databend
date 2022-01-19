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

use std::path::PathBuf;

use tests_framework::build_suites;
use tests_framework::build_tests;

#[test]
fn test_build_suites() {
    let suites = build_suites(&"../suites".parse::<PathBuf>().unwrap());

    assert_eq!(suites.len(), 3);
    assert_eq!(suites[0].name, "0_stateless");
}

#[test]
fn test_build_tests() {
    let tests = build_tests(&"../suites/0_stateless".parse::<PathBuf>().unwrap());

    assert!(tests.len() > 0);
    assert_eq!(tests[0].name, "00_0000_dummy_select_1");
}
