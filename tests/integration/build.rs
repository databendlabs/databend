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

use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use tests_framework::build_suites;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let destination = Path::new(&out_dir).join("tests.rs");
    let mut f = File::create(&destination).expect("create test file");

    let suites = build_suites(&"../suites".parse::<PathBuf>().unwrap());

    for suite in suites.iter() {
        for test in suite.tests.iter() {
            write!(
                f,
                include_str!("./test.tpl"),
                name = ["test", &suite.name, &test.name].join("_"),
                input = &test.input,
                result = &test.result,
            )
            .expect("write test")
        }
    }
}
