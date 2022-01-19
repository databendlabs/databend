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
#![feature(path_file_prefix)]

use std::cmp::Ord;
use std::collections::HashMap;
use std::fs::read_dir;
use std::path::Path;

#[derive(Debug, Default)]
pub struct Suite {
    pub name: String,
    pub tests: Vec<Test>,
}

#[derive(Debug, Default)]
pub struct Test {
    pub name: String,

    // Path of input data file.
    pub input: String,
    // The bindata of input, generated via `build.rs`.
    pub input_data: &'static str,

    // Path for expected result file.
    pub result: String,
    // The bindata of result, generated via `build.rs`.
    pub result_data: &'static str,
}

impl Test {
    /// TODO
    /// Maybe we need to extract as a trait so that we can support different
    /// test type?
    pub fn run(&self) {
        println!("Running {}", self.name)
    }
}

// Input path should the root of the suites.
pub fn build_suites(path: &Path) -> Vec<Suite> {
    let mut suites = Vec::new();

    for entry in read_dir(path).expect("read suites") {
        let entry = entry.expect("read suite entry");

        suites.push(Suite {
            name: entry.file_name().into_string().unwrap(),
            tests: build_tests(&entry.path()),
        });
    }

    suites
}

// Input path should the root of the tests.
pub fn build_tests(path: &Path) -> Vec<Test> {
    let mut tests: HashMap<String, Test> = HashMap::new();

    for entry in read_dir(path).expect("read tests") {
        let entry = entry.expect("read test entry");

        // TODO: we need to support dir in the future.
        if entry.file_type().expect("get file type").is_dir() {
            continue;
        }

        // Ignore all markdown and existing output files.
        if entry.path().ends_with(".md") || entry.path().ends_with(".output") {
            continue;
        }

        let name = entry
            .path()
            .file_prefix()
            .unwrap()
            .to_string_lossy()
            .to_string();

        match entry
            .path()
            .extension()
            .expect("file must have extension")
            .to_string_lossy()
            .as_ref()
        {
            "sql" => {
                let e = tests.entry(name.clone()).or_insert(Test {
                    name,
                    ..Test::default()
                });
                e.input = entry
                    .path()
                    .canonicalize()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string();
            }
            "result" => {
                let e = tests.entry(name.clone()).or_insert(Test {
                    name,
                    ..Test::default()
                });
                e.result = entry
                    .path()
                    .canonicalize()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string();
            }
            typ => {
                println!("not supported file type: {:?}", typ);
            }
        }
    }

    let mut tests = tests
        .into_values()
        .filter(
            // TODO: Filter empty result file for now.
            |v| !v.input.is_empty() && !v.result.is_empty(),
        )
        .collect::<Vec<Test>>();
    tests.sort_by(|x, y| x.name.cmp(&y.name));

    tests
}
