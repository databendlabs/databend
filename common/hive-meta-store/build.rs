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

use std::env;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::path::Path;
use std::process::Command;

use which::which;

#[allow(clippy::expect_fun_call)]
fn main() {
    let out_dir = env::var_os("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir);

    which("thrift").expect(
        "
              *********************************
              thrift not found, which is required to build this crate
              please install it by using your favorite package manager,
              e.g.
              - apt-get install thrift
              - brew install thrift
              *********************************
       ",
    );
    // thrift -out my_rust_program/src --gen rs -r Tutorial.thrift
    Command::new("thrift")
        .args(&[
            "-out",
            &dest_path.as_os_str().to_string_lossy(),
            "-gen",
            "rs",
            "-r",
            "src/idl/hms.thrift",
        ])
        .status()
        .unwrap();

    // unfortunately, the code that thrift generated contains attributes attributes
    // which will prevent us from using `include!` macro to embed the codes.
    //
    // If we `include!` the code generated directly, rustc will reports errors like this:
    //
    //  "error: an inner attribute is not permitted following an outer attribute"
    //
    // see also:
    // 1. https://github.com/rust-lang/rfcs/issues/752
    // 2. https://github.com/google/flatbuffers/pull/6410
    //
    // thus, we have to "patch" the code that thrift generated.

    let input_file_path = dest_path.join("hms.rs");
    let output_file_path = dest_path.join("hms_patched.rs");
    let input = BufReader::new(
        File::open(&input_file_path)
            .expect(format!("open generated file failure: {:?}", input_file_path).as_str()),
    );
    let mut output = File::create(output_file_path).expect("create output patch file failure");
    for line in input.lines() {
        let line = line.expect("readline failure");
        if !line.starts_with("#![") {
            std::writeln!(output, "{}", line).expect("write line to patched file failure");
        }
    }

    println!("cargo:rerun-if-changed=build.rs");
}
