// Copyright 2020 Datafuse Labs.
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

use std::process::Command;

use assert_cmd::cargo::CommandCargoExt;

fn init_fusectl() -> Command {
    let mut cmd = Command::cargo_bin("datafuse-cli").unwrap();
    cmd.current_dir("/tmp");
    cmd
}

fn raw_fusectl() -> Command {
    let cmd = init_fusectl();
    cmd
}

fn fusectl() -> assert_cmd::Command {
    assert_cmd::Command::from_std(raw_fusectl())
}

#[test]
fn basic() {
    fusectl()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicates::str::contains("Prints help information"))
        .stderr("");
}
