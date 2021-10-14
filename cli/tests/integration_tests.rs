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
use bendctl::error::CliError;
use predicates::prelude::*;
use tempfile::TempDir;

fn init_fusectl() -> Command {
    let mut cmd = Command::cargo_bin("bendctl").unwrap();
    cmd.current_dir("/tmp");
    cmd
}

fn raw_fusectl() -> Command {
    init_fusectl()
}

fn fusectl() -> assert_cmd::Command {
    assert_cmd::Command::from_std(raw_fusectl())
}

macro_rules! contains_all {
    ($l: expr) => {
        predicates::str::contains($l)
    };

    ($l: expr, $( $x:expr ),+ ) =>
    {
        // https://doc.rust-lang.org/rust-by-example/macros/repeat.html
        // tail recursion
        contains_all!($l).and(contains_all!($($x),+))
    };
}

#[test]
#[ignore]
fn basic() {
    fusectl()
        .arg("--help")
        .assert()
        .success()
        .stdout(contains_all!("USAGE"))
        .stderr("");
}

#[test]
#[ignore]
fn version() {
    fusectl()
        .arg("version")
        .assert()
        .success()
        .stdout(contains_all!(
            "Databend CLI",
            "Databend CLI SHA256",
            "Git commit",
            "Build date",
            "OS version"
        ))
        .stderr("");
}

#[test]
#[ignore]
fn package() -> Result<(), CliError> {
    let tmp_dir = TempDir::new()?;
    // fetch
    fusectl()
        .arg("package")
        .arg("fetch")
        .arg("v0.4.79-nightly")
        .arg("--databend_dir")
        .arg(tmp_dir.path().to_str().unwrap())
        .arg("--group")
        .arg("integration")
        .assert()
        .success()
        .stdout(contains_all!(
            "[ok] Tag v0.4.79-nightly",
            "[ok] Download",
            "[ok] Package switch to v0.4.79-nightly"
        ))
        .stderr("");

    // list
    fusectl()
        .arg("package")
        .arg("list")
        .arg("--databend_dir")
        .arg(tmp_dir.path().to_str().unwrap())
        .arg("--group")
        .arg("integration")
        .assert()
        .success()
        .stdout(contains_all!("Version", "v0.4.79-nightly"))
        .stderr("");

    // switch
    fusectl()
        .arg("package")
        .arg("switch")
        .arg("v0.4.79-nightly")
        .arg("--databend_dir")
        .arg(tmp_dir.path().to_str().unwrap())
        .arg("--group")
        .arg("integration")
        .assert()
        .success()
        .stdout(contains_all!("[ok] Package switch to v0.4.79-nightly"))
        .stderr("");
    tmp_dir.close()?;
    Ok(())
}
