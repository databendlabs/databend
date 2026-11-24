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

use std::process::Command;

#[test]
fn missing_input_reports_error_and_exits_with_failure() -> std::io::Result<()> {
    let dir = tempfile::tempdir()?;
    let input = "no_such_file";
    let output = Command::new(env!("CARGO_BIN_EXE_table-meta-inspector"))
        .current_dir(dir.path())
        .args(["-t", "sg", "-i", input])
        .output()?;

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("StorageNotFound") && stderr.contains(input),
        "unexpected stderr: {stderr}"
    );
    Ok(())
}
