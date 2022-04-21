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

use std::fs;
use std::io::Write;

use common_exception::exception::Result;
use goldenfile::Mint;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Entry {
    pub file_name: String,
    pub file_txt: String,
}

pub type VersionRunner = fn(old_version_value: String) -> String;

// VersionRunner function primitive:
// Input is old_version_value
// Returns latest_version_value
// Then the goldenfile check the latest_version_value is same as the latest file in the testdata
pub fn version_check(dir: &str, runner: VersionRunner) -> Result<()> {
    let mut mint = Mint::new(dir);
    let mut entries = vec![];
    for file in fs::read_dir(dir)? {
        let file_name = file
            .as_ref()
            .unwrap()
            .file_name()
            .to_str()
            .unwrap()
            .to_string();
        let file_txt = fs::read_to_string(file.unwrap().path().to_str().unwrap())?;
        let entry = Entry {
            file_name,
            file_txt,
        };
        entries.push(entry);
    }
    entries.sort();

    let latest_entry = entries[entries.len() - 1].clone();
    let mut file = mint.new_goldenfile(latest_entry.file_name)?;
    for entry in entries {
        write!(file, "{}", runner(entry.file_txt))?;
    }
    Ok(())
}
