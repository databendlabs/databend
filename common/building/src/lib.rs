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

use std::path::Path;

use vergen::vergen;
use vergen::Config;
use vergen::ShaKind;

/// Setup building environment:
/// - Watch git HEAD to trigger a rebuild;
/// - Generate vergen instruction to setup environment variables for building databend components. See: https://docs.rs/vergen/5.1.8/vergen/ ;
/// - Generate databend environment variables, e.g., authors.
pub fn setup() {
    if Path::new(".git/HEAD").exists() {
        println!("cargo:rerun-if-changed=.git/HEAD");
    }
    add_building_env_vars();
}

pub fn add_building_env_vars() {
    add_env_vergen();
    add_env_commit_authors();
}

pub fn add_env_vergen() {
    let mut config = Config::default();
    *config.git_mut().sha_kind_mut() = ShaKind::Short;

    if let Err(e) = vergen(config) {
        eprintln!("{}", e);
    }
}

pub fn add_env_commit_authors() {
    let r = run_script::run_script!(
        // use email to uniq authors
        r#"git shortlog HEAD -sne | awk '{$1=""; sub(" ", "    \""); print }' | awk -F'<' '!x[$1]++' | \
        awk -F'<' '!x[$2]++' | awk -F'<' '{gsub(/ +$/, "\",", $1); print $1}' | sort | xargs"#
    );
    let authors = match r {
        Ok((_, output, _)) => output,
        Err(e) => e.to_string(),
    };
    println!("cargo:rustc-env=DATABEND_COMMIT_AUTHORS={}", authors);
}
