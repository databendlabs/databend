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

use common_tracing::tracing;
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
    set_env_config();
    add_env_git_tag();
    add_env_commit_authors();
    add_env_credits_info();
}

pub fn set_env_config() {
    let mut config = Config::default();
    *config.git_mut().sha_kind_mut() = ShaKind::Short;

    if let Err(e) = vergen(config) {
        eprintln!("{}", e);
    }
}

// Get the latest tag:
// git describe --tags --abbrev=0
// v0.6.99-nightly
pub fn add_env_git_tag() {
    let r = run_script::run_script!(r#"git describe --tags --abbrev=0"#);
    let tag = match r {
        Ok((_, output, _)) => output,
        Err(e) => e.to_string(),
    };
    println!("cargo:rustc-env=VERGEN_GIT_SEMVER={}", tag);
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

pub fn add_env_credits_info() {
    let metadata_command = cargo_metadata::MetadataCommand::new();

    let deps = match cargo_license::get_dependencies_from_cargo_lock(metadata_command, false, false)
    {
        Ok(v) => v,
        Err(err) => {
            tracing::error!("{:?}", err);
            vec![]
        }
    };

    let names: Vec<String> = deps.iter().map(|x| (&x.name).to_string()).collect();
    let versions: Vec<String> = deps.iter().map(|x| x.version.to_string()).collect();
    let licenses: Vec<String> = deps
        .iter()
        .map(|x| match &x.license {
            None => "UNKNOWN".to_string(),
            Some(license) => license.to_string(),
        })
        .collect();
    println!(
        "cargo:rustc-env=DATABEND_CREDITS_NAMES={}",
        names.join(", ")
    );
    println!(
        "cargo:rustc-env=DATABEND_CREDITS_VERSIONS={}",
        versions.join(", ")
    );
    println!(
        "cargo:rustc-env=DATABEND_CREDITS_LICENSES={}",
        licenses.join(", ")
    );
}
