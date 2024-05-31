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

#![deny(unused_crate_dependencies)]
#![allow(clippy::uninlined_format_args)]

mod git;

use std::env;
use std::path::Path;

use anyhow::Result;
use gix::Repository;
use log::error;
use vergen::EmitBuilder;

const VERSION_ERROR_MESSAGE: &str = "A valid version is required for MetaClient handshaking, you could either set the `DATABEND_RELEASE_VERSION` env var or use `git fetch` to get the latest tag";

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

pub fn setup_commit_authors() {
    match gix::discover(".") {
        Ok(repo) => {
            add_env_commit_authors(&repo);
        }
        Err(_) => {
            println!(
                "cargo:warning=failed to discover git repo, set DATABEND_COMMIT_AUTHORS=unknown"
            );
            println!("cargo:rustc-env=DATABEND_COMMIT_AUTHORS=unknown");
        }
    };
}

pub fn add_building_env_vars() {
    set_env_config();
    add_env_credits_info();
    add_target_features();
    add_env_version();
}

pub fn set_env_config() {
    EmitBuilder::builder()
        .fail_on_error()
        .build_timestamp()
        .cargo_features()
        .rustc_semver()
        .git_sha(true)
        .emit()
        .expect("Unable to generate build envs");
}

pub fn add_env_version() {
    let version = discover_version().expect(VERSION_ERROR_MESSAGE);
    println!("cargo:rustc-env=DATABEND_GIT_SEMVER={}", version);
}

fn discover_version() -> Result<String> {
    match env::var("DATABEND_RELEASE_VERSION") {
        Ok(ver) => Ok(ver),
        Err(_) => {
            // env var not set, try to get the latest git tag
            let repo = gix::discover(".")?;
            let tag = git::get_latest_tag(&repo)?;
            Ok(tag)
        }
    }
}

pub fn add_env_commit_authors(repo: &Repository) {
    match git::get_commit_authors(repo) {
        Ok(authors) => println!("cargo:rustc-env=DATABEND_COMMIT_AUTHORS={}", authors),
        Err(e) => println!("cargo:rustc-env=DATABEND_COMMIT_AUTHORS={}", e),
    }
}

pub fn add_env_credits_info() {
    let metadata_command = cargo_metadata::MetadataCommand::new();

    let opt = cargo_license::GetDependenciesOpt {
        avoid_dev_deps: false,
        avoid_build_deps: false,
        direct_deps_only: false,
        root_only: false,
    };

    let deps = match cargo_license::get_dependencies_from_cargo_lock(metadata_command, opt) {
        Ok(v) => v,
        Err(err) => {
            error!("{:?}", err);
            vec![]
        }
    };

    let names: Vec<String> = deps.iter().map(|x| (x.name).to_string()).collect();
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

pub fn add_target_features() {
    match env::var_os("CARGO_CFG_TARGET_FEATURE") {
        Some(var) => match var.into_string() {
            Ok(s) => println!("cargo:rustc-env=DATABEND_CARGO_CFG_TARGET_FEATURE={}", s),
            Err(_) => {
                println!("cargo:warning=CARGO_CFG_TARGET_FEATURE was not valid utf-8");
            }
        },
        None => {
            println!("cargo:warning=CARGO_CFG_TARGET_FEATURE was not set");
        }
    };
}
