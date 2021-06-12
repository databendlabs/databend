// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::path::Path;

use vergen::vergen;
use vergen::Config;
use vergen::ShaKind;

/// Setup building environment:
/// - Watch git HEAD to trigger a rebuild;
/// - Generate vergen instruction to setup environment variables for building Fuse components. See: https://docs.rs/vergen/5.1.8/vergen/ ;
/// - Generate Fuse environment variables, e.g., authors.
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
        r#"git shortlog HEAD --summary | perl -lnE 's/^\s+\d+\s+(.+)/  "$1",/; next unless $1; say $_' | sort | xargs"#
    );
    let authors = match r {
        Ok((_, output, _)) => output,
        Err(e) => e.to_string(),
    };
    println!("cargo:rustc-env=FUSE_COMMIT_AUTHORS={}", authors);
}
