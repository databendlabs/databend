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
/// - Generate Fuse environment variables, e.g., version and authors.
pub fn setup() {
    if Path::new(".git/HEAD").exists() {
        println!("cargo:rerun-if-changed=.git/HEAD");
    }
    add_building_env_vars();
}

pub fn add_building_env_vars() {
    add_env_vergen();
    add_env_commit_authors();
    add_env_commit_version();
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

/// Generate Fuse style commit-version and keep it in environment variable FUSE_COMMIT_VERSION.
pub fn add_env_commit_version() {
    let build_semver = option_env!("VERGEN_BUILD_SEMVER");
    let git_sha = option_env!("VERGEN_GIT_SHA_SHORT");
    let rustc_semver = option_env!("VERGEN_RUSTC_SEMVER");
    let timestamp = option_env!("VERGEN_BUILD_TIMESTAMP");

    let ver = match (build_semver, git_sha, rustc_semver, timestamp) {
        (Some(v1), Some(v2), Some(v3), Some(v4)) => format!("{}-{}({}-{})", v1, v2, v3, v4),
        _ => String::new(),
    };

    println!("cargo:rustc-env=FUSE_COMMIT_VERSION={}", ver);
}
