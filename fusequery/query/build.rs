// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::path::Path;

use vergen::vergen;
use vergen::Config;
use vergen::ShaKind;

fn main() {
    if Path::new(".git/HEAD").exists() {
        println!("cargo:rerun-if-changed=.git/HEAD");
    }

    commit_version();
    commit_authors();
}

fn commit_version() {
    let mut config = Config::default();
    *config.git_mut().sha_kind_mut() = ShaKind::Short;

    if let Err(e) = vergen(config) {
        eprintln!("{}", e);
    }
}

fn commit_authors() {
    let r = run_script::run_script!(
        r#"git shortlog HEAD --summary | perl -lnE 's/^\s+\d+\s+(.+)/  "$1",/; next unless $1; say $_' | sort | xargs"#
    );
    let authors = match r {
        Ok((_, output, _)) => output,
        Err(e) => e.to_string(),
    };
    println!("cargo:rustc-env=COMMIT_AUTHORS={}", authors);
}
