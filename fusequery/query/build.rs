// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use vergen::vergen;
use vergen::Config;
use vergen::ShaKind;

fn main() {
    vergen_build();
    commit_authors();
}

fn vergen_build() {
    let mut config = Config::default();
    *config.git_mut().sha_kind_mut() = ShaKind::Short;
    vergen(config).expect("Build vergen error");
}

fn commit_authors() {
    let r = run_script::run_script!(
        r#"git shortlog HEAD --summary | perl -lnE 's/^\s+\d+\s+(.+)/  "$1",/; next unless $1; say $_' | sort | xargs"#
    );
    let output = match r {
        Ok((_, output, _)) => output,
        Err(e) => e.to_string()
    };
    println!("cargo:rustc-env=COMMIT_AUTHORS={}", output);
}
