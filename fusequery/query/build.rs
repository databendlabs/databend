// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//
// https://github.com/rust-lang/rustfmt/blob/e1ab878ccb24cda1b9e1c48865b375230385fede/build.rs

use vergen::vergen;
use vergen::Config;
use vergen::ShaKind;

fn main() {
    let mut config = Config::default();
    *config.git_mut().sha_kind_mut() = ShaKind::Short;
    vergen(config).expect("Build vergen error");

    println!("cargo:rustc-env=COMMIT_AUTHORS={}", "bohu".to_string());
}
