// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use structopt::StructOpt;

#[derive(Clone, Debug, PartialEq, StructOpt)]
pub struct Config {
    #[structopt(long, env = "NAMESPACE", default_value = "test")]
    pub namespace: String,
}

impl Config {
    pub fn create() -> Self {
        Config {
            namespace: "test".to_string(),
        }
    }
}
