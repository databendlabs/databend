// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use colored::Colorize;

use crate::cmds::Config;

pub struct Env {
    pub conf: Config,
    pub prompt: String,
}

impl Env {
    pub fn create(conf: Config) -> Self {
        let namespace = conf.namespace.clone();
        Env {
            conf,
            prompt: format!("[{}] > ", namespace.green()),
        }
    }
}
