// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::env;

use common_exception::Result;

use crate::clusters::Cluster;
use crate::configs::Config;
use crate::sessions::SessionManager;
use crate::sessions::SessionManagerRef;

pub fn try_create_sessions() -> Result<SessionManagerRef> {
    let mut config = Config::default();
    let cluster = Cluster::empty();

    // Setup log dir to the tests directory.
    config.log_dir = env::current_dir()?
        .join("../../tests/data/logs")
        .display()
        .to_string();

    SessionManager::from_conf(config, cluster)
}
