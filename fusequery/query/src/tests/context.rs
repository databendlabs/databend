// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::env;

use common_exception::Result;

use crate::configs::Config;
use crate::sessions::FuseQueryContext;
use crate::sessions::FuseQueryContextRef;

pub fn try_create_context() -> Result<FuseQueryContextRef> {
    let mut conf = Config::default();
    conf.cluster_registry_uri = "local://".to_string();

    // Setup log dir to the tests directory.
    conf.log_dir = env::current_dir()?
        .join("../../tests/data/logs")
        .display()
        .to_string();

    let ctx = FuseQueryContext::try_create(conf)?;
    ctx.with_id("2021")?;
    ctx.set_max_threads(8)?;

    Ok(ctx)
}
