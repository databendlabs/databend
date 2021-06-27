// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::env;

use common_exception::Result;

use crate::configs::Config;
use crate::sessions::FuseQueryContext;
use crate::sessions::FuseQueryContextRef;

pub fn try_create_context() -> Result<FuseQueryContextRef> {
    let mut config = Config::default();
    // Setup log dir to the tests directory.
    config.log_dir = env::current_dir()?
        .join("../../tests/data/logs")
        .display()
        .to_string();

    let ctx = FuseQueryContext::try_create(config)?;
    ctx.with_id("2021")?;
    ctx.get_settings().set_max_threads(8)?;

    Ok(ctx)
}
