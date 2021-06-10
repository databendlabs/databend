// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::sessions::FuseQueryContext;
use crate::sessions::FuseQueryContextRef;

pub fn try_create_context() -> Result<FuseQueryContextRef> {
    let ctx = FuseQueryContext::try_create()?;
    ctx.with_id("2021")?;

    ctx.set_max_threads(8)?;
    Ok(ctx)
}
