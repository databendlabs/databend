// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::error::FuseQueryResult;
use crate::sessions::{FuseQueryContext, FuseQueryContextRef};

pub fn try_create_context() -> FuseQueryResult<FuseQueryContextRef> {
    let ctx = FuseQueryContext::try_create()?;

    ctx.set_max_threads(8)?;
    Ok(ctx)
}
