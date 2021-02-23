// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use crate::error::FuseQueryResult;
use crate::sessions::{FuseQueryContext, FuseQueryContextRef};

pub fn try_create_context() -> FuseQueryResult<FuseQueryContextRef> {
    let ctx = FuseQueryContext::try_create()?;

    ctx.set_max_threads(8)?;
    Ok(ctx)
}
