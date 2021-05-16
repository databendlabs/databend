// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::FunctionCtx;

pub struct MockFunctionCtx;

impl FunctionCtx for MockFunctionCtx {
    fn current_database() -> &str {
        "default"
    }
}

