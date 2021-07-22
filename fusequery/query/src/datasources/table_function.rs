// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use crate::datasources::Table;

pub trait TableFunction: Sync + Send + Table {
    fn function_name(&self) -> &str;
    fn db(&self) -> &str;

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a;
}
