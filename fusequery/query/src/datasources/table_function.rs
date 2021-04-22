// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use crate::datasources::ITable;

pub trait ITableFunction: Sync + Send + ITable {
    fn function_name(&self) -> &str;
    fn db(&self) -> &str;

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn ITable + 'a>
    where Self: 'a;
}
