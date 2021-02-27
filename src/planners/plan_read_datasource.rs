// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::datasources::{Partitions, Statistics};
use crate::datavalues::DataSchemaRef;

#[derive(Clone)]
pub struct ReadDataSourcePlan {
    pub db: String,
    pub table: String,
    pub schema: DataSchemaRef,
    pub partitions: Partitions,
    pub statistics: Statistics,
    pub description: String,
}

impl ReadDataSourcePlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
