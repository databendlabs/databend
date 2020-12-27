// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::datasources::Partitions;
use crate::datavalues::DataSchemaRef;

#[derive(Clone)]
pub struct ReadDataSourcePlan {
    pub db: String,
    pub table: String,
    pub schema: DataSchemaRef,
    pub partitions: Partitions,
    pub description: String,
}

impl ReadDataSourcePlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
