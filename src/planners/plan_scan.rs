// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use crate::datavalues::DataSchemaRef;
use crate::planners::ExpressionPlan;

#[derive(Clone)]
pub struct ScanPlan {
    /// The name of the schema
    pub schema_name: String,

    /// The schema of the source data
    pub table_schema: DataSchemaRef,

    pub table_args: Option<ExpressionPlan>,

    /// Optional column indices to use as a projection
    pub projection: Option<Vec<usize>>,

    /// The schema description of the output
    pub projected_schema: DataSchemaRef,
}

impl ScanPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.projected_schema.clone()
    }
}
