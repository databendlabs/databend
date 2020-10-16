// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use crate::datatypes::DataSchemaRef;
use crate::error::Result;
use crate::planners::{PlanNode, ReadDataSourcePlan};

pub trait IDataSourceProvider {
    fn get_table(&self, db: String, table: String) -> Result<Arc<dyn ITable>>;
}

pub trait ITable {
    // Return the schema of this datasource.
    fn schema(&self) -> Result<DataSchemaRef>;

    // Return the ReadDataSourcePlan that how to read the datasource.
    // Here we can push down some plans(Filter/Limit/Project) to datasource for optimizer.
    // ReadDataSourcePlan determines the number of parallel executors(transforms) on processor pipeline.
    fn read_plan(&self, plans: Vec<PlanNode>) -> Result<ReadDataSourcePlan>;
}
