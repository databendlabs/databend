// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::datablocks::DataBlock;
use crate::datasources::Partition;
use crate::datavalues::DataSchemaRef;
use crate::error::Result;
use crate::planners::{PlanNode, ReadDataSourcePlan};

pub enum TableType {
    Memory,
}

pub trait ITable: Sync + Send {
    fn name(&self) -> &str;

    fn table_type(&self) -> TableType;

    // Return the schema of this datasource.
    fn schema(&self) -> Result<DataSchemaRef>;

    // Return the ReadDataSourcePlan that how to read the datasource.
    // Here we can push down some plans(Filter/Limit/Project) to datasource for optimizer.
    // ReadDataSourcePlan determines the number of parallel executors(transforms) on processor pipeline.
    fn read_plan(&self, plans: Vec<PlanNode>) -> Result<ReadDataSourcePlan>;

    fn read_partition(&self, part: &Partition) -> Result<DataBlock>;
}
