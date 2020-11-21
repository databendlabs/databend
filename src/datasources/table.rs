// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;

use crate::datasources::Partition;
use crate::datastreams::SendableDataBlockStream;
use crate::datavalues::DataSchemaRef;
use crate::error::FuseQueryResult;
use crate::planners::{PlanNode, ReadDataSourcePlan};

#[async_trait]
pub trait ITable: Sync + Send {
    fn name(&self) -> &str;

    fn schema(&self) -> FuseQueryResult<DataSchemaRef>;

    fn read_plan(&self, plans: Vec<PlanNode>) -> FuseQueryResult<ReadDataSourcePlan>;

    async fn read(&self, parts: Vec<Partition>) -> FuseQueryResult<SendableDataBlockStream>;
}
