// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;

use crate::contexts::FuseQueryContextRef;
use crate::datasources::Partition;
use crate::datastreams::SendableDataBlockStream;
use crate::datavalues::DataSchemaRef;
use crate::error::FuseQueryResult;
use crate::planners::{PlanNode, ReadDataSourcePlan};

#[async_trait]
pub trait ITable: Sync + Send {
    fn name(&self) -> &str;

    fn schema(&self) -> FuseQueryResult<DataSchemaRef>;

    fn read_plan(
        &self,
        ctx: FuseQueryContextRef,
        push_down_plan: PlanNode,
    ) -> FuseQueryResult<ReadDataSourcePlan>;

    async fn read(
        &self,
        ctx: FuseQueryContextRef,
        parts: Vec<Partition>,
    ) -> FuseQueryResult<SendableDataBlockStream>;
}
