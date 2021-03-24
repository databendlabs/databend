// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_trait::async_trait;
use fuse_query_datavalues::DataSchemaRef;

use crate::datastreams::SendableDataBlockStream;
use crate::error::FuseQueryResult;
use crate::planners::{PlanNode, ReadDataSourcePlan};
use crate::sessions::FuseQueryContextRef;

#[async_trait]
pub trait ITable: Sync + Send {
    fn name(&self) -> &str;
    fn engine(&self) -> &str;

    fn schema(&self) -> FuseQueryResult<DataSchemaRef>;

    fn read_plan(
        &self,
        ctx: FuseQueryContextRef,
        push_down_plan: PlanNode,
    ) -> FuseQueryResult<ReadDataSourcePlan>;

    async fn read(&self, ctx: FuseQueryContextRef) -> FuseQueryResult<SendableDataBlockStream>;
}
