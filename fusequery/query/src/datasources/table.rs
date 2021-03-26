// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_trait::async_trait;
use common_datavalues::DataSchemaRef;
use common_planners::{PlanNode, ReadDataSourcePlan};

use crate::datastreams::SendableDataBlockStream;
use crate::error::FuseQueryResult;
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
