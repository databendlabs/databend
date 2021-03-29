// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::Result;
use async_trait::async_trait;
use common_datavalues::DataSchemaRef;
use common_planners::{PlanNode, ReadDataSourcePlan};

use crate::datastreams::SendableDataBlockStream;
use crate::sessions::FuseQueryContextRef;

#[async_trait]
pub trait ITable: Sync + Send {
    fn name(&self) -> &str;
    fn engine(&self) -> &str;

    fn schema(&self) -> Result<DataSchemaRef>;

    fn read_plan(
        &self,
        ctx: FuseQueryContextRef,
        push_down_plan: PlanNode,
    ) -> Result<ReadDataSourcePlan>;

    async fn read(&self, ctx: FuseQueryContextRef) -> Result<SendableDataBlockStream>;
}
