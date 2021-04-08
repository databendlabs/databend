// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;

use anyhow::Result;
use common_datavalues::DataSchemaRef;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use common_streams::SendableDataBlockStream;

use crate::sessions::FuseQueryContextRef;

#[async_trait::async_trait]
pub trait ITable: Sync + Send {
    fn name(&self) -> &str;
    fn engine(&self) -> &str;
    fn as_any(&self) -> &dyn Any;
    fn schema(&self) -> Result<DataSchemaRef>;

    // Get the read source plan.
    fn read_plan(
        &self,
        ctx: FuseQueryContextRef,
        push_down_plan: PlanNode,
    ) -> Result<ReadDataSourcePlan>;

    // Read block datas from the underfling.
    async fn read(&self, ctx: FuseQueryContextRef) -> Result<SendableDataBlockStream>;
}
