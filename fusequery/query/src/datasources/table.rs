// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_planners::InsertIntoPlan;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_streams::SendableDataBlockStream;

use crate::sessions::FuseQueryContextRef;

#[async_trait::async_trait]
pub trait ITable: Sync + Send {
    fn name(&self) -> &str;
    fn engine(&self) -> &str;
    fn as_any(&self) -> &dyn Any;
    fn schema(&self) -> Result<DataSchemaRef>;
    // Is Local or Remote.
    fn is_local(&self) -> bool;
    // Get the read source plan.
    fn read_plan(&self, ctx: FuseQueryContextRef, scan: &ScanPlan) -> Result<ReadDataSourcePlan>;
    // Read block data from the underling.
    async fn read(&self, ctx: FuseQueryContextRef) -> Result<SendableDataBlockStream>;

    // temporary added, pls feel free to rm it
    async fn append_data(
        &self,
        _ctx: FuseQueryContextRef,
        _insert_plan: InsertIntoPlan
    ) -> Result<()> {
        Err(ErrorCodes::UnImplement(format!(
            "append data for local table {} is not implemented",
            self.name()
        )))
    }
}
