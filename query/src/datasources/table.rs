// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::InsertIntoPlan;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::TruncateTablePlan;
use common_streams::SendableDataBlockStream;

use crate::sessions::DatafuseQueryContextRef;

#[async_trait::async_trait]
pub trait Table: Sync + Send {
    fn name(&self) -> &str;
    fn engine(&self) -> &str;
    fn as_any(&self) -> &dyn Any;
    fn schema(&self) -> Result<DataSchemaRef>;
    // Is Local or Remote.
    fn is_local(&self) -> bool;
    // Get the read source plan.
    fn read_plan(
        &self,
        ctx: DatafuseQueryContextRef,
        scan: &ScanPlan,
        partitions: usize,
    ) -> Result<ReadDataSourcePlan>;
    // Read block data from the underling.
    async fn read(
        &self,
        ctx: DatafuseQueryContextRef,
        source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream>;

    // temporary added, pls feel free to rm it
    async fn append_data(
        &self,
        _ctx: DatafuseQueryContextRef,
        _insert_plan: InsertIntoPlan,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "append data for local table {} is not implemented",
            self.name()
        )))
    }

    async fn truncate(
        &self,
        _ctx: DatafuseQueryContextRef,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "truncate for local table {} is not implemented",
            self.name()
        )))
    }
}

pub type TablePtr = Arc<dyn Table>;
