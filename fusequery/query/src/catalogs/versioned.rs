// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::any::Any;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_streams::SendableDataBlockStream;

use crate::datasources::Table;
use crate::sessions::FuseQueryContextRef;

pub trait VersionedTable: Table {
    fn get_id(&self) -> u64;
    fn get_version(&self) -> u64;
}

pub(crate) struct VersionedTabImpl {
    id: u64,
    ver: u64,
    tbl: Arc<dyn Table>,
}

impl VersionedTabImpl {
    pub fn new(id: u64, ver: u64, tbl: Arc<dyn Table>) -> Self {
        VersionedTabImpl { id, ver, tbl }
    }
}

impl VersionedTable for VersionedTabImpl {
    fn get_id(&self) -> u64 {
        self.id
    }

    fn get_version(&self) -> u64 {
        self.ver
    }
}

#[async_trait::async_trait]
impl Table for VersionedTabImpl {
    fn name(&self) -> &str {
        self.tbl.name()
    }

    fn engine(&self) -> &str {
        self.tbl.engine()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> common_exception::Result<DataSchemaRef> {
        self.tbl.schema()
    }

    fn is_local(&self) -> bool {
        self.tbl.is_local()
    }

    fn read_plan(
        &self,
        ctx: FuseQueryContextRef,
        scan: &ScanPlan,
        partitions: usize,
    ) -> common_exception::Result<ReadDataSourcePlan> {
        self.tbl.read_plan(ctx, scan, partitions)
    }

    async fn read(
        &self,
        ctx: FuseQueryContextRef,
        source_plan: &ReadDataSourcePlan,
    ) -> common_exception::Result<SendableDataBlockStream> {
        self.tbl.read(ctx, source_plan).await
    }
}
