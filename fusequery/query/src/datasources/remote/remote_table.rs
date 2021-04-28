// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;

use common_exception::{Result, ErrorCodes};
use common_datavalues::DataSchemaRef;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::TableOptions;
use common_streams::SendableDataBlockStream;

use crate::datasources::ITable;
use crate::sessions::FuseQueryContextRef;

#[allow(dead_code)]
pub struct RemoteTable {
    pub(crate) db: String,
    name: String,
    schema: DataSchemaRef
}

impl RemoteTable {
    #[allow(dead_code)]
    pub fn try_create(
        db: String,
        name: String,
        schema: DataSchemaRef,
        _options: TableOptions
    ) -> Result<Box<dyn ITable>> {
        let table = Self { db, name, schema };
        Ok(Box::new(table))
    }
}

#[async_trait::async_trait]
impl ITable for RemoteTable {
    fn name(&self) -> &str {
        &self.name
    }

    fn engine(&self) -> &str {
        "remote"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn is_local(&self) -> bool {
        false
    }

    fn read_plan(&self, _ctx: FuseQueryContextRef, _scan: &ScanPlan) -> Result<ReadDataSourcePlan> {
        Result::Err(ErrorCodes::UnImplement("RemoteTable read_plan not yet implemented".to_string()))
    }

    async fn read(&self, _ctx: FuseQueryContextRef) -> Result<SendableDataBlockStream> {
        Result::Err(ErrorCodes::UnImplement("RemoteTable read not yet implemented".to_string()))
    }
}
