// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::ReadDataSourcePlan;
use common_streams::CorrectWithSchemaStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;
use crate::sessions::FuseQueryContextRef;

pub struct SourceTransform {
    ctx: FuseQueryContextRef,
    source_plan: ReadDataSourcePlan,
}

impl SourceTransform {
    pub fn try_create(ctx: FuseQueryContextRef, source_plan: ReadDataSourcePlan) -> Result<Self> {
        Ok(SourceTransform { ctx, source_plan })
    }
}

#[async_trait::async_trait]
impl Processor for SourceTransform {
    fn name(&self) -> &str {
        "SourceTransform"
    }

    fn connect_to(&mut self, _: Arc<dyn Processor>) -> Result<()> {
        Result::Err(ErrorCode::LogicalError(
            "Cannot call SourceTransform connect_to",
        ))
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        vec![Arc::new(EmptyProcessor::create())]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let db = self.source_plan.db.clone();
        let table = self.source_plan.table.clone();
        let remote = self.source_plan.remote;

        tracing::debug!(
            "execute, table:{:#}.{:#}, is_remote:{:#}...",
            db,
            table,
            remote
        );

        let table_meta = self.ctx.get_table(db.as_str(), table.as_str())?;
        let table = table_meta.get_inner();
        let table_stream = table.read(self.ctx.clone(), &self.source_plan);

        // We need to keep the block struct with the schema
        // Because the table may not support require columns
        Ok(Box::pin(CorrectWithSchemaStream::new(
            table_stream.await?,
            self.source_plan.schema.clone(),
        )))
    }
}
