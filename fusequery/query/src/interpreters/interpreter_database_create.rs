// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;
use common_planners::CreateDatabasePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::IInterpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::FuseQueryContextRef;

#[derive(Debug)]
pub struct CreateDatabaseInterpreter {
    ctx: FuseQueryContextRef,
    plan: CreateDatabasePlan,
}

impl CreateDatabaseInterpreter {
    #[tracing::instrument(level = "debug", skip(ctx, plan), fields(ctx.id = ctx.get_id().as_str()))]
    pub fn try_create(
        ctx: FuseQueryContextRef,
        plan: CreateDatabasePlan,
    ) -> Result<InterpreterPtr> {
        Ok(Arc::new(CreateDatabaseInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl IInterpreter for CreateDatabaseInterpreter {
    fn name(&self) -> &str {
        "CreateDatabaseInterpreter"
    }

    #[tracing::instrument(level = "debug", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let datasource = self.ctx.get_datasource();
        datasource.create_database(self.plan.clone()).await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
