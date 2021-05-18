// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;
use common_planners::DropDatabasePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::IInterpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::FuseQueryContextRef;

pub struct DropDatabaseInterpreter {
    ctx: FuseQueryContextRef,
    plan: DropDatabasePlan
}

impl DropDatabaseInterpreter {
    pub fn try_create(ctx: FuseQueryContextRef, plan: DropDatabasePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(DropDatabaseInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl IInterpreter for DropDatabaseInterpreter {
    fn name(&self) -> &str {
        "DropDatabaseInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let datasource = self.ctx.get_datasource();
        datasource.drop_database(self.plan.clone()).await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![]
        )))
    }
}
