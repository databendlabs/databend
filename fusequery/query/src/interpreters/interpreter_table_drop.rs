// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;
use common_planners::DropTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::FuseQueryContextRef;

pub struct DropTableInterpreter {
    ctx: FuseQueryContextRef,
    plan: DropTablePlan,
}

impl DropTableInterpreter {
    pub fn try_create(ctx: FuseQueryContextRef, plan: DropTablePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(DropTableInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for DropTableInterpreter {
    fn name(&self) -> &str {
        "DropTableInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let datasource = self.ctx.get_datasource();
        let database = datasource.get_database(self.plan.db.as_str())?;
        database.drop_table(self.plan.clone()).await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
