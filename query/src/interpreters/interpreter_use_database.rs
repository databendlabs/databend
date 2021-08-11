// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_exception::Result;
use common_planners::UseDatabasePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::DatafuseQueryContextRef;

pub struct UseDatabaseInterpreter {
    ctx: DatafuseQueryContextRef,
    plan: UseDatabasePlan,
}

impl UseDatabaseInterpreter {
    pub fn try_create(
        ctx: DatafuseQueryContextRef,
        plan: UseDatabasePlan,
    ) -> Result<InterpreterPtr> {
        Ok(Arc::new(UseDatabaseInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for UseDatabaseInterpreter {
    fn name(&self) -> &str {
        "UseDatabaseInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        self.ctx.set_current_database(self.plan.db.clone())?;
        let schema = Arc::new(DataSchema::empty());
        Ok(Box::pin(DataBlockStream::create(schema, None, vec![])))
    }
}
