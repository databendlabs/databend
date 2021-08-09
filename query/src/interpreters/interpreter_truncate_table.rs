// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;
use common_planners::TruncateTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::FuseQueryContextRef;

pub struct TruncateTableInterpreter {
    ctx: FuseQueryContextRef,
    plan: TruncateTablePlan,
}

impl TruncateTableInterpreter {
    pub fn try_create(ctx: FuseQueryContextRef, plan: TruncateTablePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(TruncateTableInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for TruncateTableInterpreter {
    fn name(&self) -> &str {
        "TruncateTableInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let table = self
            .ctx
            .get_table(self.plan.db.as_str(), self.plan.table.as_str())?;
        table
            .datasource()
            .truncate(self.ctx.clone(), self.plan.clone())
            .await?;
        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
