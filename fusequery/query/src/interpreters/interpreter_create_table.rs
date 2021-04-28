// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;
use common_planners::CreateTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::{IInterpreter, InterpreterPtr};
use crate::sessions::FuseQueryContextRef;
use common_exception::ErrorCodes;

pub struct CreateTableInterpreter {
    ctx: FuseQueryContextRef,
    plan: CreateTablePlan
}

impl CreateTableInterpreter {
    pub fn try_create(ctx: FuseQueryContextRef, plan: CreateTablePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(CreateTableInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl IInterpreter for CreateTableInterpreter {
    fn name(&self) -> &str {
        "CreateTableInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let datasource = self.ctx.get_datasource();
        let database = datasource.get_database(self.plan.db.as_str())?;
        database.create_table(self.plan.clone()).await.map_err(ErrorCodes::from_anyhow)?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema.clone(),
            None,
            vec![],
        )))
    }
}
