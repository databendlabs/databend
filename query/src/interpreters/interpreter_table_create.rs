// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;
use common_planners::CreateTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::catalog::Catalog;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::DatafuseQueryContextRef;

pub struct CreateTableInterpreter {
    ctx: DatafuseQueryContextRef,
    plan: CreateTablePlan,
}

impl CreateTableInterpreter {
    pub fn try_create(
        ctx: DatafuseQueryContextRef,
        plan: CreateTablePlan,
    ) -> Result<InterpreterPtr> {
        Ok(Arc::new(CreateTableInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateTableInterpreter {
    fn name(&self) -> &str {
        "CreateTableInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let datasource = self.ctx.get_datasource();
        let database = datasource.get_database(self.plan.db.as_str())?;
        database.create_table(self.plan.clone()).await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema.clone(),
            None,
            vec![],
        )))
    }
}
