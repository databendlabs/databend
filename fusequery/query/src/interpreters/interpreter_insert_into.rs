// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;
use common_planners::InsertIntoPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::catalog::Catalog;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::FuseQueryContextRef;

pub struct InsertIntoInterpreter {
    ctx: FuseQueryContextRef,
    plan: InsertIntoPlan,
}

impl InsertIntoInterpreter {
    pub fn try_create(ctx: FuseQueryContextRef, plan: InsertIntoPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(InsertIntoInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for InsertIntoInterpreter {
    fn name(&self) -> &str {
        "InsertIntoInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let datasource = self.ctx.get_datasource();
        let database = datasource.get_database(self.plan.db_name.as_str())?;
        let table = database.get_table(self.plan.tbl_name.as_str())?;
        table
            .datasource()
            .append_data(self.ctx.clone(), self.plan.clone())
            .await?;
        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
