// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use common_planners::CreatePlan;
use common_streams::{DataBlockStream, SendableDataBlockStream};

use crate::datasources::TableFactory;
use crate::interpreters::IInterpreter;
use crate::sessions::FuseQueryContextRef;

pub struct CreateInterpreter {
    ctx: FuseQueryContextRef,
    plan: CreatePlan,
}

impl CreateInterpreter {
    pub fn try_create(ctx: FuseQueryContextRef, plan: CreatePlan) -> Result<Arc<dyn IInterpreter>> {
        Ok(Arc::new(CreateInterpreter { ctx, plan }))
    }
}

#[async_trait]
impl IInterpreter for CreateInterpreter {
    fn name(&self) -> &str {
        "CreateInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let engine = self.plan.engine.to_string();

        let datasource = self.ctx.get_datasource();
        let table = TableFactory::create_table(
            &engine,
            self.ctx.clone(),
            self.plan.db.clone(),
            self.plan.table.clone(),
            self.plan.schema.clone(),
            self.plan.options.clone(),
        )?;

        datasource.write().add_table(&self.plan.db, table.into())?;
        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema.clone(),
            None,
            vec![],
        )))
    }
}
