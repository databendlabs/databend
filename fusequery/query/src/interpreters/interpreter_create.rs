// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use common_planners::CreatePlan;
use common_streams::{DataBlockStream, SendableDataBlockStream};

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
        let datasource = self.ctx.get_datasource();
        let database = datasource.read().get_database(self.plan.db.as_str())?;
        database.create_table(self.plan.clone())?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema.clone(),
            None,
            vec![],
        )))
    }
}
