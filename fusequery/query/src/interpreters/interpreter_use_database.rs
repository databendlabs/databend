// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::bail;
use anyhow::Result;
use common_datavalues::DataSchema;
use common_planners::UseDatabasePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::IInterpreter;
use crate::sessions::FuseQueryContextRef;

pub struct UseDatabaseInterpreter {
    ctx: FuseQueryContextRef,
    plan: UseDatabasePlan,
}

impl UseDatabaseInterpreter {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        plan: UseDatabasePlan,
    ) -> Result<Arc<dyn IInterpreter>> {
        Ok(Arc::new(UseDatabaseInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl IInterpreter for UseDatabaseInterpreter {
    fn name(&self) -> &str {
        "UseDatabaseInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let db = self.plan.db.clone();
        if self.ctx.get_datasource().get_databases()?.contains(&db) {
            self.ctx.set_default_db(db)?;
        } else {
            bail!("Unknown database: {}", db)
        }
        let schema = Arc::new(DataSchema::empty());
        Ok(Box::pin(DataBlockStream::create(schema, None, vec![])))
    }
}
