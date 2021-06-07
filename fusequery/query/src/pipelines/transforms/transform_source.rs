// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_exception::ErrorCodes;
use common_exception::Result;
use common_streams::SendableDataBlockStream;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;
use crate::sessions::FuseQueryContextRef;

pub struct SourceTransform {
    ctx: FuseQueryContextRef,
    db: String,
    table: String,
    remote: bool,
}

impl SourceTransform {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        db: &str,
        table: &str,
        remote: bool,
    ) -> Result<Self> {
        Ok(SourceTransform {
            ctx,
            db: db.to_string(),
            table: table.to_string(),
            remote,
        })
    }
}

#[async_trait::async_trait]
impl IProcessor for SourceTransform {
    fn name(&self) -> &str {
        "SourceTransform"
    }

    fn connect_to(&mut self, _: Arc<dyn IProcessor>) -> Result<()> {
        Result::Err(ErrorCodes::LogicalError(
            "Cannot call SourceTransform connect_to",
        ))
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        vec![Arc::new(EmptyProcessor::create())]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let table = if self.remote {
            self.ctx
                .get_remote_table(self.db.as_str(), self.table.as_str())
                .await?
        } else {
            self.ctx.get_table(self.db.as_str(), self.table.as_str())?
        };

        table.read(self.ctx.clone()).await
    }
}
