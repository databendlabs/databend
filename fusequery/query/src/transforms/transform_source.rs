// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;

use crate::datastreams::SendableDataBlockStream;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::processors::{EmptyProcessor, IProcessor};
use crate::sessions::FuseQueryContextRef;

pub struct SourceTransform {
    ctx: FuseQueryContextRef,
    db: String,
    table: String,
}

impl SourceTransform {
    pub fn try_create(ctx: FuseQueryContextRef, db: &str, table: &str) -> FuseQueryResult<Self> {
        Ok(SourceTransform {
            ctx,
            db: db.to_string(),
            table: table.to_string(),
        })
    }
}

#[async_trait]
impl IProcessor for SourceTransform {
    fn name(&self) -> &str {
        "SourceTransform"
    }

    fn connect_to(&mut self, _: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        Err(FuseQueryError::build_internal_error(
            "Cannot call SourceTransform connect_to".to_string(),
        ))
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        vec![Arc::new(EmptyProcessor::create())]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        let table = self.ctx.get_table(self.db.as_str(), self.table.as_str())?;
        table.read(self.ctx.clone()).await
    }
}
