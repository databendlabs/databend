// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use async_trait::async_trait;
use std::sync::Arc;

use crate::datastreams::SendableDataBlockStream;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::processors::IProcessor;
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
        Err(FuseQueryError::Internal(
            "Cannot call SourceTransform connect_to".to_string(),
        ))
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        unimplemented!()
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        let table = self.ctx.get_table(self.db.as_str(), self.table.as_str())?;
        table.read(self.ctx.clone()).await
    }
}
