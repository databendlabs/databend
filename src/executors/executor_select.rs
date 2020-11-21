// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;
use std::sync::Arc;

use crate::contexts::Context;
use crate::datastreams::SendableDataBlockStream;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::executors::IExecutor;
use crate::planners::SelectPlan;

pub struct SelectExecutor {}

impl SelectExecutor {
    pub fn try_create(
        _ctx: Arc<Context>,
        _plan: SelectPlan,
    ) -> FuseQueryResult<Arc<dyn IExecutor>> {
        Ok(Arc::new(SelectExecutor {}))
    }
}

#[async_trait]
impl IExecutor for SelectExecutor {
    fn name(&self) -> &'static str {
        "SelectExecutor"
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        Err(FuseQueryError::Unsupported(
            "Unsupported select executor".to_string(),
        ))
    }
}
