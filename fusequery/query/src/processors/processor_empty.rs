// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;

use crate::common_datavalues::DataSchema;
use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::processors::IProcessor;

pub struct EmptyProcessor {}

impl EmptyProcessor {
    pub fn create() -> Self {
        EmptyProcessor {}
    }
}

#[async_trait]
impl IProcessor for EmptyProcessor {
    fn name(&self) -> &str {
        "EmptyProcessor"
    }

    fn connect_to(&mut self, _: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        Err(FuseQueryError::build_internal_error(
            "Cannot call EmptyProcessor connect_to".to_owned(),
        ))
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        vec![Arc::new(EmptyProcessor::create())]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        Ok(Box::pin(DataBlockStream::create(
            Arc::new(DataSchema::empty()),
            None,
            vec![],
        )))
    }
}
