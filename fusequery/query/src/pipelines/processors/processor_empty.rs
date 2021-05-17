// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_datavalues::DataSchema;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::pipelines::processors::IProcessor;

pub struct EmptyProcessor {}

impl EmptyProcessor {
    pub fn create() -> Self {
        EmptyProcessor {}
    }
}

#[async_trait::async_trait]
impl IProcessor for EmptyProcessor {
    fn name(&self) -> &str {
        "EmptyProcessor"
    }

    fn connect_to(&mut self, _: Arc<dyn IProcessor>) -> Result<()> {
        Result::Err(ErrorCodes::IllegalTransformConnectionState(
            "Cannot call EmptyProcessor connect_to"
        ))
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        vec![Arc::new(EmptyProcessor::create())]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        Ok(Box::pin(DataBlockStream::create(
            Arc::new(DataSchema::empty()),
            None,
            vec![]
        )))
    }
}
