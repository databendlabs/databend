// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_runtime::tokio::sync::mpsc;
use common_streams::SendableDataBlockStream;
use log::error;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::pipelines::processors::Processor;
use crate::sessions::FuseQueryContextRef;

pub struct CreateSetProcessor {
    ctx: FuseQueryContextRef,
    piplines: Vec<Pipeline>,
    names: Vec<String>,
    //inputs: Vec<Arc<dyn Processor>>,
}

impl CreateSetProcessor {
    pub fn create(ctx: FuseQueryContextRef, subqueries: &Vec<PlanNode>, names: Vec<String>) -> Self {
        CreateSetsProcessor {
            ctx,
            subqueries: subqueries,
            names: names,
        }
    }
}

#[async_trait::async_trait]
impl Processor for CreateSetProcessor {
    fn name(&self) -> &str {
        "CreateSetProcessor"
    }

    fn connect_to(&mut self, _: Arc<dyn Processor>) -> Result<()> {
        Result::Err(ErrorCode::IllegalTransformConnectionState(
            "Cannot call EmptyProcessor connect_to",
        ))
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        vec![Arc::new(EmptyProcessor::create())]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let size = pipelines.len();
        for i in (0..size) {
            pipeline = pipelines[i];
            name = names[i];
            let stream = pipeline.await?;
            let result = stream.try_collect::<Vec<_>>().await?;
            let b = if result.len() > 0 { true } else { false };
            context.subquery_res_map.insert(name, b);
        }
        Ok(Box::pin(DataBlockStream::create(
            Arc::new(DataSchema::empty()),
            None,
            vec![],
        )))
    }
}
