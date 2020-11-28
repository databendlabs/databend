// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;
use std::sync::Arc;

use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::datavalues::{DataSchema, DataSchemaRef};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::processors::{FormatterSettings, IProcessor};

pub struct EmptyProcessor {}

impl EmptyProcessor {
    pub fn create() -> Self {
        EmptyProcessor {}
    }
}

#[async_trait]
impl IProcessor for EmptyProcessor {
    fn name(&self) -> &'static str {
        "EmptyProcessor"
    }

    fn schema(&self) -> FuseQueryResult<DataSchemaRef> {
        Err(FuseQueryError::Internal(
            "Cannot get EmptyProcessor schema".to_string(),
        ))
    }

    fn connect_to(&mut self, _: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        Err(FuseQueryError::Internal(
            "Cannot call EmptyProcessor connect_to".to_string(),
        ))
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        Ok(Box::pin(DataBlockStream::create(
            Arc::new(DataSchema::empty()),
            None,
            vec![],
        )))
    }

    fn format(
        &self,
        f: &mut std::fmt::Formatter,
        _setting: &mut FormatterSettings,
    ) -> std::fmt::Result {
        write!(f, "")
    }
}
