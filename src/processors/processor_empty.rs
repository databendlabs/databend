// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use async_trait::async_trait;

use crate::datastreams::SendableDataBlockStream;
use crate::error::FuseQueryResult;
use crate::processors::{FormatterSettings, IProcessor};

pub struct EmptyProcessor {}

impl EmptyProcessor {
    pub fn create() -> Self {
        EmptyProcessor {}
    }
}

#[async_trait]
impl IProcessor for EmptyProcessor {
    fn name(&self) -> String {
        "EmptyProcessor".to_owned()
    }

    fn connect_to(&mut self, _: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        unimplemented!()
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        unimplemented!()
    }

    fn format(
        &self,
        f: &mut std::fmt::Formatter,
        _setting: &mut FormatterSettings,
    ) -> std::fmt::Result {
        write!(f, "")
    }
}
