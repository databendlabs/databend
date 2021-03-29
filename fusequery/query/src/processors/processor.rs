// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::datastreams::SendableDataBlockStream;

/// Formatter settings for PlanStep debug.
pub struct FormatterSettings {
    pub ways: usize,
    pub indent: usize,
    pub indent_char: &'static str,
    pub prefix: &'static str,
    pub prev_ways: usize,
    pub prev_name: String,
}

#[async_trait]
pub trait IProcessor: Sync + Send {
    /// Processor name.
    fn name(&self) -> &str;

    /// Connect to the input processor, add an edge on the DAG.
    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> Result<()>;

    /// Inputs.
    fn inputs(&self) -> Vec<Arc<dyn IProcessor>>;

    /// Reference used for downcast.
    fn as_any(&self) -> &dyn Any;

    /// Execute the processor.
    async fn execute(&self) -> Result<SendableDataBlockStream>;
}
