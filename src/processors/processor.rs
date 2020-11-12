// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_std::sync::Arc;
use async_trait::async_trait;

use crate::error::Result;
use crate::streams::DataBlockStream;

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
    fn name(&self) -> &'static str;
    fn connect_to(&mut self, input: Arc<dyn IProcessor>);
    fn format(
        &self,
        f: &mut std::fmt::Formatter,
        setting: &mut FormatterSettings,
    ) -> std::fmt::Result;
    async fn execute(&self) -> Result<DataBlockStream>;
}
