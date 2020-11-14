// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_std::sync::Arc;
use async_trait::async_trait;

use crate::datastreams::DataBlockStream;
use crate::error::Result;

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
    fn name(&self) -> &'static str;

    /// Connect to the input processor, add an edge on the DAG.
    fn connect_to(&mut self, input: Arc<dyn IProcessor>);

    /// Execute the processor.
    async fn execute(&self) -> Result<DataBlockStream>;

    /// Format the processor.
    fn format(
        &self,
        f: &mut std::fmt::Formatter,
        setting: &mut FormatterSettings,
    ) -> std::fmt::Result {
        if setting.indent > 0 {
            writeln!(f)?;
            for _ in 0..setting.indent {
                write!(f, "{}", setting.indent_char)?;
            }
        }
        write!(
            f,
            "{} {} × {} {}",
            setting.prefix,
            self.name(),
            setting.ways,
            if setting.ways == 1 {
                "processor"
            } else {
                "processors"
            },
        )
    }
}
