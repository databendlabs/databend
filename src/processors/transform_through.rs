// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_std::sync::Arc;
use async_trait::async_trait;

use crate::error::Result;
use crate::processors::{EmptyTransform, FormatterSettings, IProcessor};
use crate::streams::DataBlockStream;

pub struct ThroughTransform {
    input: Arc<dyn IProcessor>,
}

impl ThroughTransform {
    pub fn create() -> Self {
        ThroughTransform {
            input: Arc::new(EmptyTransform::create()),
        }
    }
}

#[async_trait]
impl IProcessor for ThroughTransform {
    fn name(&self) -> &'static str {
        "ThroughTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) {
        self.input = input;
    }

    fn format(
        &self,
        f: &mut std::fmt::Formatter,
        setting: &mut FormatterSettings,
    ) -> std::fmt::Result {
        let indent = setting.indent;
        let prefix = setting.indent_char;

        if indent > 0 {
            writeln!(f)?;
            for _ in 0..indent {
                write!(f, "{}", prefix)?;
            }
        }

        write!(
            f,
            "{} Expand ({} × {}) to ({} × {})",
            setting.prefix,
            setting.prev_name,
            setting.prev_ways,
            self.name(),
            setting.ways
        )
    }

    async fn execute(&self) -> Result<DataBlockStream> {
        Ok(self.input.execute().await?)
    }
}
