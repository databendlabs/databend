// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use async_trait::async_trait;

use crate::datastreams::SendableDataBlockStream;
use crate::error::FuseQueryResult;
use crate::processors::{EmptyProcessor, FormatterSettings, IProcessor};

pub struct ThroughProcessor {
    input: Arc<dyn IProcessor>,
}

impl ThroughProcessor {
    pub fn create() -> Self {
        ThroughProcessor {
            input: Arc::new(EmptyProcessor::create()),
        }
    }
}

#[async_trait]
impl IProcessor for ThroughProcessor {
    fn name(&self) -> &str {
        "ThroughProcessor"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        self.input = input;
        Ok(())
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        Ok(self.input.execute().await?)
    }

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
            "{} Expand ({} × {}) to ({} × {})",
            setting.prefix,
            setting.prev_name,
            setting.prev_ways,
            self.name(),
            setting.ways
        )
    }
}
