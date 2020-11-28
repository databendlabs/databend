// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;
use std::sync::Arc;
use tokio::stream::StreamExt;

use crate::datastreams::SendableDataBlockStream;
use crate::datavalues::DataSchemaRef;
use crate::error::FuseQueryResult;
use crate::processors::{EmptyProcessor, FormatterSettings, IProcessor};

pub struct MergeProcessor {
    list: Vec<Arc<dyn IProcessor>>,
}

impl MergeProcessor {
    pub fn create() -> Self {
        MergeProcessor { list: vec![] }
    }
}

#[async_trait]
impl IProcessor for MergeProcessor {
    fn name(&self) -> &'static str {
        "MergeProcessor"
    }

    fn schema(&self) -> FuseQueryResult<DataSchemaRef> {
        self.list[0].schema()
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        self.list.push(input);
        Ok(())
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        let mut result = EmptyProcessor::create().execute().await?;

        for xproc in &self.list {
            let proc = xproc.clone();
            let next = proc.execute().await?;
            result = Box::pin(result.merge(next));
        }
        Ok(result)
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
            "{} Merge ({} × {} {}) to ({} × {})",
            setting.prefix,
            setting.prev_name,
            setting.prev_ways,
            if setting.prev_ways == 1 {
                "processor"
            } else {
                "processors"
            },
            self.name(),
            setting.ways
        )
    }
}
