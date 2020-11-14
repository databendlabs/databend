// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_std::{prelude::*, sync::Arc};
use async_trait::async_trait;

use crate::datastreams::DataBlockStream;
use crate::error::Result;
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

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) {
        self.list.push(input);
    }

    async fn execute(&self) -> Result<DataBlockStream> {
        let mut result = EmptyProcessor::create().execute().await?;

        for proc in &self.list {
            let proc_clone = proc.clone();
            let next = async_std::task::spawn(async move { proc_clone.execute().await }).await?;
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
