// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_std::{prelude::*, sync::Arc};
use async_trait::async_trait;

use crate::error::Result;
use crate::processors::{EmptyTransform, FormatterSettings, IProcessor};
use crate::streams::DataBlockStream;

pub struct MergeTransform {
    list: Vec<Arc<dyn IProcessor>>,
}

impl MergeTransform {
    pub fn create() -> Self {
        MergeTransform { list: vec![] }
    }
}

#[async_trait]
impl IProcessor for MergeTransform {
    fn name(&self) -> &'static str {
        "MergeTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) {
        self.list.push(input);
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
            "{} Merge ({} × {}) to ({} × {})",
            setting.prefix,
            setting.prev_name,
            setting.prev_ways,
            self.name(),
            setting.ways
        )
    }

    async fn execute(&self) -> Result<DataBlockStream> {
        let mut result = EmptyTransform::create().execute().await?;

        for proc in &self.list {
            let proc_clone = proc.clone();
            let next = async_std::task::spawn(async move { proc_clone.execute().await }).await?;
            result = Box::pin(result.merge(next));
        }
        Ok(result)
    }
}
