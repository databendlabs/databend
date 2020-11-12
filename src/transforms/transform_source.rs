// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_std::sync::Arc;
use async_trait::async_trait;

use crate::datablocks::DataBlock;
use crate::error::Result;
use crate::processors::{FormatterSettings, IProcessor};
use crate::streams::{ChunkStream, DataBlockStream};

pub struct SourceTransform {
    data: Vec<DataBlock>,
}

impl SourceTransform {
    pub fn create(data: Vec<DataBlock>) -> Self {
        SourceTransform { data }
    }
}

#[async_trait]
impl IProcessor for SourceTransform {
    fn name(&self) -> &'static str {
        "SourceTransform"
    }

    fn connect_to(&mut self, _: Arc<dyn IProcessor>) {
        unimplemented!()
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
        write!(f, "{} {} × {}", setting.prefix, self.name(), setting.ways)
    }

    async fn execute(&self) -> Result<DataBlockStream> {
        Ok(Box::pin(ChunkStream::create(self.data.clone())))
    }
}
