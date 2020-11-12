// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_std::{stream::StreamExt, sync::Arc};
use async_trait::async_trait;

use crate::datablocks::DataBlock;
use crate::datatypes::{DataField, DataSchema, DataType};
use crate::error::Result;
use crate::functions::FunctionFactory;
use crate::processors::{EmptyTransform, FormatterSettings, IProcessor};
use crate::streams::{ChunkStream, DataBlockStream};

pub struct CountTransform {
    input: Arc<dyn IProcessor>,
}

impl CountTransform {
    pub fn create() -> Self {
        CountTransform {
            input: Arc::new(EmptyTransform::create()),
        }
    }
}

#[async_trait]
impl IProcessor for CountTransform {
    fn name(&self) -> &'static str {
        "CountTransform"
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
        write!(f, "{} {} × {}", setting.prefix, self.name(), setting.ways)
    }

    async fn execute(&self) -> Result<DataBlockStream> {
        let mut func = FunctionFactory::get("COUNT", &[])?;

        let mut exec = self.input.execute().await?;
        while let Some(v) = exec.next().await {
            func.accumulate(&v?)?;
        }

        Ok(Box::pin(ChunkStream::create(vec![DataBlock::new(
            DataSchema::new(vec![DataField::new("count", DataType::UInt64, false)]),
            vec![func.aggregate()?],
        )])))
    }
}
