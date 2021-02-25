// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::StreamExt;
use tokio::sync::mpsc;

use crate::datablocks::DataBlock;
use crate::datastreams::{ChannelStream, SendableDataBlockStream};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::processors::{FormatterSettings, IProcessor};

pub struct MergeProcessor {
    inputs: Vec<Arc<dyn IProcessor>>,
}

impl MergeProcessor {
    pub fn create() -> Self {
        MergeProcessor { inputs: vec![] }
    }
}

#[async_trait]
impl IProcessor for MergeProcessor {
    fn name(&self) -> &str {
        "MergeProcessor"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        self.inputs.push(input);
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        self.inputs.clone()
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        let partitions = self.inputs.len();
        match partitions {
            0 => Err(FuseQueryError::Internal(
                "Merge processor cannot be zero".to_string(),
            )),
            1 => self.inputs[0].execute().await,
            _ => {
                let (sender, receiver) = mpsc::channel::<FuseQueryResult<DataBlock>>(partitions);
                for i in 0..partitions {
                    let input = self.inputs[i].clone();
                    let sender = sender.clone();
                    tokio::spawn(async move {
                        let mut stream = match input.execute().await {
                            Err(e) => {
                                sender.send(Err(e)).await.ok();
                                return;
                            }
                            Ok(stream) => stream,
                        };

                        while let Some(item) = stream.next().await {
                            sender.send(item).await.ok();
                        }
                    });
                }
                Ok(Box::pin(ChannelStream { input: receiver }))
            }
        }
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
