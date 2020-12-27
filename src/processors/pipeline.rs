// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use num::range;

use crate::datastreams::SendableDataBlockStream;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::processors::{FormatterSettings, IProcessor, MergeProcessor};

pub type Pipe = Vec<Arc<dyn IProcessor>>;

pub struct Pipeline {
    processors: Vec<Pipe>,
}

impl Pipeline {
    pub fn create() -> Self {
        Pipeline { processors: vec![] }
    }

    pub fn create_from_pipeline(from: Pipeline) -> Self {
        let mut pipeline = Pipeline { processors: vec![] };

        for x in from.processors {
            pipeline.processors.push(x);
        }
        pipeline
    }

    pub fn pipe_num(&self) -> usize {
        match self.processors.last() {
            None => 0,
            Some(v) => v.len(),
        }
    }

    pub fn add_source(&mut self, source: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        if self.processors.first().is_none() {
            let mut first = vec![];
            first.push(source);
            self.processors.push(first);
        } else {
            self.processors[0].push(source);
        }
        Ok(())
    }

    /// Add a normal processor to the pipeline.
    ///
    /// processor1 --> processor1_1
    ///
    /// processor2 --> processor2_1
    ///
    /// processor3 --> processor3_1
    ///
    pub fn add_simple_transform(
        &mut self,
        f: impl Fn() -> FuseQueryResult<Box<dyn IProcessor>>,
    ) -> FuseQueryResult<()> {
        let last = self.processors.last().ok_or_else(|| {
            FuseQueryError::Internal("Can't add transform to an empty pipe list".to_string())
        })?;
        let mut items = Vec::with_capacity(last.len());
        for x in last {
            let mut p = f()?;
            p.connect_to(x.clone())?;
            items.push(Arc::from(p));
        }
        self.processors.push(items);
        Ok(())
    }

    /// Merge many(or one)-ways processors into one-way.
    ///
    /// processor1 --
    ///               \
    /// processor2      --> processor
    ///               /
    /// processor3 --
    ///
    pub fn merge_processor(&mut self) -> FuseQueryResult<()> {
        let last = self.processors.last().ok_or_else(|| {
            FuseQueryError::Internal(
                "Can't merge processor when the last pipe is empty".to_string(),
            )
        })?;

        if last.len() > 1 {
            let mut p = MergeProcessor::create();
            for x in last {
                p.connect_to(x.clone())?;
            }
            self.processors.push(vec![Arc::from(p)]);
        }
        Ok(())
    }

    pub async fn execute(&mut self) -> FuseQueryResult<SendableDataBlockStream> {
        if self.processors.last().unwrap().len() > 1 {
            self.merge_processor()?;
        }
        self.processors.last().unwrap()[0].execute().await
    }
}

impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let setting = &mut FormatterSettings {
            ways: 0,
            indent: 0,
            indent_char: "  ",
            prefix: "└─",
            prev_ways: 0,
            prev_name: "".to_string(),
        };

        let pipes = self.processors.iter().as_slice();
        for i in range(0, pipes.len()).rev() {
            let cur = &pipes[i];
            if i > 0 {
                let next = &pipes[i - 1];
                setting.prev_ways = next.len();
                setting.prev_name = next[0].name().to_string();
            }
            setting.ways = cur.len();
            setting.indent += 1;
            cur.first().unwrap().format(f, setting)?;
        }
        write!(f, "")
    }
}
