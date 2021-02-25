// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::sync::Arc;

use crate::datastreams::SendableDataBlockStream;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::processors::{IProcessor, MergeProcessor};

pub type Pipe = Vec<Arc<dyn IProcessor>>;

pub struct Pipeline {
    pub processors: Vec<Pipe>,
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
