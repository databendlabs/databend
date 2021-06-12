// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::SendableDataBlockStream;

use crate::pipelines::processors::IProcessor;
use crate::pipelines::processors::MergeProcessor;
use crate::pipelines::processors::Pipe;
use crate::sessions::FuseQueryContextRef;

pub struct Pipeline {
    ctx: FuseQueryContextRef,
    pipes: Vec<Pipe>,
}

impl Pipeline {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        Pipeline { ctx, pipes: vec![] }
    }

    /// Reset the pipeline.
    pub fn reset(&mut self) {
        self.pipes.clear();
    }

    /// The number of pipes.
    pub fn nums(&self) -> usize {
        match self.pipes.last() {
            None => 0,
            Some(v) => v.nums(),
        }
    }

    pub fn pipes(&self) -> Vec<Pipe> {
        self.pipes.clone()
    }

    pub fn pipe_by_index(&self, index: usize) -> Pipe {
        self.pipes[index].clone()
    }

    /// Last pipe of the pipeline.
    pub fn last_pipe(&self) -> Result<&Pipe> {
        self.pipes
            .last()
            .ok_or_else(|| ErrorCode::IllegalPipelineState("Pipeline last pipe can not be none"))
    }

    pub fn add_source(&mut self, source: Arc<dyn IProcessor>) -> Result<()> {
        if self.pipes.first().is_none() {
            let mut first = Pipe::create();
            first.add(source);
            self.pipes.push(first);
        } else {
            self.pipes[0].add(source);
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
        f: impl Fn() -> Result<Box<dyn IProcessor>>,
    ) -> Result<()> {
        let last_pipe = self.last_pipe()?;
        let mut new_pipe = Pipe::create();
        for x in last_pipe.processors() {
            let mut p = f()?;
            p.connect_to(x.clone())?;
            new_pipe.add(Arc::from(p));
        }
        self.pipes.push(new_pipe);
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
    pub fn merge_processor(&mut self) -> Result<()> {
        let last_pipe = self.last_pipe()?;
        if last_pipe.nums() > 1 {
            let mut merge = MergeProcessor::create(self.ctx.clone());
            for x in last_pipe.processors() {
                merge.connect_to(x.clone())?;
            }
            let mut new_pipe = Pipe::create();
            new_pipe.add(Arc::from(merge));
            self.pipes.push(new_pipe);
        }
        Ok(())
    }

    pub async fn execute(&mut self) -> Result<SendableDataBlockStream> {
        if self.last_pipe()?.nums() > 1 {
            self.merge_processor()?;
        }
        self.last_pipe()?.first().execute().await
    }
}
