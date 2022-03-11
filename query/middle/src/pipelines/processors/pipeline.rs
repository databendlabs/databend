// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use super::MixedProcessor;
use crate::pipelines::processors::MergeProcessor;
use crate::pipelines::processors::Pipe;
use crate::pipelines::processors::Processor;
use crate::sessions::QueryContext;

pub struct Pipeline {
    ctx: Arc<QueryContext>,
    pipes: Vec<Pipe>,
}

impl Pipeline {
    pub fn create(ctx: Arc<QueryContext>) -> Self {
        Pipeline { ctx, pipes: vec![] }
    }

    /// Reset the pipeline.
    pub fn reset(&mut self) {
        self.pipes.clear();
    }

    /// The number of pipes.
    pub fn nums(&self) -> usize {
        self.pipes.last().map_or(0, |v| v.nums())
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

    pub fn add_source(&mut self, source: Arc<dyn Processor>) -> Result<()> {
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
        f: impl Fn() -> Result<Box<dyn Processor>>,
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

    /// Mixed M processors into N processes.
    ///
    /// processor1 --          processor1
    ///               \      /
    /// processor2      -->
    ///               /      \
    /// processor3 --          processor2
    ///
    pub fn mixed_processor(&mut self, n: usize) -> Result<()> {
        if n == 1 {
            return self.merge_processor();
        }
        let last_pipe = self.last_pipe()?;

        // do nothing when m == n
        if last_pipe.nums() == n {
            return Ok(());
        }

        let mut processor = MixedProcessor::create(self.ctx.clone(), n);
        for x in last_pipe.processors() {
            processor.connect_to(x)?;
        }

        let mut new_pipe = Pipe::create();
        for _i in 0..n - 1 {
            let processor = processor.share()?;
            new_pipe.add(Arc::from(processor));
        }
        new_pipe.add(Arc::from(processor));
        self.pipes.push(new_pipe);

        Ok(())
    }

    #[tracing::instrument(level = "debug", name="pipeline_execute", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    pub async fn execute(&mut self) -> Result<SendableDataBlockStream> {
        if self.last_pipe()?.nums() > 1 {
            self.merge_processor()?;
        }
        self.last_pipe()?.first().execute().await
    }
}
