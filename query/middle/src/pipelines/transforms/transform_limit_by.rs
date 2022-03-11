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

use std::any::Any;
use std::sync::Arc;

use common_exception::Result;
use common_planners::Expression;
use common_streams::LimitByStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;

pub struct LimitByTransform {
    input: Arc<dyn Processor>,
    limit_by_exprs: Vec<Expression>,
    limit: usize,
}

impl LimitByTransform {
    pub fn create(limit: usize, limit_by_exprs: Vec<Expression>) -> Self {
        Self {
            input: Arc::new(EmptyProcessor::create()),
            limit,
            limit_by_exprs,
        }
    }
}

#[async_trait::async_trait]
impl Processor for LimitByTransform {
    fn name(&self) -> &str {
        "LimitByTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn Processor>) -> Result<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    #[tracing::instrument(level = "debug", name="limit_by_execute" skip(self))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        tracing::debug!("execute...");

        Ok(Box::pin(LimitByStream::try_create(
            self.input.execute().await?,
            self.limit,
            self.limit_by_exprs
                .iter()
                .map(|col| col.column_name())
                .collect(),
        )?))
    }
}
