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

use common_base::Progress;
use common_base::ProgressCallback;
use common_base::ProgressValues;
use common_exception::Result;
use common_planners::PlanNode;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::InterpreterQueryLog;
use crate::sessions::QueryContext;

pub struct InterceptorInterpreter {
    inner: InterpreterPtr,
    query_log: InterpreterQueryLog,
    result_metric: Arc<Progress>,
}

impl InterceptorInterpreter {
    pub fn create(ctx: Arc<QueryContext>, inner: InterpreterPtr, plan: PlanNode) -> Self {
        InterceptorInterpreter {
            inner,
            query_log: InterpreterQueryLog::create(ctx, plan),
            result_metric: Arc::new(Progress::create()),
        }
    }

    /// Get the last sink stream progress values.
    fn result_metric_callback(&self) -> Result<ProgressCallback> {
        let current = self.result_metric.clone();
        Ok(Box::new(move |value: &ProgressValues| {
            current.incr(value);
        }))
    }
}

#[async_trait::async_trait]
impl Interpreter for InterceptorInterpreter {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn execute(
        &self,
        input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let result_stream = self.inner.execute(input_stream).await?;
        let metric_stream =
            ProgressStream::try_create(result_stream, self.result_metric_callback()?)?;
        Ok(Box::pin(metric_stream))
    }

    async fn start(&self) -> Result<()> {
        self.query_log.log_start().await
    }

    async fn finish(&self) -> Result<()> {
        let result_metrics = self.result_metric.get_values();
        self.query_log
            .log_finish(
                result_metrics.read_rows as u64,
                result_metrics.read_bytes as u64,
            )
            .await
    }
}
