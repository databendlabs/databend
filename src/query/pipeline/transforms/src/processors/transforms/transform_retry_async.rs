// Copyright 2021 Datafuse Labs
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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use log::info;

use super::AsyncTransform;

pub trait AsyncRetry: AsyncTransform {
    fn retry_on(&self, err: &databend_common_exception::ErrorCode) -> bool;
    fn retry_strategy(&self) -> RetryStrategy;
}

#[derive(Clone)]
pub struct RetryStrategy {
    pub retry_times: usize,
    pub retry_sleep_duration: Option<tokio::time::Duration>,
}

pub struct AsyncRetryWrapper<T: AsyncRetry + 'static> {
    retries: usize,
    t: T,
}

impl<T: AsyncRetry + 'static> AsyncRetryWrapper<T> {
    pub fn create(inner: T) -> Self {
        Self {
            t: inner,
            retries: 0,
        }
    }
}

#[async_trait::async_trait]
impl<T: AsyncRetry + 'static> AsyncTransform for AsyncRetryWrapper<T> {
    const NAME: &'static str = T::NAME;

    async fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let strategy = self.t.retry_strategy();
        while self.retries < strategy.retry_times {
            match self.t.transform(data.clone()).await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    // Add log to know which error is retrying
                    info!(
                        "Retry {} times for transform {} error: {:?}",
                        self.retries,
                        Self::NAME,
                        e
                    );
                    if !self.t.retry_on(&e) {
                        return Err(e);
                    }
                    if let Some(duration) = strategy.retry_sleep_duration {
                        tokio::time::sleep(duration).await;
                    }
                    self.retries += 1;
                }
            }
        }
        self.t.transform(data.clone()).await
    }

    fn name(&self) -> String {
        Self::NAME.to_string()
    }

    async fn on_start(&mut self) -> Result<()> {
        self.t.on_start().await
    }

    async fn on_finish(&mut self) -> Result<()> {
        self.t.on_finish().await
    }
}
