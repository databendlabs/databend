// Copyright 2022 Datafuse Labs.
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

use async_trait::async_trait;
use opendal::error::Result as DalResult;
use opendal::ops::OpDelete;
use opendal::ops::OpRead;
use opendal::ops::OpStat;
use opendal::ops::OpWrite;
use opendal::readers::CallbackReader;
use opendal::Accessor;
use opendal::BoxedAsyncReader;
use opendal::Layer;
use opendal::Metadata;

use crate::metrics::DalMetrics;

#[derive(Clone, Default)]
pub struct DalContext {
    inner: Option<Arc<dyn Accessor>>,
    metrics: Arc<DalMetrics>,
}

impl DalContext {
    pub fn new(inner: Arc<dyn Accessor>) -> Self {
        DalContext {
            inner: Some(inner),
            metrics: Arc::new(Default::default()),
        }
    }

    pub fn get_metrics(&self) -> Arc<DalMetrics> {
        self.metrics.clone()
    }
}

impl Layer for DalContext {
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        Arc::new(DalContext {
            inner: Some(inner),
            metrics: self.metrics.clone(),
        })
    }
}

#[async_trait]
impl Accessor for DalContext {
    async fn read(&self, args: &OpRead) -> DalResult<BoxedAsyncReader> {
        let metric = self.metrics.clone();
        self.inner.as_ref().unwrap().read(args).await.map(|reader| {
            let r = CallbackReader::new(reader, move |n| {
                metric.inc_read_bytes(n);
            });

            Box::new(r) as BoxedAsyncReader
        })
    }

    async fn write(&self, r: BoxedAsyncReader, args: &OpWrite) -> DalResult<usize> {
        self.inner.as_ref().unwrap().write(r, args).await.map(|n| {
            self.metrics.inc_write_bytes(n);
            n
        })
    }

    async fn stat(&self, args: &OpStat) -> DalResult<Metadata> {
        self.inner.as_ref().unwrap().stat(args).await
    }

    async fn delete(&self, args: &OpDelete) -> DalResult<()> {
        self.inner.as_ref().unwrap().delete(args).await
    }
}
