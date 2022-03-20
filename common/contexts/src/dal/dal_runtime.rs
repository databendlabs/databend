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
use common_base::tokio::runtime::Handle;
use opendal::error::Result as DalResult;
use opendal::ops::OpDelete;
use opendal::ops::OpList;
use opendal::ops::OpRead;
use opendal::ops::OpStat;
use opendal::ops::OpWrite;
use opendal::Accessor;
use opendal::BoxedAsyncReader;
use opendal::BoxedObjectStream;
use opendal::Layer;
use opendal::Metadata;

/// # TODO
///
/// DalRuntime is used to make sure all IO task are running in the same runtime.
/// So that we will not bothered by `dispatch dropped` panic.
///
/// However, the new processor framework will make sure that all async task running
/// in the same, global, separate, IO only async runtime, so we can remove `DalRuntime`
/// after new processor framework finished.
#[derive(Clone, Debug)]
pub struct DalRuntime {
    inner: Option<Arc<dyn Accessor>>,
    runtime: Handle,
}

impl DalRuntime {
    pub fn new(runtime: Handle) -> Self {
        DalRuntime {
            inner: None,
            runtime,
        }
    }
}

impl Layer for DalRuntime {
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        Arc::new(DalRuntime {
            inner: Some(inner),
            runtime: self.runtime.clone(),
        })
    }
}

#[async_trait]
impl Accessor for DalRuntime {
    async fn read(&self, args: &OpRead) -> DalResult<BoxedAsyncReader> {
        let op = self.inner.as_ref().unwrap().clone();
        let args = args.clone();
        self.runtime
            .spawn(async move { op.read(&args).await })
            .await
            .expect("join must success")
    }

    async fn write(&self, r: BoxedAsyncReader, args: &OpWrite) -> DalResult<usize> {
        let op = self.inner.as_ref().unwrap().clone();
        let args = args.clone();
        self.runtime
            .spawn(async move { op.write(r, &args).await })
            .await
            .expect("join must success")
    }

    async fn stat(&self, args: &OpStat) -> DalResult<Metadata> {
        let op = self.inner.as_ref().unwrap().clone();
        let args = args.clone();
        self.runtime
            .spawn(async move { op.stat(&args).await })
            .await
            .expect("join must success")
    }

    async fn delete(&self, args: &OpDelete) -> DalResult<()> {
        let op = self.inner.as_ref().unwrap().clone();
        let args = args.clone();
        self.runtime
            .spawn(async move { op.delete(&args).await })
            .await
            .expect("join must success")
    }

    async fn list(&self, args: &OpList) -> DalResult<BoxedObjectStream> {
        let op = self.inner.as_ref().unwrap().clone();
        let args = args.clone();
        self.runtime
            .spawn(async move { op.list(&args).await })
            .await
            .expect("join must success")
    }
}
