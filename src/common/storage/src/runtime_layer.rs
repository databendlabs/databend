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

use std::io::Result;
use std::sync::Arc;

use async_trait::async_trait;
use common_base::base::tokio::runtime::Handle;
use common_base::base::AsyncThreadTracker;
use common_base::base::ThreadTracker;
use opendal::ops::OpCreate;
use opendal::ops::OpDelete;
use opendal::ops::OpList;
use opendal::ops::OpRead;
use opendal::ops::OpStat;
use opendal::ops::OpWrite;
use opendal::Accessor;
use opendal::BytesReader;
use opendal::Layer;
use opendal::ObjectMetadata;
use opendal::ObjectReader;
use opendal::ObjectStreamer;

/// # TODO
///
/// DalRuntime is used to make sure all IO task are running in the same runtime.
/// So that we will not bothered by `dispatch dropped` panic.
///
/// However, the new processor framework will make sure that all async task running
/// in the same, global, separate, IO only async runtime, so we can remove `RuntimeLayer`
/// after new processor framework finished.
#[derive(Clone, Debug)]
pub struct RuntimeLayer {
    runtime: Handle,
}

impl RuntimeLayer {
    pub fn new(runtime: Handle) -> Self {
        RuntimeLayer { runtime }
    }
}

impl Layer for RuntimeLayer {
    fn layer(&self, inner: Arc<dyn Accessor>) -> Arc<dyn Accessor> {
        Arc::new(RuntimeAccessor {
            inner,
            runtime: self.runtime.clone(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct RuntimeAccessor {
    inner: Arc<dyn Accessor>,
    runtime: Handle,
}

#[async_trait]
impl Accessor for RuntimeAccessor {
    fn inner(&self) -> Option<Arc<dyn Accessor>> {
        Some(self.inner.clone())
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<()> {
        let op = self.inner.clone();
        let path = path.to_string();
        let future = async move { op.create(&path, args).await };
        let future = AsyncThreadTracker::create(ThreadTracker::fork(), future);
        self.runtime.spawn(future).await.expect("join must success")
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<ObjectReader> {
        let op = self.inner.clone();
        let path = path.to_string();
        let future = async move { op.read(&path, args).await };
        let future = AsyncThreadTracker::create(ThreadTracker::fork(), future);
        self.runtime.spawn(future).await.expect("join must success")
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<u64> {
        let op = self.inner.clone();
        let path = path.to_string();
        let future = async move { op.write(&path, args, r).await };
        let future = AsyncThreadTracker::create(ThreadTracker::fork(), future);
        self.runtime.spawn(future).await.expect("join must success")
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<ObjectMetadata> {
        let op = self.inner.clone();
        let path = path.to_string();
        let future = async move { op.stat(&path, args).await };
        let future = AsyncThreadTracker::create(ThreadTracker::fork(), future);
        self.runtime.spawn(future).await.expect("join must success")
    }

    async fn delete(&self, path: &str, args: OpDelete) -> Result<()> {
        let op = self.inner.clone();
        let path = path.to_string();
        let future = async move { op.delete(&path, args).await };
        let future = AsyncThreadTracker::create(ThreadTracker::fork(), future);
        self.runtime.spawn(future).await.expect("join must success")
    }

    async fn list(&self, path: &str, args: OpList) -> Result<ObjectStreamer> {
        let op = self.inner.clone();
        let path = path.to_string();
        let future = async move { op.list(&path, args).await };
        let future = AsyncThreadTracker::create(ThreadTracker::fork(), future);
        self.runtime.spawn(future).await.expect("join must success")
    }
}
