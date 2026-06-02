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

use log::warn;
use opendal::Error;
use opendal::ErrorKind;
use opendal::Result;
use opendal::raw::Access;
use opendal::raw::Layer;
use opendal::raw::LayeredAccess;
use opendal::raw::OpDelete;
use opendal::raw::OpList;
use opendal::raw::OpRead;
use opendal::raw::OpWrite;
use opendal::raw::RpDelete;
use opendal::raw::RpList;
use opendal::raw::RpRead;
use opendal::raw::RpWrite;
use opendal::raw::oio;

#[derive(Debug, Clone)]
pub struct DummyDeleteLayer;

impl<A: Access> Layer<A> for DummyDeleteLayer {
    type LayeredAccess = DummyDeleteAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        DummyDeleteAccessor { inner }
    }
}

#[derive(Debug, Clone)]
pub struct DummyDeleteAccessor<A: Access> {
    inner: A,
}

impl<A: Access> LayeredAccess for DummyDeleteAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type Writer = A::Writer;
    type Lister = A::Lister;
    type Deleter = DummyDeleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner.read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        self.inner.write(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner.list(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((RpDelete::default(), DummyDeleter::default()))
    }
}

#[derive(Debug, Default)]
pub struct DummyDeleter {
    queued_paths: Vec<String>,
}

impl oio::Delete for DummyDeleter {
    fn delete(&mut self, path: &str, _args: OpDelete) -> Result<()> {
        warn!(
            "DummyDeleteLayer: intercepted delete request for '{}' (operation disabled for safety)",
            path
        );
        self.queued_paths.push(path.to_string());
        Ok(())
    }

    async fn flush(&mut self) -> Result<usize> {
        if self.queued_paths.is_empty() {
            return Ok(0);
        }
        let paths = self.queued_paths.drain(..).collect::<Vec<_>>();
        warn!(
            "DummyDeleteLayer: rejecting flush of {} delete(s): {:?}",
            paths.len(),
            paths
        );
        Err(Error::new(
            ErrorKind::Unsupported,
            "delete operation is disabled for safety",
        ))
    }
}
