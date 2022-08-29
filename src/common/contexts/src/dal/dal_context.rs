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
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use opendal::io_util::observe_read;
use opendal::io_util::ReadEvent;
use opendal::ops::OpCreate;
use opendal::ops::OpDelete;
use opendal::ops::OpList;
use opendal::ops::OpPresign;
use opendal::ops::OpRead;
use opendal::ops::OpStat;
use opendal::ops::OpWrite;
use opendal::ops::PresignedRequest;
use opendal::Accessor;
use opendal::AccessorMetadata;
use opendal::BytesReader;
use opendal::DirStreamer;
use opendal::Layer;
use opendal::ObjectMetadata;

use crate::DalMetrics;

#[derive(Clone, Default, Debug)]
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

    fn get_inner(&self) -> Result<Arc<dyn Accessor>> {
        match &self.inner {
            None => Err(Error::new(
                ErrorKind::Other,
                "dal context must init wrongly, inner accessor is empty",
            )),
            Some(inner) => Ok(inner.clone()),
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
    fn metadata(&self) -> AccessorMetadata {
        self.get_inner()
            .expect("must have valid accessor")
            .metadata()
    }

    fn presign(&self, args: &OpPresign) -> Result<PresignedRequest> {
        self.get_inner()?.presign(args)
    }

    async fn create(&self, args: &OpCreate) -> Result<()> {
        self.get_inner()?.create(args).await
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let metric = self.metrics.clone();

        self.get_inner()?.read(args).await.map(|r| {
            let mut last_pending = None;
            let r = observe_read(r, move |e| {
                let start = match last_pending {
                    None => Instant::now(),
                    Some(t) => t,
                };
                match e {
                    ReadEvent::Pending => last_pending = Some(start),
                    ReadEvent::Read(n) => {
                        last_pending = None;
                        metric.inc_read_bytes(n);
                    }
                    ReadEvent::Error(_) => last_pending = None,
                    _ => {}
                }
                metric.inc_read_bytes_cost(start.elapsed().as_millis() as u64);
            });

            Box::new(r) as BytesReader
        })
    }

    async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {
        let metric = self.metrics.clone();

        let mut last_pending = None;

        let r = observe_read(r, move |e| {
            let start = match last_pending {
                None => Instant::now(),
                Some(t) => t,
            };
            match e {
                ReadEvent::Pending => last_pending = Some(start),
                ReadEvent::Read(n) => {
                    last_pending = None;
                    metric.inc_write_bytes(n);
                }
                ReadEvent::Error(_) => last_pending = None,
                _ => {}
            }
            metric.inc_write_bytes_cost(start.elapsed().as_millis() as u64);
        });

        self.get_inner()?.write(args, Box::new(r)).await
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        self.get_inner()?.stat(args).await
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        self.get_inner()?.delete(args).await
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        self.get_inner()?.list(args).await
    }
}
