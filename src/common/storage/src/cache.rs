//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::io::Cursor;
use futures::AsyncReadExt;
use opendal::layers::CachePolicy;
use opendal::raw::Accessor;
use opendal::raw::BytesReader;
use opendal::raw::RpRead;
use opendal::Error;
use opendal::ErrorKind;
use opendal::OpRead;
use opendal::OpWrite;
use opendal::Result;

/// TODO: implement more complex cache logic.
///
/// For example:
///
/// - Implement a top n heap, and only cache files exist in heap.
/// - Only cache data file, and ignore snapshot files.
#[derive(Debug, Default)]
pub struct FuseCachePolicy {}

impl FuseCachePolicy {
    pub fn new() -> Self {
        FuseCachePolicy::default()
    }

    fn cache_path(&self, path: &str, args: &OpRead) -> String {
        format!("{path}.cache-{}", args.range().to_header())
    }
}

#[async_trait]
impl CachePolicy for FuseCachePolicy {
    fn on_read(
        &self,
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        path: &str,
        args: OpRead,
    ) -> BoxFuture<'static, Result<(RpRead, BytesReader)>> {
        let path = path.to_string();
        let cache_path = self.cache_path(&path, &args);
        Box::pin(async move {
            match cache.read(&cache_path, OpRead::default()).await {
                Ok(v) => Ok(v),
                Err(err) if err.kind() == ErrorKind::ObjectNotFound => {
                    let (rp, mut r) = inner.read(&path, args.clone()).await?;

                    let size = rp.clone().into_metadata().content_length();
                    // If size < 8MiB, we can optimize by buffer in memory.
                    // TODO: make this configurable.
                    if size <= 8 * 1024 * 1024 {
                        let mut bs = Vec::with_capacity(size as usize);
                        r.read_to_end(&mut bs).await.map_err(|err| {
                            Error::new(
                                ErrorKind::Unexpected,
                                "read from underlying storage service",
                            )
                            .set_source(err)
                        })?;
                        let bs = Bytes::from(bs);

                        // Ignore errors returned by cache services.
                        let _ = cache
                            .write(
                                &cache_path,
                                OpWrite::new(size),
                                Box::new(Cursor::new(bs.clone())),
                            )
                            .await;
                        Ok((rp, Box::new(Cursor::new(bs)) as BytesReader))
                    } else {
                        // Ignore errors returned by cache services.
                        let _ = cache.write(&cache_path, OpWrite::new(size), r).await;

                        match cache.read(&cache_path, OpRead::default()).await {
                            Ok(v) => Ok(v),
                            Err(_) => return inner.read(&path, args).await,
                        }
                    }
                }
                Err(_) => return inner.read(&path, args).await,
            }
        })
    }
}
