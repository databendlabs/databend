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

use std::io::SeekFrom;

use async_compat::CompatExt;
use async_trait::async_trait;
use common_exception::Result;
use tokio;
use tokio::io::AsyncSeekExt;

use crate::ops::Delete;
use crate::ops::Object;
use crate::ops::Read;
use crate::ops::ReadBuilder;
use crate::ops::Reader;
use crate::ops::Stat;
use crate::ops::Write;
use crate::ops::WriteBuilder;

/// TODO: https://github.com/datafuselabs/databend/issues/3677
#[derive(Default)]
pub struct Builder {}

impl Builder {
    pub fn finish(self) -> Backend {
        Backend {}
    }
}

/// TODO: https://github.com/datafuselabs/databend/issues/3677
pub struct Backend {}

impl Backend {
    pub fn build() -> Builder {
        Builder::default()
    }
}

#[async_trait]
impl<S: Send + Sync> Read<S> for Backend {
    async fn read(&self, args: &ReadBuilder<S>) -> Result<Reader> {
        let mut f = tokio::fs::OpenOptions::new()
            .read(true)
            .open(&args.path)
            .await
            .unwrap();

        if args.offset.is_some() {
            f.seek(SeekFrom::Start(args.offset.unwrap() as u64)).await?;
        }

        if args.size.is_some() {
            f.set_len(args.size.unwrap()).await?;
        }

        Ok(Box::new(f.compat()))
    }
}

#[async_trait]
impl<S: Send + Sync> Write<S> for Backend {
    async fn write(&self, mut r: Reader, args: &WriteBuilder<S>) -> Result<usize> {
        let mut f = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&args.path)
            .await
            .unwrap();

        let s = tokio::io::copy(&mut r.compat_mut(), &mut f).await.unwrap();

        Ok(s as usize)
    }
}

#[async_trait]
impl<S: Send + Sync> Stat<S> for Backend {
    async fn stat(&self, path: &str) -> Result<Object> {
        let meta = tokio::fs::metadata(path).await.unwrap();
        let o = Object {
            path: path.to_string(),
            size: meta.len(),
        };
        Ok(o)
    }
}

#[async_trait]
impl<S: Send + Sync> Delete<S> for Backend {
    async fn delete(&self, path: &str) -> Result<()> {
        tokio::fs::remove_file(path).await.unwrap();
        Ok(())
    }
}
