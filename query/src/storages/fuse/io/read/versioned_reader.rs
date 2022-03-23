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

use std::io::ErrorKind;
use std::marker::PhantomData;

use common_exception::ErrorCode;
use common_exception::Result;
use futures::AsyncRead;
use serde::de::DeserializeOwned;
use serde_json::from_slice;

use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::SegmentInfoVersion;
use crate::storages::fuse::meta::SnapshotVersion;
use crate::storages::fuse::meta::TableSnapshot;

#[async_trait::async_trait]
pub trait VersionedReader<T> {
    async fn load<R>(&self, read: R) -> Result<T>
    where R: AsyncRead + Unpin + Send;
}

#[async_trait::async_trait]
impl VersionedReader<TableSnapshot> for SnapshotVersion {
    async fn load<R>(&self, reader: R) -> Result<TableSnapshot>
    where R: AsyncRead + Unpin + Send {
        let r = match self {
            SnapshotVersion::V1(v) => do_load_new(reader, v).await?,
            SnapshotVersion::V0(v) => do_load_new(reader, v).await?.into(),
        };
        Ok(r)
    }
}

#[async_trait::async_trait]
impl VersionedReader<SegmentInfo> for SegmentInfoVersion {
    async fn load<R>(&self, reader: R) -> Result<SegmentInfo>
    where R: AsyncRead + Unpin + Send {
        let r = match self {
            SegmentInfoVersion::V1(v) => do_load_new(reader, v).await?,
            SegmentInfoVersion::V0(v) => do_load_new(reader, v).await?.into(),
        };
        Ok(r)
    }
}

async fn do_load_new<R, T>(mut reader: R, _v: &PhantomData<T>) -> Result<T>
where
    T: DeserializeOwned,
    R: AsyncRead + Unpin + Send,
{
    let mut buffer: Vec<u8> = vec![];
    use futures::AsyncReadExt;
    reader.read_to_end(&mut buffer).await.map_err(|e| {
        let msg = e.to_string();
        if e.kind() == ErrorKind::NotFound {
            ErrorCode::DalPathNotFound(msg)
        } else {
            ErrorCode::DalTransportError(msg)
        }
    })?;
    Ok(from_slice::<T>(&buffer)?)
}
