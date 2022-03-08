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

use common_exception::ErrorCode;
use common_exception::Result;
use futures::AsyncRead;
use serde_json::from_slice;

use crate::storages::fuse::io::VersionedLoader;
use crate::storages::fuse::meta::v1::snapshot::TableSnapshot;

pub enum SnapshotVersions {
    V0,
    V1,
}

impl TryFrom<u64> for SnapshotVersions {
    type Error = ErrorCode;
    fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(SnapshotVersions::V0),
            1 => Ok(SnapshotVersions::V1),
            _ => Err(ErrorCode::LogicalError("unknown snapshot version")),
        }
    }
}

// TODO more type safe versioned meta data and loader

#[async_trait::async_trait]
impl VersionedLoader<TableSnapshot> for SnapshotVersions {
    async fn vload<R>(
        &self,
        mut reader: R,
        _location: &str,
        _len_hint: Option<u64>,
    ) -> Result<TableSnapshot>
    where
        R: AsyncRead + Unpin + Send,
    {
        let mut buffer = vec![];

        use futures::AsyncReadExt;

        use crate::storages::fuse::meta::v0::snapshot::TableSnapshot as TableSnapshotV0;
        reader.read_to_end(&mut buffer).await.map_err(|e| {
            let msg = e.to_string();
            if e.kind() == ErrorKind::NotFound {
                ErrorCode::DalPathNotFound(msg)
            } else {
                ErrorCode::DalTransportError(msg)
            }
        })?;
        let r = match self {
            SnapshotVersions::V1 => from_slice(&buffer)?,
            SnapshotVersions::V0 => from_slice::<TableSnapshotV0>(&buffer)?.into(),
        };
        Ok(r)
    }
}
