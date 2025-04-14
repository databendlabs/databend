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

use databend_common_exception::ErrorCode;
use parquet::format::FileMetaData;
use serde::Deserialize;
use serde::Serialize;

use crate::BloomIndexMeta;

#[derive(Clone, Serialize, Deserialize)]
pub struct NgramIndexMeta {
    pub inner: BloomIndexMeta,
}

impl TryFrom<&NgramIndexMeta> for Vec<u8> {
    type Error = ErrorCode;
    fn try_from(value: &NgramIndexMeta) -> std::result::Result<Self, Self::Error> {
        Vec::<u8>::try_from(&value.inner)
    }
}

impl TryFrom<&[u8]> for NgramIndexMeta {
    type Error = ErrorCode;

    fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
        Ok(NgramIndexMeta {
            inner: BloomIndexMeta::try_from(value)?,
        })
    }
}

impl TryFrom<FileMetaData> for NgramIndexMeta {
    type Error = databend_common_exception::ErrorCode;

    fn try_from(meta: FileMetaData) -> std::result::Result<Self, Self::Error> {
        Ok(NgramIndexMeta {
            inner: BloomIndexMeta::try_from(meta)?,
        })
    }
}
