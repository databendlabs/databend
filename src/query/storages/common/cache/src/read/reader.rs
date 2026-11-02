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

use std::borrow::Cow;
use std::future::Future;

use databend_common_exception::Result;

use crate::CachedItem;
use crate::DiskFill;

/// What the memory cache tells a reader on a miss.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadCtx {
    /// What the read may do with the disk cache: background tasks read through read-only
    /// handles, which must not fill it.
    pub disk: DiskFill,
}

impl ReadCtx {
    /// For reads of storage itself rather than of what queries read, e.g. recovery and checks:
    /// neither read nor fill the disk cache.
    pub const BYPASS: ReadCtx = ReadCtx {
        disk: DiskFill::Bypass,
    };
}

impl Default for ReadCtx {
    fn default() -> Self {
        ReadCtx {
            disk: DiskFill::ReadWrite,
        }
    }
}

/// IO and decoding for one kind of data. Knows nothing about the memory cache; reads its bytes
/// through `DiskCachedStorage` with `ctx.disk`.
pub trait Reader: Send + Sync {
    type Key: Send + Sync;
    type Item: CachedItem;

    fn cache_key<'a>(&self, key: &'a Self::Key) -> Cow<'a, str>;

    fn read(
        &self,
        key: &Self::Key,
        ctx: &ReadCtx,
    ) -> impl Future<Output = Result<Self::Item>> + Send;
}
