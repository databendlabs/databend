// Copyright 2023 Datafuse Labs.
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

use crate::providers::BytesCache;
use crate::providers::DiskCache;
use crate::providers::ItemCache;
use crate::read::cached_reader::CachedReader;

pub type InMemoryItemCacheReader<T, L> = CachedReader<T, L, ItemCache<T>>;
pub type InMemoryBytesCacheReader<T, L> = CachedReader<T, L, BytesCache>;
// NOTE: dummy impl, just for api testing
pub type DiskCacheReader<T, L> = CachedReader<T, L, DiskCache>;
