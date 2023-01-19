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

use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use opendal::Operator;
use storages_common_table_meta::meta::BlockBloomFilterIndexVersion;
use storages_common_table_meta::meta::BlockFilter;
use storages_common_table_meta::meta::Location;

use super::loader::load_bloom_filter_by_columns;

#[async_trait::async_trait]
pub trait BloomFilterReader {
    async fn read_filter(
        &self,
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        columns: &[String],
        index_length: u64,
    ) -> common_exception::Result<BlockFilter>;
}

#[async_trait::async_trait]
impl BloomFilterReader for Location {
    async fn read_filter(
        &self,
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        columns: &[String],
        index_length: u64,
    ) -> common_exception::Result<BlockFilter> {
        let (path, ver) = &self;
        let index_version = BlockBloomFilterIndexVersion::try_from(*ver)?;
        match index_version {
            BlockBloomFilterIndexVersion::V0(_) => Err(ErrorCode::DeprecatedIndexFormat(
                "bloom filter index version(v0) is deprecated",
            )),
            BlockBloomFilterIndexVersion::V2(_) | BlockBloomFilterIndexVersion::V3(_) => {
                let res =
                    load_bloom_filter_by_columns(ctx, dal, columns, path, index_length).await?;
                Ok(res)
            }
        }
    }
}
