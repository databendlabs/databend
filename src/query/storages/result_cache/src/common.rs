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

use common_exception::Result;
use common_expression::Column;
use common_expression::DataBlock;
use common_io::prelude::deserialize_from_slice;
use common_io::prelude::serialize_into_buf;
use sha2::Digest;
use sha2::Sha256;

const RESULT_CACHE_PREFIX: &str = "_result_cache";

#[inline(always)]
pub fn gen_result_cache_key(raw: &str) -> String {
    format!("{:x}", Sha256::digest(raw))
}

#[inline(always)]
pub(crate) fn gen_result_cache_meta_key(tenant: &str, key: &str) -> String {
    format!("{RESULT_CACHE_PREFIX}/{tenant}/{key}")
}

#[inline(always)]
pub(crate) fn gen_result_cache_dir(key: &str) -> String {
    format!("{RESULT_CACHE_PREFIX}/{key}")
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ResultCacheValue {
    /// The original query SQL.
    pub sql: String,
    /// The query time.
    pub query_time: u64,
    /// Time-to-live of this query.
    pub ttl: u64,
    /// The size of the result cache (bytes).
    pub result_size: usize,
    /// The number of rows in the result cache.
    pub num_rows: usize,
    /// The sha256 of the partitions for each table in the query.
    pub partitions_shas: Vec<String>,
    /// The location of the result cache file.
    pub location: String,
}

pub(crate) fn write_blocks_to_buffer(blocks: &[DataBlock], buf: &mut Vec<u8>) -> Result<()> {
    let columns = blocks
        .iter()
        .map(|b| {
            b.convert_to_full()
                .columns()
                .iter()
                .map(|c| c.value.as_column().unwrap().clone())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    serialize_into_buf(buf, &columns)
}

pub(crate) fn read_blocks_from_buffer(buf: &mut &[u8]) -> Result<Vec<DataBlock>> {
    let cols: Vec<Vec<Column>> = deserialize_from_slice(buf)?;
    let blocks = cols
        .into_iter()
        .map(DataBlock::new_from_columns)
        .collect::<Vec<_>>();
    Ok(blocks)
}
