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
//

use storages_common_cache::BytesMemoryCacheReader;
use storages_common_table_meta::caches::CacheManager;

use crate::io::read::column_data_loader::ColumnDataLoader;

pub type BloomIndexColumnDataReader = BytesMemoryCacheReader<Vec<u8>, ColumnDataLoader>;
pub fn new_bloom_index_column_data_reader(
    accessor: ColumnDataLoader,
) -> BloomIndexColumnDataReader {
    BloomIndexColumnDataReader::new(
        CacheManager::instance().get_bloom_index_cache(),
        "BLOOM_INDEX_DATA_CACHE".to_owned(),
        accessor,
    )
}
