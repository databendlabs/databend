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

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use std::time::Instant;

use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CatalogInfo;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::InMemoryCacheTTLReader;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_cache::Loader;

use crate::IcebergTable;

pub struct LoaderWrapper<T>(T);
pub type IcebergTableReader = InMemoryCacheTTLReader<
    (Arc<dyn Table>, AtomicBool, Instant),
    LoaderWrapper<(Arc<dyn iceberg::Catalog>, Arc<CatalogInfo>)>,
>;

pub(crate) const SEP_STR: &str = "\u{001f}";

#[async_trait::async_trait]
impl Loader<(Arc<dyn Table>, AtomicBool, Instant)>
    for LoaderWrapper<(Arc<dyn iceberg::Catalog>, Arc<CatalogInfo>)>
{
    #[async_backtrace::framed]
    async fn load(&self, params: &LoadParams) -> Result<(Arc<dyn Table>, AtomicBool, Instant)> {
        let keys: Vec<&str> = params.location.split(SEP_STR).collect();
        let inner = &self.0;
        let iceberg = IcebergTable::try_create_from_iceberg_catalog(
            inner.0.clone(),
            inner.1.clone(),
            keys[0],
            keys[1],
        )
        .await?;

        let tbl = Arc::new(iceberg) as Arc<dyn Table>;
        Ok((tbl, AtomicBool::new(false), Instant::now()))
    }
}

pub fn iceberg_table_cache_reader(
    catalog: Arc<dyn iceberg::Catalog>,
    info: Arc<CatalogInfo>,
) -> IcebergTableReader {
    IcebergTableReader::new(
        CacheManager::instance().get_iceberg_table_cache(),
        LoaderWrapper((catalog, info)),
        Duration::from_secs(600),
    )
}
