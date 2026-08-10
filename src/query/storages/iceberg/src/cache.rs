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
use databend_common_exception::ErrorCode;
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

pub(crate) fn table_cache_key(catalog_name: &str, database_name: &str, table_name: &str) -> String {
    format!("{catalog_name}{SEP_STR}{database_name}{SEP_STR}{table_name}")
}

fn parse_table_cache_key(cache_key: &str) -> Result<(&str, &str)> {
    let keys: Vec<&str> = cache_key.split(SEP_STR).collect();
    if keys.len() != 3 {
        return Err(ErrorCode::Internal(format!(
            "Invalid iceberg table cache key: {cache_key:?}"
        )));
    }
    Ok((keys[1], keys[2]))
}

#[async_trait::async_trait]
impl Loader<(Arc<dyn Table>, AtomicBool, Instant)>
    for LoaderWrapper<(Arc<dyn iceberg::Catalog>, Arc<CatalogInfo>)>
{
    #[async_backtrace::framed]
    async fn load(&self, params: &LoadParams) -> Result<(Arc<dyn Table>, AtomicBool, Instant)> {
        let (database_name, table_name) = parse_table_cache_key(&params.location)?;
        let inner = &self.0;
        let iceberg = IcebergTable::try_create_from_iceberg_catalog(
            inner.0.clone(),
            inner.1.clone(),
            database_name,
            table_name,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_cache_key_roundtrip() -> Result<()> {
        let cache_key = table_cache_key("iceberg_rest", "db1", "tbl1");
        let (database_name, table_name) = parse_table_cache_key(&cache_key)?;
        assert_eq!(database_name, "db1");
        assert_eq!(table_name, "tbl1");
        Ok(())
    }

    #[test]
    fn test_parse_invalid_table_cache_key() {
        let err = parse_table_cache_key("db1\u{001f}tbl1").unwrap_err();
        assert!(err.message().contains("Invalid iceberg table cache key"));
    }
}
