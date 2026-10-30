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
use std::sync::Arc;
use std::time::Duration;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CatalogInfo;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::IcebergTableCacheValue;
use databend_storages_common_cache::Reader;
use databend_storages_common_cache::TTLReader;

use crate::IcebergTable;
use crate::credential::credential_refresh_at;

/// Loads an iceberg table by its `table_cache_key`.
pub struct IcebergTableLoader {
    catalog: Arc<dyn iceberg::Catalog>,
    info: Arc<CatalogInfo>,
}

pub type IcebergTableReader = TTLReader<IcebergTableLoader>;

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

impl Reader for IcebergTableLoader {
    type Key = String;
    type Item = IcebergTableCacheValue;

    fn cache_key<'a>(&self, key: &'a String) -> Cow<'a, str> {
        Cow::Borrowed(key)
    }

    #[async_backtrace::framed]
    async fn read(&self, key: &String) -> Result<IcebergTableCacheValue> {
        let (database_name, table_name) = parse_table_cache_key(key)?;
        let iceberg = IcebergTable::try_create_from_iceberg_catalog(
            self.catalog.clone(),
            self.info.clone(),
            database_name,
            table_name,
        )
        .await?;

        let credential_refresh_at = credential_refresh_at(iceberg.iceberg_table());
        Ok(IcebergTableCacheValue::new(
            Arc::new(iceberg),
            credential_refresh_at,
        ))
    }
}

pub fn iceberg_table_cache_reader(
    catalog: Arc<dyn iceberg::Catalog>,
    info: Arc<CatalogInfo>,
) -> IcebergTableReader {
    let cache = CacheManager::instance()
        .kind_manager()
        .mem_cache(CacheKind::IcebergTable);
    IcebergTableReader::new(
        cache,
        IcebergTableLoader { catalog, info },
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
