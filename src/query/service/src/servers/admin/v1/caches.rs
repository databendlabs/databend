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

use databend_common_config::GlobalConfig;
use databend_storages_common_cache::CacheManager;
use poem::IntoResponse;
use poem::web::Json;
use serde::Serialize;

#[derive(Serialize)]
pub struct CacheInfo {
    pub node: String,
    pub name: String,
    pub pool: Option<String>,
    pub is_pool: bool,
    pub enabled: bool,
    pub num_items: u64,
    /// Charged bytes, including values evicted but still held by readers.
    pub size: u64,
    pub in_cache_size: u64,
    /// Limit in `unit`; null means bounded only by the containing pool.
    pub capacity: Option<u64>,
    pub unit: String,
    pub access: u64,
    pub hit: u64,
    pub miss: u64,
}

/// Local cache statistics, including disabled caches and pool summaries.
/// Unlike a SQL query of system.caches, this does not contact other nodes or create a session.
/// Access, hit and miss counters are cumulative, not rates. Pool rows overlap their cache rows.
/// Served on the internal admin port: `curl http://127.0.0.1:8080/v1/caches`.
#[poem::handler]
#[async_backtrace::framed]
pub async fn caches_handler() -> poem::Result<impl IntoResponse> {
    let config = GlobalConfig::instance();
    let caches = CacheManager::instance()
        .rows()
        .into_iter()
        .map(|row| CacheInfo {
            node: config.query.node_id.clone(),
            name: row.name,
            pool: row.pool.map(|pool| pool.name().to_string()),
            is_pool: row.kind.is_none(),
            enabled: row.enabled,
            num_items: row.num_items,
            size: row.size,
            in_cache_size: row.in_cache_size,
            capacity: row.capacity,
            unit: row.unit.to_string(),
            access: row.access,
            hit: row.hit,
            miss: row.miss,
        })
        .collect::<Vec<_>>();
    Ok(Json(caches))
}
