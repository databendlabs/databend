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

use databend_common_config::CacheConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::info;

use crate::CacheKind;
use crate::KindLimit;
use crate::Pool;
use crate::shared::SharedStore;

const MAX_TOTAL_RATIO: u64 = 80;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolCaps {
    pub meta: usize,
    pub index: usize,
    pub data: usize,
}

impl PoolCaps {
    pub fn from_ratios(memory_limit: usize, meta: u64, index: u64, data: u64) -> Result<Self> {
        let total = meta + index + data;
        if total > MAX_TOTAL_RATIO {
            return Err(ErrorCode::InvalidConfig(format!(
                "cache pool ratios sum to {total}%, must be at most {MAX_TOTAL_RATIO}%"
            )));
        }
        let of = |ratio: u64| (memory_limit as u128 * ratio as u128 / 100) as usize;
        Ok(PoolCaps {
            meta: of(meta),
            index: of(index),
            data: of(data),
        })
    }

    pub fn get(&self, pool: Pool) -> usize {
        match pool {
            Pool::Meta => self.meta,
            Pool::Index => self.index,
            Pool::Data => self.data,
        }
    }
}

impl SharedStore {
    /// Pools sized by ratio of the server memory limit, `max_server_memory_usage`, which must
    /// be set: the pools are bounded by it and reclaimed when it is reached.
    pub fn from_config(config: &CacheConfig, max_server_memory_usage: u64) -> Result<SharedStore> {
        // The pools are a share of the server memory limit, and shrink when it is reached:
        // without a limit they would neither be bounded by it nor ever be reclaimed.
        let memory_limit = match max_server_memory_usage {
            0 => {
                return Err(ErrorCode::InvalidConfig(
                    "cache.shared_memory_enabled requires a server memory limit: set \
                     query.max_memory_limit_enabled and query.max_server_memory_usage",
                ));
            }
            limit => limit,
        };
        let memory_limit = usize::try_from(memory_limit)
            .map_err(|_| ErrorCode::InvalidConfig("memory limit exceeds this platform"))?;
        let caps = PoolCaps::from_ratios(
            memory_limit,
            config.meta_max_ratio,
            config.index_max_ratio,
            config.data_max_ratio,
        )?;
        let limits = config
            .kind_max_bytes
            .iter()
            .map(|(name, bytes)| {
                let kind = CacheKind::from_name(name).ok_or_else(|| {
                    ErrorCode::InvalidConfig(format!(
                        "unknown cache '{name}' in cache.kind_max_bytes"
                    ))
                })?;
                let bytes = usize::try_from(*bytes).map_err(|_| {
                    ErrorCode::InvalidConfig(format!(
                        "cache.kind_max_bytes.{name} exceeds this platform"
                    ))
                })?;
                Ok((kind, KindLimit::bytes(bytes)))
            })
            .collect::<Result<Vec<_>>>()?;
        let shards = std::thread::available_parallelism().map_or(8, |n| n.get().max(8));
        info!(
            "[CacheManager] Shared cache pools: meta {} bytes, index {} bytes, data {} bytes",
            caps.meta, caps.index, caps.data
        );
        Ok(SharedStore::new(caps, limits, shards))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_ratios() {
        let caps = PoolCaps::from_ratios(1000, 10, 40, 10).unwrap();
        assert_eq!(caps, PoolCaps {
            meta: 100,
            index: 400,
            data: 100
        });
        assert!(PoolCaps::from_ratios(1000, 30, 40, 11).is_err());
    }
}
