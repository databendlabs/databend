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

use std::collections::HashMap;
use std::sync::LazyLock;

use databend_common_config::CacheConfig;

use crate::CacheDef;
use crate::CacheKind;
use crate::Pool;
use crate::Unit;
use crate::legacy::LegacyCapacity;

/// How the legacy backend sizes one kind.
#[derive(Clone, Copy, Debug)]
pub struct LegacyKind {
    /// The options sizing the kind, named when the shared backend ignores them.
    pub options: &'static str,
    pub unit: Unit,
    /// Capacity in `unit`, from the config and `max_server_memory_usage`; 0 disables the kind.
    pub size: fn(&CacheConfig, u64) -> u64,
    /// Values held by readers stay counted against the capacity, and only values nobody holds
    /// are evicted for room.
    pub counts_held_values: bool,
    /// Resizing the kind also resizes this one: the two share one budget, see `split_budget`.
    pub shares_budget_with: Option<CacheKind>,
}

impl LegacyKind {
    /// Bounded by its number of values.
    pub fn items(options: &'static str, size: fn(&CacheConfig, u64) -> u64) -> Self {
        Self::new(options, Unit::Count, size)
    }

    /// Bounded by the bytes of its values.
    pub fn bytes(options: &'static str, size: fn(&CacheConfig, u64) -> u64) -> Self {
        Self::new(options, Unit::Bytes, size)
    }

    pub fn counting_held_values(self) -> Self {
        LegacyKind {
            counts_held_values: true,
            ..self
        }
    }

    pub fn sharing_budget_with<D: CacheDef>(self) -> Self {
        LegacyKind {
            shares_budget_with: Some(CacheKind::of::<D>()),
            ..self
        }
    }

    /// A shared budget of `total`: three quarters for the kind, one for the other.
    pub fn split_budget(total: usize) -> (usize, usize) {
        let other = total / 4;
        (total - other, other)
    }

    fn new(options: &'static str, unit: Unit, size: fn(&CacheConfig, u64) -> u64) -> Self {
        LegacyKind {
            options,
            unit,
            size,
            counts_held_values: false,
            shares_budget_with: None,
        }
    }
}

/// The kinds the legacy backend supports. The others are disabled in it.
#[derive(Default)]
pub struct LegacyKinds {
    kinds: HashMap<CacheKind, LegacyKind>,
}

impl LegacyKinds {
    /// The built-in legacy kinds, see `LegacyKinds::builtin`.
    pub fn global() -> &'static LegacyKinds {
        static KINDS: LazyLock<LegacyKinds> = LazyLock::new(LegacyKinds::builtin);
        &KINDS
    }

    pub fn register<D: CacheDef>(&mut self, kind: LegacyKind) -> &mut Self {
        let previous = self.kinds.insert(CacheKind::of::<D>(), kind);
        assert!(
            previous.is_none(),
            "legacy cache {} is registered twice",
            D::NAME
        );
        self
    }

    pub fn get(&self, kind: CacheKind) -> Option<&LegacyKind> {
        self.kinds.get(&kind)
    }

    /// The capacity of every kind. `enable_table_meta_cache = false` disables every kind
    /// outside the data pool, as before.
    pub fn capacities(
        &self,
        config: &CacheConfig,
        max_server_memory_usage: u64,
    ) -> Vec<(CacheKind, LegacyCapacity)> {
        CacheKind::all()
            .map(|kind| {
                let capacity = match self.get(kind) {
                    Some(_) if !config.enable_table_meta_cache && kind.pool() != Pool::Data => {
                        LegacyCapacity::Disabled
                    }
                    Some(legacy) => {
                        let size = (legacy.size)(config, max_server_memory_usage) as usize;
                        match (size, legacy.unit) {
                            (0, _) => LegacyCapacity::Disabled,
                            (n, Unit::Count) => LegacyCapacity::Items(n),
                            (n, Unit::Bytes) => LegacyCapacity::Bytes(n),
                        }
                    }
                    None => LegacyCapacity::Disabled,
                };
                (kind, capacity)
            })
            .collect()
    }

    /// The legacy options `config` changes; the shared backend does not read them.
    pub fn ignored_options(
        &self,
        config: &CacheConfig,
        max_server_memory_usage: u64,
    ) -> Vec<&'static str> {
        let default = CacheConfig::default();
        let mut ignored = Vec::new();
        if config.enable_table_meta_cache != default.enable_table_meta_cache {
            ignored.push("enable_table_meta_cache");
        }
        for legacy in CacheKind::all().filter_map(|kind| self.get(kind)) {
            let size = |config| (legacy.size)(config, max_server_memory_usage);
            if size(config) != size(&default) && !ignored.contains(&legacy.options) {
                ignored.push(legacy.options);
            }
        }
        ignored
    }
}
