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

use std::any::TypeId;
use std::fmt;
use std::sync::LazyLock;

use crate::CacheDef;
use crate::DiskInfo;
use crate::KindLimit;
use crate::Pool;

/// A registered cache kind: its position in the registry.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CacheKind(u16);

impl CacheKind {
    /// The kind defined by `D` in the process-wide registry.
    pub fn of<D: CacheDef>() -> CacheKind {
        CacheRegistry::global().kind_of::<D>()
    }

    /// Every registered kind, in registration order.
    pub fn all() -> impl Iterator<Item = CacheKind> {
        CacheRegistry::global().kinds()
    }

    /// Accepts the kind name (`memory_cache_*`) and the name of its pre-refactor hybrid cache,
    /// the same without the `memory_` prefix.
    pub fn from_name(name: &str) -> Option<CacheKind> {
        CacheRegistry::global().find(name)
    }

    pub fn info(self) -> &'static KindInfo {
        CacheRegistry::global().info(self)
    }

    pub fn name(self) -> &'static str {
        self.info().name
    }

    pub fn pool(self) -> Pool {
        self.info().pool
    }

    /// Position in the registry, for per-kind state kept in a slice.
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

impl fmt::Debug for CacheKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// What the backends and the disk tiers know about a kind, taken from its `CacheDef`.
#[derive(Debug)]
pub struct KindInfo {
    pub name: &'static str,
    pub pool: Pool,
    pub shared_limit: KindLimit,
    pub disk: Option<DiskInfo>,
    def: TypeId,
}

/// The cache kinds of the process. Kinds are identified by their `CacheDef` type and numbered in
/// registration order, which is also the order of `system.caches`.
#[derive(Default)]
pub struct CacheRegistry {
    kinds: Vec<KindInfo>,
}

impl CacheRegistry {
    /// The registry with every built-in kind, see `CacheRegistry::builtin`.
    pub fn global() -> &'static CacheRegistry {
        static REGISTRY: LazyLock<CacheRegistry> = LazyLock::new(CacheRegistry::builtin);
        &REGISTRY
    }

    /// Every kind registers the same way; a disk tier comes with its definition.
    pub fn register<D: CacheDef>(&mut self) -> &mut Self {
        assert!(
            self.find(D::NAME).is_none() && self.position::<D>().is_none(),
            "cache {} is registered twice",
            D::NAME
        );
        self.kinds.push(KindInfo {
            name: D::NAME,
            pool: D::POOL,
            shared_limit: D::SHARED_LIMIT,
            disk: D::DISK.map(|disk| disk.info),
            def: TypeId::of::<D>(),
        });
        self
    }

    /// Panics if `D` is not registered: every `CacheDef` must be.
    pub fn kind_of<D: CacheDef>(&self) -> CacheKind {
        match self.position::<D>() {
            Some(index) => CacheKind(index as u16),
            None => panic!("cache {} is not registered", D::NAME),
        }
    }

    pub fn kinds(&self) -> impl Iterator<Item = CacheKind> + use<> {
        (0..self.kinds.len() as u16).map(CacheKind)
    }

    pub fn info(&self, kind: CacheKind) -> &KindInfo {
        &self.kinds[kind.index()]
    }

    pub fn find(&self, name: &str) -> Option<CacheKind> {
        self.kinds
            .iter()
            .position(|k| k.name == name || k.name.strip_prefix("memory_") == Some(name))
            .map(|index| CacheKind(index as u16))
    }

    fn position<D: CacheDef>(&self) -> Option<usize> {
        self.kinds.iter().position(|k| k.def == TypeId::of::<D>())
    }
}
