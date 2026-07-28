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

use databend_meta_client::kvapi;

use crate::tenant_key::ident::TIdent;
use crate::tenant_key::resource::TenantResource;

#[derive(Clone, Debug, Eq, PartialEq, kvapi::KeyCodec)]
pub struct SourceTableMV {
    pub source_table_id: u64,
    pub mv_table_id: u64,
}

impl SourceTableMV {
    pub fn new(source_table_id: u64, mv_table_id: u64) -> Self {
        Self {
            source_table_id,
            mv_table_id,
        }
    }
}

/// Source generation to which one materialized view is bound.
///
/// This value cannot be `EmptyProto`: key existence would express both the
/// durable dependency and its validity. Invalidating a source would then
/// require deleting every relationship, losing the dependencies needed to
/// discover invalid MVs for management and refresh. Recording the bound
/// generation keeps the relationship and derives validity by comparison with
/// the expected source generation. The source binding-version key's KV
/// sequence is deliberately not persisted here; it is only an internal
/// transaction CAS token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MVSourceBinding {
    pub bound_source_generation: u64,
}

/// `__fd_materialized_view_by_source/<tenant>/<source_table_id>/<mv_table_id>`
/// -> [`MVSourceBinding`]
pub type SourceTableMVIdent = TIdent<SourceTableMVResource, SourceTableMV>;

pub struct SourceTableMVResource;

impl TenantResource for SourceTableMVResource {
    const PREFIX: &'static str = "__fd_materialized_view_by_source";
    const TYPE: &'static str = "SourceTableMVIdent";
    const HAS_TENANT: bool = true;
    type ValueType = MVSourceBinding;
}

#[cfg(test)]
mod tests {
    use databend_meta_client::kvapi::testing::assert_round_trip;

    use super::SourceTableMV;
    use super::SourceTableMVIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_source_table_mv_ident() {
        let ident = SourceTableMVIdent::new_generic(
            Tenant::new_literal("tenant1"),
            SourceTableMV::new(42, 7),
        );
        assert_round_trip(ident, "__fd_materialized_view_by_source/tenant1/42/7");
    }
}
