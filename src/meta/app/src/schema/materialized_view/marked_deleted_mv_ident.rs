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

use super::MVId;
use crate::schema::EmptyProto;
use crate::tenant_key::ident::TIdent;
use crate::tenant_key::resource::TenantResource;

/// Identifies an MV that is no longer visible but still awaits physical GC.
/// `db_id` partitions tombstones for database-scoped vacuum.
#[derive(Clone, Debug, Eq, PartialEq, kvapi::KeyCodec)]
pub struct MarkedDeletedMVId {
    pub db_id: u64,
    pub mv_id: MVId,
}

impl MarkedDeletedMVId {
    pub fn new(db_id: u64, mv_id: MVId) -> Self {
        Self { db_id, mv_id }
    }
}

pub type MarkedDeletedMVIdent = TIdent<MarkedDeletedMVResource, MarkedDeletedMVId>;

/// Durable work item for reclaiming an MV's Fuse-backed data and metadata.
pub struct MarkedDeletedMVResource;

impl TenantResource for MarkedDeletedMVResource {
    const PREFIX: &'static str = "__fd_marked_deleted_materialized_view";
    const TYPE: &'static str = "MarkedDeletedMVIdent";
    const HAS_TENANT: bool = true;
    type ValueType = EmptyProto;
}

#[cfg(test)]
mod tests {
    use databend_meta_client::kvapi::testing::assert_round_trip;

    use super::MarkedDeletedMVIdent;
    use crate::schema::materialized_view::MVId;
    use crate::schema::materialized_view::MarkedDeletedMVId;
    use crate::tenant::Tenant;

    #[test]
    fn test_marked_deleted_mv_ident() {
        let ident = MarkedDeletedMVIdent::new_generic(
            Tenant::new_literal("tenant1"),
            MarkedDeletedMVId::new(3, MVId::new(42)),
        );
        assert_round_trip(ident, "__fd_marked_deleted_materialized_view/tenant1/3/42");
    }
}
