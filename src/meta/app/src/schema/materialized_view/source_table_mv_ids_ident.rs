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

use crate::schema::SourceTableMVIds;
use crate::tenant_key::ident::TIdent;
use crate::tenant_key::resource::TenantResource;

/// `__fd_materialized_view_by_source/<tenant>/<source_table_id>` -> [`SourceTableMVIds`]
pub type SourceTableMVIdsIdent = TIdent<SourceTableMVIdsResource, u64>;

pub struct SourceTableMVIdsResource;

impl TenantResource for SourceTableMVIdsResource {
    const PREFIX: &'static str = "__fd_materialized_view_by_source";
    const TYPE: &'static str = "SourceTableMVIdsIdent";
    const HAS_TENANT: bool = true;
    type ValueType = SourceTableMVIds;
}

#[cfg(test)]
mod tests {
    use databend_meta_client::kvapi::testing::assert_round_trip;

    use super::SourceTableMVIdsIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_source_table_mv_ids_ident() {
        let ident = SourceTableMVIdsIdent::new(Tenant::new_literal("tenant1"), 42);
        assert_round_trip(ident, "__fd_materialized_view_by_source/tenant1/42");
    }
}
