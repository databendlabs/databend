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

#[derive(Clone, Debug, Eq, PartialEq, kvapi::KeyCodec)]
pub struct MVSourceIndex {
    pub source_table_id: u64,
    pub mv_id: MVId,
}

impl MVSourceIndex {
    pub fn new(source_table_id: u64, mv_id: MVId) -> Self {
        Self {
            source_table_id,
            mv_id,
        }
    }
}

pub type MVSourceIndexIdent = TIdent<MVSourceIndexResource, MVSourceIndex>;

/// One reverse dependency edge from a source table to a materialized view.
pub struct MVSourceIndexResource;

impl TenantResource for MVSourceIndexResource {
    const PREFIX: &'static str = "__fd_materialized_view_by_source";
    const TYPE: &'static str = "MVSourceIndexIdent";
    const HAS_TENANT: bool = true;
    type ValueType = EmptyProto;
}

#[cfg(test)]
mod tests {
    use databend_meta_client::kvapi::testing::assert_round_trip;

    use super::MVSourceIndex;
    use super::MVSourceIndexIdent;
    use crate::schema::materialized_view::MVId;
    use crate::tenant::Tenant;

    #[test]
    fn test_mv_source_index_ident() {
        let ident = MVSourceIndexIdent::new_generic(
            Tenant::new_literal("tenant1"),
            MVSourceIndex::new(7, MVId::new(42)),
        );
        assert_round_trip(ident, "__fd_materialized_view_by_source/tenant1/7/42");
    }
}
