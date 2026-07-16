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

use crate::data_id::DataId;
use crate::tenant_key::ident::TIdent;

pub type MVId = DataId<Resource>;
pub type MVMetaIdent = TIdent<Resource, MVId>;

pub struct Resource;

impl crate::tenant_key::resource::TenantResource for Resource {
    const PREFIX: &'static str = "__fd_materialized_views";
    const TYPE: &'static str = "MVMetaIdent";
    const HAS_TENANT: bool = true;
    type ValueType = super::MVMeta;
}

#[cfg(test)]
mod tests {
    use databend_meta_client::kvapi::testing::assert_round_trip;

    use super::MVId;
    use super::MVMetaIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_mv_meta_ident() {
        let ident = MVMetaIdent::new_generic(Tenant::new_literal("tenant1"), MVId::new(42));
        assert_round_trip(ident, "__fd_materialized_views/tenant1/42");
    }
}
