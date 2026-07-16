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

use super::MVId;
use crate::tenant_key::ident::TIdent;

pub type MVDefinitionIdent = TIdent<MVDefinitionResource, MVId>;

pub struct MVDefinitionResource;

impl crate::tenant_key::resource::TenantResource for MVDefinitionResource {
    const PREFIX: &'static str = "__fd_materialized_view_definitions";
    const TYPE: &'static str = "MVDefinitionIdent";
    const HAS_TENANT: bool = true;
    type ValueType = super::MVDefinition;
}

#[cfg(test)]
mod tests {
    use databend_meta_client::kvapi::testing::assert_round_trip;

    use super::MVDefinitionIdent;
    use crate::schema::materialized_view::MVId;
    use crate::tenant::Tenant;

    #[test]
    fn test_mv_definition_ident() {
        let ident = MVDefinitionIdent::new_generic(Tenant::new_literal("tenant1"), MVId::new(42));
        assert_round_trip(ident, "__fd_materialized_view_definitions/tenant1/42");
    }
}
