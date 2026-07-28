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

use crate::tenant_key::ident::TIdent;
use crate::tenant_key::resource::TenantResource;

/// `__fd_materialized_view_source_binding_version/<tenant>/<source_table_id>`
/// -> [`MVSourceBindingVersion`]
///
/// This value is not `EmptyProto` with the KV sequence used as generation. A
/// missing key is observed as generation 0, but its first put receives a
/// nonzero KV sequence, so a relationship atomically bound to 0 would be stale
/// immediately. The explicit generation lets the first successful CREATE MV
/// atomically write both generation 0 and a relationship bound to generation
/// 0. If CREATE fails, neither record is published. CREATE uses the KV sequence
/// as an internal CAS token, while source TableMeta CAS serializes subsequent
/// generation increments.
pub type MVSourceBindingVersionIdent = TIdent<MVSourceBindingVersionResource, u64>;

/// Current semantic MV-binding generation of one source table.
///
/// A missing record is logically generation 0. Once created, this record is
/// retained for the lifetime of the source table to prevent generation ABA.
/// RENAME/DROP/MODIFY COLUMN advances it atomically with the source TableMeta;
/// ADD COLUMN, table rename, and table DROP/UNDROP leave it unchanged.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MVSourceBindingVersion {
    pub current_source_generation: u64,
}

pub struct MVSourceBindingVersionResource;

impl TenantResource for MVSourceBindingVersionResource {
    const PREFIX: &'static str = "__fd_materialized_view_source_binding_version";
    const TYPE: &'static str = "MVSourceBindingVersionIdent";
    const HAS_TENANT: bool = true;
    type ValueType = MVSourceBindingVersion;
}

#[cfg(test)]
mod tests {
    use databend_meta_client::kvapi::testing::assert_round_trip;

    use super::MVSourceBindingVersionIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_mv_source_binding_version_ident() {
        let ident = MVSourceBindingVersionIdent::new(Tenant::new_literal("tenant1"), 42);
        assert_round_trip(
            ident,
            "__fd_materialized_view_source_binding_version/tenant1/42",
        );
    }
}
