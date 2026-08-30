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

use std::num::ParseIntError;

use databend_meta_client::kvapi;

use crate::schema::table_lock_ident_v2::format_table_lock_revision_v2;
use crate::tenant::ToTenant;
use crate::tenant_key::ident::TIdent;

#[derive(Debug, Clone, PartialEq, Eq, Hash, kvapi::KeyCodec)]
pub struct SegmentRewriteClaimName {
    table_id: u64,
    revision: String,
}

/// Identifies one segment rewrite claim.
pub type SegmentRewriteClaimIdent = TIdent<Resource, SegmentRewriteClaimName>;

pub use kvapi_impl::Resource;

pub const SEGMENT_REWRITE_CLAIM_SEQ_KEY: &str = "__fd_segment_rewrite_claim_seq";

impl SegmentRewriteClaimIdent {
    pub fn new(tenant: impl ToTenant, table_id: u64, revision: u64) -> Self {
        Self::new_generic(tenant, SegmentRewriteClaimName {
            table_id,
            revision: format_table_lock_revision_v2(revision),
        })
    }

    pub fn table_id(&self) -> u64 {
        self.name().table_id
    }

    pub fn try_revision(&self) -> Result<u64, ParseIntError> {
        self.name().revision.replace('_', "").parse()
    }
}

mod kvapi_impl {
    use crate::schema::LockMeta;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;

    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_segment_rewrite_claim";
        const TYPE: &'static str = "SegmentRewriteClaimIdent";
        const HAS_TENANT: bool = true;
        type ValueType = LockMeta;
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_client::kvapi::testing::assert_round_trip;

    use super::SegmentRewriteClaimIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_segment_rewrite_claim_ident() {
        let ident = SegmentRewriteClaimIdent::new(Tenant::new_literal("test"), 5, 6);
        assert_round_trip(
            ident,
            "__fd_segment_rewrite_claim/test/5/000_000_000_000_000_000_006",
        );
    }
}
