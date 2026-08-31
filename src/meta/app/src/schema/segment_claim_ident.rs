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

use crate::tenant::ToTenant;
use crate::tenant_key::ident::TIdent;

#[derive(Debug, Clone, PartialEq, Eq, Hash, kvapi::KeyCodec)]
pub struct SegmentClaimName {
    table_id: u64,
    claim_id: String,
}

/// Identifies one segment claim.
pub type SegmentClaimIdent = TIdent<Resource, SegmentClaimName>;

pub use kvapi_impl::Resource;

pub const SEGMENT_CLAIM_SEQ_KEY: &str = "__fd_segment_claim_seq";

impl SegmentClaimIdent {
    pub fn new(tenant: impl ToTenant, table_id: u64, claim_id: u64) -> Self {
        Self::new_generic(tenant, SegmentClaimName {
            table_id,
            claim_id: format_claim_id(claim_id),
        })
    }

    pub fn table_id(&self) -> u64 {
        self.name().table_id
    }

    pub fn try_claim_id(&self) -> Result<u64, ParseIntError> {
        self.name().claim_id.replace('_', "").parse()
    }
}

fn format_claim_id(claim_id: u64) -> String {
    format!("{:021}", claim_id).chars().enumerate().fold(
        String::new(),
        |mut output, (index, character)| {
            if index > 0 && index % 3 == 0 {
                output.push('_');
            }
            output.push(character);
            output
        },
    )
}

mod kvapi_impl {
    use crate::schema::SegmentClaimMeta;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;

    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_segment_claim";
        const TYPE: &'static str = "SegmentClaimIdent";
        const HAS_TENANT: bool = true;
        type ValueType = SegmentClaimMeta;
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_client::kvapi::testing::assert_round_trip;

    use super::SegmentClaimIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_segment_claim_ident() {
        let ident = SegmentClaimIdent::new(Tenant::new_literal("test"), 5, 6);
        assert_round_trip(
            ident,
            "__fd_segment_claim/test/5/000_000_000_000_000_000_006",
        );
    }
}
