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
use crate::tenant_key::resource::TenantResource;

/// All materialized views that depend on one source table.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MVSourceIndex {
    mv_ids: Vec<MVId>,
}

impl MVSourceIndex {
    /// Constructs an index from IDs that are already sorted and unique.
    pub fn try_from_ids(mv_ids: Vec<MVId>) -> Option<Self> {
        mv_ids
            .windows(2)
            .all(|pair| *pair[0] < *pair[1])
            .then_some(Self { mv_ids })
    }

    pub fn mv_ids(&self) -> &[MVId] {
        &self.mv_ids
    }

    pub fn is_empty(&self) -> bool {
        self.mv_ids.is_empty()
    }

    pub fn contains(&self, mv_id: MVId) -> bool {
        self.mv_ids
            .binary_search_by_key(&*mv_id, |candidate| **candidate)
            .is_ok()
    }

    pub fn add(&mut self, mv_id: MVId) -> bool {
        match self
            .mv_ids
            .binary_search_by_key(&*mv_id, |candidate| **candidate)
        {
            Ok(_) => false,
            Err(pos) => {
                self.mv_ids.insert(pos, mv_id);
                true
            }
        }
    }

    pub fn remove(&mut self, mv_id: MVId) -> bool {
        match self
            .mv_ids
            .binary_search_by_key(&*mv_id, |candidate| **candidate)
        {
            Ok(pos) => {
                self.mv_ids.remove(pos);
                true
            }
            Err(_) => false,
        }
    }
}

pub type MVSourceIndexIdent = TIdent<MVSourceIndexResource, u64>;

/// Reverse dependency set for one source table.
pub struct MVSourceIndexResource;

impl TenantResource for MVSourceIndexResource {
    const PREFIX: &'static str = "__fd_materialized_view_by_source";
    const TYPE: &'static str = "MVSourceIndexIdent";
    const HAS_TENANT: bool = true;
    type ValueType = MVSourceIndex;
}

#[cfg(test)]
mod tests {
    use databend_meta_client::kvapi::testing::assert_round_trip;

    use super::MVId;
    use super::MVSourceIndex;
    use super::MVSourceIndexIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_mv_source_index_ident() {
        let ident = MVSourceIndexIdent::new_generic(Tenant::new_literal("tenant1"), 7);
        assert_round_trip(ident, "__fd_materialized_view_by_source/tenant1/7");
    }

    #[test]
    fn test_mv_source_index_add_remove() {
        assert!(MVSourceIndex::try_from_ids(vec![MVId::new(43), MVId::new(42)]).is_none());
        assert!(MVSourceIndex::try_from_ids(vec![MVId::new(42), MVId::new(42)]).is_none());

        let mut index = MVSourceIndex::default();
        assert!(index.add(MVId::new(43)));
        assert!(index.add(MVId::new(42)));
        assert!(!index.add(MVId::new(42)));
        assert_eq!(index.mv_ids(), &[MVId::new(42), MVId::new(43)]);
        assert!(index.remove(MVId::new(42)));
        assert!(!index.remove(MVId::new(42)));
        assert_eq!(index.mv_ids(), &[MVId::new(43)]);
    }
}
