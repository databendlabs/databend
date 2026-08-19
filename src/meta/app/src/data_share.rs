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

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use chrono::DateTime;
use chrono::Utc;
use databend_meta_client::kvapi;

use crate::data_id::DataId;
use crate::tenant_key::ident::TIdent;
use crate::tenant_key::resource::TenantResource;

/// `__fd_data_share/<provider>/<name> -> share_id`.
pub type ShareNameIdent = TIdent<ShareNameResource>;

/// Globally unique ID of a data share.
pub type ShareId = DataId<ShareNameResource>;

pub struct ShareNameResource;

impl TenantResource for ShareNameResource {
    const PREFIX: &'static str = "__fd_data_share";
    const TYPE: &'static str = "ShareNameIdent";
    const HAS_TENANT: bool = true;
    type ValueType = ShareId;
}

/// `__fd_data_share_by_id/<share_id> -> DataShareMeta`.
#[derive(Clone, Debug, Copy, Default, Eq, PartialEq, kvapi::StructKey)]
#[structkey(prefix = "__fd_data_share_by_id")]
pub struct ShareIdIdent {
    pub share_id: u64,
}

impl ShareIdIdent {
    pub fn new(share_id: u64) -> Self {
        Self { share_id }
    }
}

impl kvapi::Key for ShareIdIdent {
    type ValueType = DataShareMeta;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataShareMeta {
    pub provider: String,
    pub name: String,
    pub created_on: DateTime<Utc>,
    pub comment: Option<String>,
    pub accounts: BTreeSet<String>,
    pub database: Option<DataShareDatabaseGrant>,
    pub tables: BTreeMap<String, DataShareTableGrant>,
    /// Provider-owned named storage connection used to read every shared table.
    pub connection: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataShareDatabaseGrant {
    pub database: String,
    pub database_id: u64,
    pub shared_on: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataShareTableGrant {
    pub table_id: u64,
    pub shared_on: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use databend_meta_client::kvapi::testing::assert_round_trip;

    use super::ShareIdIdent;
    use super::ShareNameIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_data_share_keys() {
        assert_round_trip(
            ShareNameIdent::new(Tenant::new_literal("provider"), "sales"),
            "__fd_data_share/provider/sales",
        );
        assert_round_trip(ShareIdIdent::new(42), "__fd_data_share_by_id/42");
    }
}
