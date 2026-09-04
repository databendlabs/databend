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

use std::collections::BTreeSet;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;

use crate::tenant::Tenant;

/// Maximum number of unique segment locations protected by one maintenance claim.
pub const MAX_SEGMENT_LOCATIONS_PER_CLAIM: usize = 128;

/// A short-lived maintenance claim over immutable segment objects.
///
/// Claims reduce duplicate maintenance work. Snapshot OCC remains the
/// correctness boundary for committing changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentClaimMeta {
    pub user: String,
    pub node: String,
    pub query_id: String,
    pub created_on: DateTime<Utc>,
    pub segment_locations: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListSegmentClaimsReq {
    pub tenant: Tenant,
    pub table_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSegmentClaimReq {
    pub tenant: Tenant,
    pub table_id: u64,
    pub ttl: Duration,
    pub user: String,
    pub node: String,
    pub query_id: String,
    pub segment_locations: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSegmentClaimReply {
    pub claim_id: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtendSegmentClaimReq {
    pub tenant: Tenant,
    pub table_id: u64,
    pub claim_id: u64,
    pub ttl: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteSegmentClaimReq {
    pub tenant: Tenant,
    pub table_id: u64,
    pub claim_id: u64,
}
