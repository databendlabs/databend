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

pub mod marked_deleted_mv_ident;
pub mod mv_definition_ident;
pub mod mv_meta_ident;
pub mod mv_source_index_ident;

use chrono::DateTime;
use chrono::Utc;
use databend_meta_client::types::MatchSeq;
pub use marked_deleted_mv_ident::MarkedDeletedMVId;
pub use marked_deleted_mv_ident::MarkedDeletedMVIdent;
pub use marked_deleted_mv_ident::MarkedDeletedMVResource;
pub use mv_definition_ident::MVDefinitionIdent;
pub use mv_definition_ident::MVDefinitionResource;
pub use mv_meta_ident::MVId;
pub use mv_meta_ident::MVMetaIdent;
pub use mv_meta_ident::Resource as MVMetaResource;
pub use mv_source_index_ident::MVSourceIndex;
pub use mv_source_index_ident::MVSourceIndexIdent;
pub use mv_source_index_ident::MVSourceIndexResource;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MVState {
    Valid,
    Stale,
    Refreshing,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceProgress {
    pub table_meta_seq: u64,
    pub snapshot_location: Option<String>,
}

/// Mutable synchronization and refresh metadata for a materialized view.
///
/// The MV identity is carried by `MVMetaIdent`; it is intentionally not
/// duplicated in this value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MVMeta {
    pub source_table_id: u64,
    pub source_progress: SourceProgress,
    pub state: MVState,
    pub last_refresh_time: Option<DateTime<Utc>>,
    pub last_refresh_error: Option<String>,
}

/// Update the mutable metadata of a materialized view with optimistic
/// concurrency control.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateMVMetaReq {
    pub ident: MVMetaIdent,
    /// Expected sequence of the `MVMeta` stored at `ident`.
    pub mv_meta_seq: MatchSeq,
    pub new_mv_meta: MVMeta,
}

/// Optimistic concurrency condition for the complete MV dependency set of a
/// source table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MVSourceIndexCondition {
    pub ident: MVSourceIndexIdent,
    /// Expected sequence of the `MVSourceIndex` stored at `ident`.
    pub source_index_seq: u64,
}

/// Immutable defining query for a materialized view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MVDefinition {
    pub original_query: String,
    pub query: String,
}
