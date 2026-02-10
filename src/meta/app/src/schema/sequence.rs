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

use std::fmt;

use chrono::DateTime;
use chrono::Utc;
pub use kvapi_impl::SequenceRsc;

use super::CreateOption;
use crate::tenant::Tenant;
use crate::tenant_key::ident::TIdent;

/// Defines the meta-service key for sequence.
pub type SequenceIdent = TIdent<SequenceRsc>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequenceMeta {
    pub create_on: DateTime<Utc>,
    pub update_on: DateTime<Utc>,
    pub comment: Option<String>,
    pub step: i64,
    pub current: u64,

    /// Storage version:
    ///
    /// - By default the version is 0, which stores the value in `current` field.
    /// - With version == 1, it stores the value of the sequence in standalone key that support `FetchAddU64`.
    pub storage_version: u64,
}

impl fmt::Display for SequenceMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SequenceMeta{{ create_on={:?} update_on={:?} comment={:?} step={} current={} }}",
            self.create_on, self.update_on, self.comment, self.step, self.current
        )
    }
}

impl From<CreateSequenceReq> for SequenceMeta {
    fn from(p: CreateSequenceReq) -> Self {
        SequenceMeta {
            comment: p.comment.clone(),
            create_on: p.create_on,
            update_on: p.create_on,
            step: p.increment as i64,
            current: p.start,
            storage_version: p.storage_version,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSequenceReq {
    pub create_option: CreateOption,
    pub ident: SequenceIdent,
    pub create_on: DateTime<Utc>,
    pub start: u64,
    pub increment: u64,
    pub comment: Option<String>,

    /// See: [`SequenceMeta::storage_version`]
    pub storage_version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSequenceReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetSequenceNextValueReq {
    pub ident: SequenceIdent,
    pub count: u64,
}

/// The collection of sequence value in range `[start, end)`, e.g.:
/// `start + i * step` where `start + i + step < end`,
/// or `[start + 0 * step, start + 1 * step, ...)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetSequenceNextValueReply {
    /// The first value in the sequence, inclusive.
    pub start: u64,
    // step has no use until now
    pub step: i64,
    /// The right bound, exclusive.
    pub end: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetSequenceReq {
    pub ident: SequenceIdent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetSequenceReply {
    pub meta: SequenceMeta,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListSequencesReq {
    pub tenant: Tenant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListSequencesReply {
    pub info: Vec<(String, SequenceMeta)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropSequenceReq {
    pub if_exists: bool,
    pub ident: SequenceIdent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropSequenceReply {
    // Whether it is dropped
    pub success: bool,
}

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;

    use super::SequenceMeta;
    use crate::tenant_key::resource::TenantResource;

    pub struct SequenceRsc;
    impl TenantResource for SequenceRsc {
        const PREFIX: &'static str = "__fd_sequence";
        const HAS_TENANT: bool = true;
        type ValueType = SequenceMeta;
    }

    impl kvapi::Value for SequenceMeta {
        type KeyType = super::SequenceIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use crate::schema::SequenceIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_sequence_ident() {
        let tenant = Tenant::new_literal("dummy");
        let ident = SequenceIdent::new_generic(tenant, "3".to_string());

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_sequence/dummy/3");

        assert_eq!(ident, SequenceIdent::from_str_key(&key).unwrap());
    }

    #[test]
    fn test_sequence_ident_with_key_space() {
        // TODO(xp): implement this test
        // let tenant = Tenant::new_literal("test");
        // let ident = IndexIdIdent::new(tenant, 3);
        //
        // let key = ident.to_string_key();
        // assert_eq!(key, "__fd_catalog_by_id/3");
        //
        // assert_eq!(ident, IndexIdIdent::from_str_key(&key).unwrap());
    }
}
