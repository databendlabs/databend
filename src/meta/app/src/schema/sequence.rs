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

use chrono::DateTime;
use chrono::Utc;
use kvapi_impl::Resource;

use super::CreateOption;
use crate::tenant_key::ident::TIdent;

/// Defines the meta-service key for sequence.
pub type SequenceIdent = TIdent<Resource>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequenceMeta {
    pub create_on: DateTime<Utc>,
    pub update_on: DateTime<Utc>,
    pub comment: Option<String>,
    pub start: u64,
    pub step: i64,
    pub current: u64,
}

impl From<CreateSequenceReq> for SequenceMeta {
    fn from(p: CreateSequenceReq) -> Self {
        SequenceMeta {
            comment: p.comment.clone(),
            create_on: p.create_on,
            update_on: p.create_on,
            start: 1,
            step: 1,
            current: 1,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSequenceReq {
    pub create_option: CreateOption,
    pub ident: SequenceIdent,
    pub create_on: DateTime<Utc>,
    pub comment: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSequenceReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetSequenceNextValueReq {
    pub ident: SequenceIdent,
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetSequenceNextValueReply {
    pub start: u64,
    // step has no use until now
    pub step: i64,
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
pub struct DropSequenceReq {
    pub if_exists: bool,
    pub ident: SequenceIdent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropSequenceReply {
    // return prev seq if drop success
    pub prev: Option<u64>,
}

mod kvapi_impl {

    use databend_common_meta_kvapi::kvapi;

    use super::SequenceMeta;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_sequence";
        const HAS_TENANT: bool = true;
        type ValueType = SequenceMeta;
    }

    impl kvapi::Value for SequenceMeta {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }
}
