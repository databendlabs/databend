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
use crate::tenant::Tenant;
use crate::tenant_key::ident::TIdent;

/// Defines the meta-service key for sequence.
pub type SequenceIdent = TIdent<Resource>;

#[derive(Hash, Clone, Debug, PartialEq, Eq)]
pub struct SequenceNameIdent {
    pub tenant: Tenant,
    pub sequence_name: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
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
    pub name_ident: SequenceNameIdent,
    pub create_on: DateTime<Utc>,
    pub comment: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSequenceReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetSequenceNextValueReq {
    pub name_ident: SequenceNameIdent,
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
    pub name_ident: SequenceNameIdent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetSequenceReply {
    pub meta: SequenceMeta,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropSequenceReq {
    pub if_exists: bool,
    pub name_ident: SequenceNameIdent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropSequenceReply {}

mod kvapi_impl {

    use databend_common_exception::ErrorCode;
    use databend_common_meta_kvapi::kvapi;

    use super::SequenceMeta;
    use crate::tenant_key::errors::ExistError;
    use crate::tenant_key::errors::UnknownError;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_sequence";
        type ValueType = SequenceMeta;
    }

    impl kvapi::Value for SequenceMeta {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }

    impl From<ExistError<Resource>> for ErrorCode {
        fn from(err: ExistError<Resource>) -> Self {
            ErrorCode::ConnectionAlreadyExists(err.to_string())
        }
    }

    impl From<UnknownError<Resource>> for ErrorCode {
        fn from(err: UnknownError<Resource>) -> Self {
            // Special case: use customized message to keep backward compatibility.
            // TODO: consider using the default message in the future(`err.to_string()`)
            ErrorCode::SequenceError(format!("Sequence '{}' does not exist.", err.name()))
                .add_message_back(err.ctx())
        }
    }
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;

    use super::SequenceMeta;
    use super::SequenceNameIdent;
    use crate::tenant::Tenant;

    /// "__fd_sequence/<tenant>/<seq_name>"
    impl kvapi::Key for SequenceNameIdent {
        const PREFIX: &'static str = "__fd_sequence";

        type ValueType = SequenceMeta;

        fn parent(&self) -> Option<String> {
            None
        }

        fn to_string_key(&self) -> String {
            kvapi::KeyBuilder::new_prefixed(Self::PREFIX)
                .push_str(&self.tenant.tenant)
                .push_str(&self.sequence_name)
                .done()
        }

        fn from_str_key(s: &str) -> Result<Self, kvapi::KeyError> {
            let mut p = kvapi::KeyParser::new_prefixed(s, Self::PREFIX)?;

            let tenant = p.next_nonempty()?;
            let sequence_name = p.next_str()?;
            p.done()?;

            let tenant = Tenant::new_nonempty(tenant);

            Ok(Self {
                tenant,
                sequence_name,
            })
        }
    }
}
