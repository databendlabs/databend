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

pub mod data_mask_name_ident;
pub mod mask_policy_table_id_list_ident;
use std::collections::BTreeSet;
use std::fmt::Display;
use std::fmt::Formatter;

use chrono::DateTime;
use chrono::Utc;
pub use data_mask_name_ident::DataMaskNameIdent;
pub use mask_policy_table_id_list_ident::MaskPolicyTableIdListIdent;

use crate::schema::CreateOption;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DatamaskId {
    pub id: u64,
}

impl Display for DatamaskId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DatamaskMeta {
    // Vec<(arg_name, arg_type)>
    pub args: Vec<(String, String)>,
    pub return_type: String,
    pub body: String,
    pub comment: Option<String>,
    pub create_on: DateTime<Utc>,
    pub update_on: Option<DateTime<Utc>>,
}

impl From<CreateDatamaskReq> for DatamaskMeta {
    fn from(p: CreateDatamaskReq) -> Self {
        DatamaskMeta {
            args: p.args.clone(),
            return_type: p.return_type.clone(),
            body: p.body.clone(),
            comment: p.comment.clone(),
            create_on: p.create_on,
            update_on: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateDatamaskReq {
    pub create_option: CreateOption,
    pub name: DataMaskNameIdent,
    pub args: Vec<(String, String)>,
    pub return_type: String,
    pub body: String,
    pub comment: Option<String>,
    pub create_on: DateTime<Utc>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateDatamaskReply {
    pub id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropDatamaskReq {
    pub if_exists: bool,
    pub name: DataMaskNameIdent,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropDatamaskReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetDatamaskReq {
    pub name: DataMaskNameIdent,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetDatamaskReply {
    pub policy: DatamaskMeta,
}

/// A list of table ids
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, Default, PartialEq)]
pub struct MaskpolicyTableIdList {
    pub id_list: BTreeSet<u64>,
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::KeyBuilder;
    use databend_common_meta_kvapi::kvapi::KeyError;
    use databend_common_meta_kvapi::kvapi::KeyParser;

    use super::DatamaskId;
    use crate::data_mask::DatamaskMeta;

    impl kvapi::KeyCodec for DatamaskId {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_u64(self.id)
        }

        fn decode_key(parser: &mut KeyParser) -> Result<Self, KeyError>
        where Self: Sized {
            let id = parser.next_u64()?;
            Ok(Self { id })
        }
    }

    /// "__fd_datamask_by_id/<id>"
    impl kvapi::Key for DatamaskId {
        const PREFIX: &'static str = "__fd_datamask_by_id";

        type ValueType = DatamaskMeta;

        fn parent(&self) -> Option<String> {
            None
        }
    }

    impl kvapi::Value for DatamaskMeta {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }
}
