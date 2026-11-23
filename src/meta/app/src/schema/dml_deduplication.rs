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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_meta_client::kvapi;

use super::UpdateStreamMetaReq;
use crate::principal::UserIdentity;
use crate::tenant::Tenant;

/// An opaque, nonempty client token. This is not a Meta key.
#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "String")]
pub struct DmlDeduplicationToken(String);

impl DmlDeduplicationToken {
    pub const MAX_BYTES: usize = 256;

    /// The empty setting means the client did not request deduplication.
    pub fn from_setting(value: String) -> Result<Option<Self>> {
        if value.is_empty() {
            return Ok(None);
        }
        Self::try_from(value).map(Some)
    }

    pub fn validate_setting(value: &str) -> Result<()> {
        if value.len() > Self::MAX_BYTES {
            return Err(ErrorCode::WrongValueForVariable(format!(
                "deduplicate_label must be at most {} bytes",
                Self::MAX_BYTES
            )));
        }
        Ok(())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for DmlDeduplicationToken {
    type Error = ErrorCode;

    fn try_from(value: String) -> Result<Self> {
        Self::validate_setting(&value)?;
        if value.is_empty() {
            return Err(ErrorCode::WrongValueForVariable(
                "A deduplication token cannot be empty",
            ));
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DmlDeduplicationOperation {
    Insert,
    InsertOverwrite,
    Replace,
    Update,
    Delete,
    Merge,
    CopyIntoTable,
}

impl DmlDeduplicationOperation {
    fn key_segment(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::InsertOverwrite => "insert_overwrite",
            Self::Replace => "replace",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Merge => "merge",
            Self::CopyIntoTable => "copy_into_table",
        }
    }
}

/// A retry token bound to the authenticated account and the resolved write.
/// The enclosing Meta API call or transaction supplies the tenant.
#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DmlDeduplicationId {
    pub principal: UserIdentity,
    pub catalog_id: u64,
    pub table_id: u64,
    pub operation: DmlDeduplicationOperation,
    pub token: DmlDeduplicationToken,
}

impl DmlDeduplicationId {
    pub fn key(&self, tenant: &Tenant) -> DmlDeduplicationKey {
        DmlDeduplicationKey {
            tenant: tenant.tenant_name().to_owned(),
            username: self.principal.username.clone(),
            hostname: self.principal.hostname.clone(),
            catalog_id: self.catalog_id,
            table_id: self.table_id,
            operation: self.operation.key_segment().to_owned(),
            token: self.token.as_str().to_owned(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, kvapi::StructKey)]
#[structkey(prefix = "__fd_dml_deduplication_v1")]
pub struct DmlDeduplicationKey {
    tenant: String,
    username: String,
    hostname: String,
    catalog_id: u64,
    table_id: u64,
    operation: String,
    token: String,
}

impl kvapi::Key for DmlDeduplicationKey {
    type ValueType = Vec<u8>;
}

/// The durable metadata updated by the statement that supplied the token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DmlDeduplicationEffect {
    Table(u64),
    Streams(Vec<u64>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DmlDeduplicationMarker {
    pub id: DmlDeduplicationId,
    pub effect: DmlDeduplicationEffect,
}

impl DmlDeduplicationMarker {
    pub fn for_stream_updates(id: DmlDeduplicationId, updates: &[UpdateStreamMetaReq]) -> Self {
        Self {
            id,
            effect: DmlDeduplicationEffect::Streams(
                updates.iter().map(|req| req.stream_id).collect(),
            ),
        }
    }
}
