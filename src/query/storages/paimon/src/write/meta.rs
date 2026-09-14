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
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use paimon::spec::DataFileMeta;
use paimon::spec::IndexFileMeta;
use paimon::table::CommitMessage;
use serde::Deserialize;
use serde::Serialize;

/// Serializable commit metadata that flows across Exchange as `BlockMetaInfo`.
///
/// Each `CommitMessage` is stored as a JSON string. `paimon` 0.2.0's `CommitMessage`
/// does not implement `Serialize`/`Deserialize`, but all nested public types
/// (`DataFileMeta`, `IndexFileMeta`) do — so we encode via a field-complete wire
/// shape that reuses those types (no Databend-local `DataFileMeta` mirror).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PaimonCommitMeta {
    pub messages_json: Vec<String>,
    #[serde(default)]
    pub route_owners: Vec<(Vec<u8>, String, u64)>,
}

#[typetag::serde(name = "paimon_commit_meta")]
impl BlockMetaInfo for PaimonCommitMeta {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        PaimonCommitMeta::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl PaimonCommitMeta {
    pub fn try_from_messages(messages: Vec<CommitMessage>) -> Result<Self> {
        let mut messages_json = Vec::with_capacity(messages.len());
        for message in messages {
            let json =
                serde_json::to_string(&CommitMessageWire::from(&message)).map_err(|err| {
                    ErrorCode::Internal(format!("Failed to serialize Paimon CommitMessage: {err}"))
                })?;
            messages_json.push(json);
        }
        Ok(Self {
            messages_json,
            route_owners: Vec::new(),
        })
    }

    pub fn into_messages(self) -> Result<Vec<CommitMessage>> {
        let mut messages = Vec::with_capacity(self.messages_json.len());
        for json in self.messages_json {
            let wire: CommitMessageWire = serde_json::from_str(&json).map_err(|err| {
                ErrorCode::Internal(format!("Failed to deserialize Paimon CommitMessage: {err}"))
            })?;
            messages.push(wire.into_commit_message());
        }
        Ok(messages)
    }
}

/// Field-complete wire encoding for one `CommitMessage`.
///
/// Reuses paimon `DataFileMeta` / `IndexFileMeta` serde; does not drop fields and
/// does not redefine nested file metadata in Databend.
#[derive(Serialize, Deserialize)]
struct CommitMessageWire {
    partition: Vec<u8>,
    bucket: i32,
    new_files: Vec<DataFileMeta>,
    new_index_files: Vec<IndexFileMeta>,
    deleted_files: Vec<DataFileMeta>,
}

impl From<&CommitMessage> for CommitMessageWire {
    fn from(message: &CommitMessage) -> Self {
        Self {
            partition: message.partition.clone(),
            bucket: message.bucket,
            new_files: message.new_files.clone(),
            new_index_files: message.new_index_files.clone(),
            deleted_files: message.deleted_files.clone(),
        }
    }
}

impl CommitMessageWire {
    fn into_commit_message(self) -> CommitMessage {
        CommitMessage {
            partition: self.partition,
            bucket: self.bucket,
            new_files: self.new_files,
            new_index_files: self.new_index_files,
            deleted_files: self.deleted_files,
        }
    }
}
