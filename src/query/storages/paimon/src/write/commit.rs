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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::AsyncSink;
use databend_common_pipeline::sinks::AsyncSinker;
use paimon::table::CommitMessage;

use crate::error::map_paimon_error;
use crate::write::meta::PaimonCommitMeta;

fn merge_route_owners(
    owners: &mut HashMap<Vec<u8>, (String, u64)>,
    incoming: &[(Vec<u8>, String, u64)],
) -> Result<()> {
    for (route, executor, lane) in incoming {
        if let Some((previous_executor, previous_lane)) = owners.get(route) {
            if previous_executor != executor || previous_lane != lane {
                return Err(ErrorCode::Internal(format!(
                    "Paimon route key mapped to multiple writer lanes: executor={previous_executor}/{executor}, lane={previous_lane}/{lane}"
                )));
            }
        } else {
            owners.insert(route.clone(), (executor.clone(), *lane));
        }
    }
    Ok(())
}

/// Coordinator sink that collects writer commit metas and submits one snapshot.
pub struct PaimonCommitSink {
    table: paimon::Table,
    messages: Vec<CommitMessage>,
    route_owners: HashMap<Vec<u8>, (String, u64)>,
}

impl PaimonCommitSink {
    pub fn new(table: paimon::Table) -> Self {
        Self {
            table,
            messages: Vec::new(),
            route_owners: HashMap::new(),
        }
    }

    pub fn try_create(input: Arc<InputPort>, table: paimon::Table) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncSinker::create(
            input,
            Self::new(table),
        )))
    }
}

#[async_trait]
impl AsyncSink for PaimonCommitSink {
    const NAME: &'static str = "PaimonCommitSink";
    // Match Fuse multi_table_insert_commit: on pipeline abort after partial
    // consume, do not call on_finish() — otherwise we would still commit.
    const CALL_ON_FINISH_ON_ERROR: bool = false;

    async fn consume(&mut self, block: DataBlock) -> Result<bool> {
        if let Some(meta) = block.get_meta() {
            let meta = PaimonCommitMeta::downcast_ref_from(meta)
                .ok_or_else(|| ErrorCode::Internal("invalid Paimon commit meta".to_string()))?;
            merge_route_owners(&mut self.route_owners, &meta.route_owners)?;
            self.messages.extend(meta.clone().into_messages()?);
        }
        Ok(false)
    }

    async fn on_finish(&mut self) -> Result<()> {
        if self.messages.is_empty() {
            return Ok(());
        }
        self.table
            .new_write_builder()
            .new_commit()
            .commit(std::mem::take(&mut self.messages))
            .await
            .map_err(map_paimon_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_route_owners_rejects_cross_executor_conflict() {
        let mut owners = HashMap::new();
        merge_route_owners(&mut owners, &[(vec![1], "node-a".to_string(), 1)]).unwrap();
        let err = merge_route_owners(&mut owners, &[(vec![1], "node-b".to_string(), 2)])
            .expect_err("same route on another writer must fail");
        assert!(err.message().contains("multiple writer lanes"));
    }
}
