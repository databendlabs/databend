// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_meta_api::KVApi;
use common_meta_raft_store::MetaGrpcGetAction;
use common_meta_raft_store::MetaGrpcWriteAction;
use common_meta_raft_store::RequestFor;
use common_meta_types::protobuf::RaftReply;
use serde::Serialize;

use crate::meta_service::MetaNode;

pub trait ReplySerializer {
    type Output;
    fn serialize<T>(&self, v: T) -> Result<Self::Output, ErrorCode>
    where T: Serialize;
}

pub struct ActionHandler {
    /// The raft-based meta data entry.
    pub(crate) meta_node: Arc<MetaNode>,
}

#[async_trait::async_trait]
pub trait RequestHandler<T>: Sync + Send
where T: RequestFor
{
    async fn handle(&self, req: T) -> common_exception::Result<T::Reply>;
}

impl ActionHandler {
    pub fn create(meta_node: Arc<MetaNode>) -> Self {
        ActionHandler { meta_node }
    }

    pub async fn execute_write(&self, action: MetaGrpcWriteAction) -> RaftReply {
        // To keep the code IDE-friendly, we manually expand the enum variants and dispatch them one by one

        match action {
            MetaGrpcWriteAction::UpsertKV(a) => {
                let r = self.meta_node.upsert_kv(a).await.map_err(|e| e.message());
                RaftReply::from(r)
            }
            // database
            MetaGrpcWriteAction::CreateDatabase(a) => {
                let r = self.handle(a).await.map_err(|e| e.message());
                RaftReply::from(r)
            }
            MetaGrpcWriteAction::DropDatabase(a) => {
                let r = self.handle(a).await.map_err(|e| e.message());
                RaftReply::from(r)
            }

            // table
            MetaGrpcWriteAction::CreateTable(a) => {
                let r = self.handle(a).await.map_err(|e| e.message());
                RaftReply::from(r)
            }
            MetaGrpcWriteAction::DropTable(a) => {
                let r = self.handle(a).await.map_err(|e| e.message());
                RaftReply::from(r)
            }
            MetaGrpcWriteAction::CommitTable(a) => {
                let r = self.handle(a).await.map_err(|e| e.message());
                RaftReply::from(r)
            }
        }
    }

    pub async fn execute_read<S, R>(
        &self,
        action: MetaGrpcGetAction,
        s: S,
    ) -> common_exception::Result<R>
    where
        S: ReplySerializer<Output = R>,
    {
        // To keep the code IDE-friendly, we manually expand the enum variants and dispatch them one by one

        match action {
            MetaGrpcGetAction::GetKV(a) => s.serialize(self.meta_node.get_kv(&a.key).await?),
            MetaGrpcGetAction::MGetKV(a) => s.serialize(self.meta_node.mget_kv(&a.keys).await?),
            MetaGrpcGetAction::PrefixListKV(a) => {
                s.serialize(self.meta_node.prefix_list_kv(&a.0).await?)
            }

            // database
            MetaGrpcGetAction::GetDatabase(a) => s.serialize(self.handle(a).await?),
            MetaGrpcGetAction::ListDatabases(a) => s.serialize(self.handle(a).await?),

            // table
            MetaGrpcGetAction::GetTable(a) => s.serialize(self.handle(a).await?),
            MetaGrpcGetAction::ListTables(a) => s.serialize(self.handle(a).await?),
            MetaGrpcGetAction::GetTableExt(a) => s.serialize(self.handle(a).await?),
        }
    }
}
