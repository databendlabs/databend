// Copyright 2020 Datafuse Labs.
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
use common_meta_flight::MetaFlightAction;
use common_meta_flight::RequestFor;
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

    pub async fn execute<S, R>(
        &self,
        action: MetaFlightAction,
        s: S,
    ) -> common_exception::Result<R>
    where
        S: ReplySerializer<Output = R>,
    {
        // To keep the code IDE-friendly, we manually expand the enum variants and dispatch them one by one

        match action {
            MetaFlightAction::UpsertKV(a) => s.serialize(self.meta_node.upsert_kv(a).await?),
            MetaFlightAction::GetKV(a) => s.serialize(self.meta_node.get_kv(&a.key).await?),
            MetaFlightAction::MGetKV(a) => s.serialize(self.meta_node.mget_kv(&a.keys).await?),
            MetaFlightAction::PrefixListKV(a) => {
                s.serialize(self.meta_node.prefix_list_kv(&a.0).await?)
            }

            // database
            MetaFlightAction::CreateDatabase(a) => s.serialize(self.handle(a).await?),
            MetaFlightAction::GetDatabase(a) => s.serialize(self.handle(a).await?),
            MetaFlightAction::DropDatabase(a) => s.serialize(self.handle(a).await?),
            MetaFlightAction::GetDatabases(a) => s.serialize(self.handle(a).await?),

            // table
            MetaFlightAction::CreateTable(a) => s.serialize(self.handle(a).await?),
            MetaFlightAction::DropTable(a) => s.serialize(self.handle(a).await?),
            MetaFlightAction::GetTable(a) => s.serialize(self.handle(a).await?),
            MetaFlightAction::GetTables(a) => s.serialize(self.handle(a).await?),
            MetaFlightAction::GetTableExt(a) => s.serialize(self.handle(a).await?),
            MetaFlightAction::CommitTable(a) => s.serialize(self.handle(a).await?),
        }
    }
}
