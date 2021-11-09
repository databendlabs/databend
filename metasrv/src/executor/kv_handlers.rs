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
//

use common_meta_api::KVApi;
use common_meta_flight::GetKVAction;
use common_meta_flight::MGetKVAction;
use common_meta_flight::PrefixListReq;
use common_meta_types::GetKVActionReply;
use common_meta_types::MGetKVActionReply;
use common_meta_types::PrefixListReply;
use common_meta_types::UpsertKVAction;
use common_meta_types::UpsertKVActionReply;

use crate::executor::action_handler::RequestHandler;
use crate::executor::ActionHandler;

#[async_trait::async_trait]
impl RequestHandler<UpsertKVAction> for ActionHandler {
    async fn handle(&self, act: UpsertKVAction) -> common_exception::Result<UpsertKVActionReply> {
        self.meta_node.upsert_kv(act).await
    }
}

#[async_trait::async_trait]
impl RequestHandler<GetKVAction> for ActionHandler {
    async fn handle(&self, act: GetKVAction) -> common_exception::Result<GetKVActionReply> {
        self.meta_node.get_kv(&act.key).await
    }
}

#[async_trait::async_trait]
impl RequestHandler<MGetKVAction> for ActionHandler {
    async fn handle(&self, act: MGetKVAction) -> common_exception::Result<MGetKVActionReply> {
        self.meta_node.mget_kv(&act.keys).await
    }
}

#[async_trait::async_trait]
impl RequestHandler<PrefixListReq> for ActionHandler {
    async fn handle(&self, act: PrefixListReq) -> common_exception::Result<PrefixListReply> {
        self.meta_node.prefix_list_kv(&(act.0)).await
    }
}
