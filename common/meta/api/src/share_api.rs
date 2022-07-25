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

use common_meta_app::share::AddShareAccountReply;
use common_meta_app::share::AddShareAccountReq;
use common_meta_app::share::CreateShareReply;
use common_meta_app::share::CreateShareReq;
use common_meta_app::share::DropShareReply;
use common_meta_app::share::DropShareReq;
use common_meta_app::share::GetShareGrantObjectReply;
use common_meta_app::share::GetShareGrantObjectReq;
use common_meta_app::share::GrantShareObjectReply;
use common_meta_app::share::GrantShareObjectReq;
use common_meta_app::share::RemoveShareAccountReply;
use common_meta_app::share::RemoveShareAccountReq;
use common_meta_app::share::RevokeShareObjectReply;
use common_meta_app::share::RevokeShareObjectReq;
use common_meta_types::MetaResult;

#[async_trait::async_trait]
pub trait ShareApi: Sync + Send {
    async fn create_share(&self, req: CreateShareReq) -> MetaResult<CreateShareReply>;

    async fn drop_share(&self, req: DropShareReq) -> MetaResult<DropShareReply>;

    async fn grant_object(&self, req: GrantShareObjectReq) -> MetaResult<GrantShareObjectReply>;
    async fn revoke_object(&self, req: RevokeShareObjectReq) -> MetaResult<RevokeShareObjectReply>;

    async fn add_share_account(&self, req: AddShareAccountReq) -> MetaResult<AddShareAccountReply>;
    async fn remove_share_account(
        &self,
        req: RemoveShareAccountReq,
    ) -> MetaResult<RemoveShareAccountReply>;

    async fn get_share_grant_objects(
        &self,
        req: GetShareGrantObjectReq,
    ) -> MetaResult<GetShareGrantObjectReply>;
}
