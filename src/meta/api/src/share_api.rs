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

use common_meta_app::share::*;
use common_meta_types::MetaResult;

#[async_trait::async_trait]
pub trait ShareApi: Sync + Send {
    async fn show_shares(&self, req: ShowSharesReq) -> MetaResult<ShowSharesReply>;
    async fn create_share(&self, req: CreateShareReq) -> MetaResult<CreateShareReply>;

    async fn drop_share(&self, req: DropShareReq) -> MetaResult<DropShareReply>;

    async fn grant_share_object(
        &self,
        req: GrantShareObjectReq,
    ) -> MetaResult<GrantShareObjectReply>;
    async fn revoke_share_object(
        &self,
        req: RevokeShareObjectReq,
    ) -> MetaResult<RevokeShareObjectReply>;

    async fn add_share_tenants(
        &self,
        req: AddShareAccountsReq,
    ) -> MetaResult<AddShareAccountsReply>;
    async fn remove_share_tenants(
        &self,
        req: RemoveShareAccountsReq,
    ) -> MetaResult<RemoveShareAccountsReply>;

    async fn get_share_grant_objects(
        &self,
        req: GetShareGrantObjectReq,
    ) -> MetaResult<GetShareGrantObjectReply>;

    // Return all the grant tenants of the share
    async fn get_grant_tenants_of_share(
        &self,
        req: GetShareGrantTenantsReq,
    ) -> MetaResult<GetShareGrantTenantsReply>;

    // Return all the grant privileges of the object
    async fn get_grant_privileges_of_object(
        &self,
        req: GetObjectGrantPrivilegesReq,
    ) -> MetaResult<GetObjectGrantPrivilegesReply>;
}
