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

use databend_common_meta_app::share::*;

use crate::kv_app_error::KVAppError;

#[async_trait::async_trait]
pub trait ShareApi: Sync + Send {
    async fn show_shares(&self, req: ShowSharesReq) -> Result<ShowSharesReply, KVAppError>;
    async fn create_share(&self, req: CreateShareReq) -> Result<CreateShareReply, KVAppError>;

    async fn drop_share(&self, req: DropShareReq) -> Result<DropShareReply, KVAppError>;

    async fn grant_share_object(
        &self,
        req: GrantShareObjectReq,
    ) -> Result<GrantShareObjectReply, KVAppError>;
    async fn revoke_share_object(
        &self,
        req: RevokeShareObjectReq,
    ) -> Result<RevokeShareObjectReply, KVAppError>;

    async fn add_share_tenants(
        &self,
        req: AddShareAccountsReq,
    ) -> Result<AddShareAccountsReply, KVAppError>;
    async fn remove_share_tenants(
        &self,
        req: RemoveShareAccountsReq,
    ) -> Result<RemoveShareAccountsReply, KVAppError>;

    async fn get_share_grant_objects(
        &self,
        req: GetShareGrantObjectReq,
    ) -> Result<GetShareGrantObjectReply, KVAppError>;

    // Return all the grant tenants of the share
    async fn get_grant_tenants_of_share(
        &self,
        req: GetShareGrantTenantsReq,
    ) -> Result<GetShareGrantTenantsReply, KVAppError>;

    // Return all the grant privileges of the object
    async fn get_grant_privileges_of_object(
        &self,
        req: GetObjectGrantPrivilegesReq,
    ) -> Result<GetObjectGrantPrivilegesReply, KVAppError>;

    async fn create_share_endpoint(
        &self,
        req: CreateShareEndpointReq,
    ) -> Result<CreateShareEndpointReply, KVAppError>;

    async fn upsert_share_endpoint(
        &self,
        req: UpsertShareEndpointReq,
    ) -> Result<UpsertShareEndpointReply, KVAppError>;

    async fn get_share_endpoint(
        &self,
        req: GetShareEndpointReq,
    ) -> Result<GetShareEndpointReply, KVAppError>;

    async fn drop_share_endpoint(
        &self,
        req: DropShareEndpointReq,
    ) -> Result<DropShareEndpointReply, KVAppError>;
}
