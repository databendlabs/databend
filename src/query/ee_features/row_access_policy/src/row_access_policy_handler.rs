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

use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_exception::Result;
use databend_common_meta_app::row_access_policy::CreateRowAccessPolicyReply;
use databend_common_meta_app::row_access_policy::CreateRowAccessPolicyReq;
use databend_common_meta_app::row_access_policy::DropRowAccessPolicyReq;
use databend_common_meta_app::row_access_policy::RowAccessPolicyId;
use databend_common_meta_app::row_access_policy::RowAccessPolicyMeta;
use databend_common_meta_app::row_access_policy::row_access_policy_name_ident::Resource;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_common_meta_store::MetaStore;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;

#[async_trait::async_trait]
pub trait RowAccessPolicyHandler: Sync + Send {
    async fn create_row_access_policy(
        &self,
        meta_api: Arc<MetaStore>,
        req: CreateRowAccessPolicyReq,
    ) -> std::result::Result<
        std::result::Result<CreateRowAccessPolicyReply, ExistError<Resource>>,
        MetaError,
    >;

    async fn drop_row_access_policy(
        &self,
        meta_api: Arc<MetaStore>,
        req: DropRowAccessPolicyReq,
    ) -> Result<()>;

    async fn get_row_access_policy(
        &self,
        meta_api: Arc<MetaStore>,
        tenant: &Tenant,
        name: String,
    ) -> Result<(SeqV<RowAccessPolicyId>, SeqV<RowAccessPolicyMeta>)>;

    async fn get_row_access_policy_by_id(
        &self,
        meta_api: Arc<MetaStore>,
        tenant: &Tenant,
        policy_id: u64,
    ) -> Result<SeqV<RowAccessPolicyMeta>>;
}

pub struct RowAccessPolicyHandlerWrapper {
    handler: Box<dyn RowAccessPolicyHandler>,
}

impl RowAccessPolicyHandlerWrapper {
    pub fn new(handler: Box<dyn RowAccessPolicyHandler>) -> Self {
        Self { handler }
    }

    pub async fn create_row_access_policy(
        &self,
        meta_api: Arc<MetaStore>,
        req: CreateRowAccessPolicyReq,
    ) -> std::result::Result<
        std::result::Result<CreateRowAccessPolicyReply, ExistError<Resource>>,
        MetaError,
    > {
        self.handler.create_row_access_policy(meta_api, req).await
    }

    pub async fn drop_row_access_policy(
        &self,
        meta_api: Arc<MetaStore>,
        req: DropRowAccessPolicyReq,
    ) -> Result<()> {
        self.handler.drop_row_access_policy(meta_api, req).await
    }

    pub async fn get_row_access_policy(
        &self,
        meta_api: Arc<MetaStore>,
        tenant: &Tenant,
        name: String,
    ) -> Result<(SeqV<RowAccessPolicyId>, SeqV<RowAccessPolicyMeta>)> {
        self.handler
            .get_row_access_policy(meta_api, tenant, name)
            .await
    }

    pub async fn get_row_access_policy_by_id(
        &self,
        meta_api: Arc<MetaStore>,
        tenant: &Tenant,
        policy_id: u64,
    ) -> Result<SeqV<RowAccessPolicyMeta>> {
        self.handler
            .get_row_access_policy_by_id(meta_api, tenant, policy_id)
            .await
    }
}

pub fn get_row_access_policy_handler() -> Arc<RowAccessPolicyHandlerWrapper> {
    GlobalInstance::get()
}
