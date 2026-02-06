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
use databend_common_meta_app::data_mask::CreateDatamaskReply;
use databend_common_meta_app::data_mask::CreateDatamaskReq;
use databend_common_meta_app::data_mask::DatamaskMeta;
use databend_common_meta_app::data_mask::DropDatamaskReq;
use databend_common_meta_app::data_mask::data_mask_name_ident::Resource;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_common_meta_store::MetaStore;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;

#[async_trait::async_trait]
pub trait DatamaskHandler: Sync + Send {
    async fn create_data_mask(
        &self,
        meta_api: Arc<MetaStore>,
        req: CreateDatamaskReq,
    ) -> std::result::Result<
        std::result::Result<CreateDatamaskReply, ExistError<Resource>>,
        MetaError,
    >;

    async fn drop_data_mask(
        &self,
        meta_api: Arc<MetaStore>,
        req: DropDatamaskReq,
    ) -> Result<Option<u64>>;

    async fn get_data_mask(
        &self,
        meta_api: Arc<MetaStore>,
        tenant: &Tenant,
        name: String,
    ) -> Result<DatamaskMeta>;

    async fn get_data_mask_by_id(
        &self,
        meta_api: Arc<MetaStore>,
        tenant: &Tenant,
        policy_id: u64,
    ) -> Result<SeqV<DatamaskMeta>>;
}

pub struct DatamaskHandlerWrapper {
    handler: Box<dyn DatamaskHandler>,
}

impl DatamaskHandlerWrapper {
    pub fn new(handler: Box<dyn DatamaskHandler>) -> Self {
        Self { handler }
    }

    pub async fn create_data_mask(
        &self,
        meta_api: Arc<MetaStore>,
        req: CreateDatamaskReq,
    ) -> std::result::Result<
        std::result::Result<CreateDatamaskReply, ExistError<Resource>>,
        MetaError,
    > {
        self.handler.create_data_mask(meta_api, req).await
    }

    pub async fn drop_data_mask(
        &self,
        meta_api: Arc<MetaStore>,
        req: DropDatamaskReq,
    ) -> Result<Option<u64>> {
        self.handler.drop_data_mask(meta_api, req).await
    }

    pub async fn get_data_mask(
        &self,
        meta_api: Arc<MetaStore>,
        tenant: &Tenant,
        name: String,
    ) -> Result<DatamaskMeta> {
        self.handler.get_data_mask(meta_api, tenant, name).await
    }

    pub async fn get_data_mask_by_id(
        &self,
        meta_api: Arc<MetaStore>,
        tenant: &Tenant,
        policy_id: u64,
    ) -> Result<SeqV<DatamaskMeta>> {
        self.handler
            .get_data_mask_by_id(meta_api, tenant, policy_id)
            .await
    }
}

pub fn get_datamask_handler() -> Arc<DatamaskHandlerWrapper> {
    GlobalInstance::get()
}
