// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_exception::Result;
use databend_common_meta_api::DatamaskApi;
use databend_common_meta_app::data_mask::CreateDatamaskReq;
use databend_common_meta_app::data_mask::DataMaskNameIdent;
use databend_common_meta_app::data_mask::DatamaskMeta;
use databend_common_meta_app::data_mask::DropDatamaskReq;
use databend_common_meta_app::data_mask::GetDatamaskReq;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_enterprise_data_mask_feature::data_mask_handler::DatamaskHandler;
use databend_enterprise_data_mask_feature::data_mask_handler::DatamaskHandlerWrapper;

pub struct RealDatamaskHandler {}

#[async_trait::async_trait]
impl DatamaskHandler for RealDatamaskHandler {
    async fn create_data_mask(
        &self,
        meta_api: Arc<MetaStore>,
        req: CreateDatamaskReq,
    ) -> Result<()> {
        let _ = meta_api.create_data_mask(req).await?;

        Ok(())
    }

    async fn drop_data_mask(&self, meta_api: Arc<MetaStore>, req: DropDatamaskReq) -> Result<()> {
        let _ = meta_api.drop_data_mask(req).await?;

        Ok(())
    }

    async fn get_data_mask(
        &self,
        meta_api: Arc<MetaStore>,
        tenant: &Tenant,
        name: String,
    ) -> Result<DatamaskMeta> {
        let resp = meta_api
            .get_data_mask(GetDatamaskReq {
                name: DataMaskNameIdent::new(tenant, name),
            })
            .await?;
        Ok(resp.policy.data)
    }
}

impl RealDatamaskHandler {
    pub fn init() -> Result<()> {
        let rm = RealDatamaskHandler {};
        let wrapper = DatamaskHandlerWrapper::new(Box::new(rm));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }
}
