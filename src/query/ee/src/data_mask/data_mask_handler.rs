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
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::data_mask::CreateDatamaskReply;
use databend_common_meta_app::data_mask::CreateDatamaskReq;
use databend_common_meta_app::data_mask::DataMaskNameIdent;
use databend_common_meta_app::data_mask::DatamaskMeta;
use databend_common_meta_app::data_mask::DropDatamaskReq;
use databend_common_meta_app::data_mask::data_mask_name_ident::Resource;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_common_meta_store::MetaStore;
use databend_enterprise_data_mask_feature::data_mask_handler::DatamaskHandler;
use databend_enterprise_data_mask_feature::data_mask_handler::DatamaskHandlerWrapper;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;

use crate::meta_service_error;

pub struct RealDatamaskHandler {}

#[async_trait::async_trait]
impl DatamaskHandler for RealDatamaskHandler {
    async fn create_data_mask(
        &self,
        meta_api: Arc<MetaStore>,
        req: CreateDatamaskReq,
    ) -> std::result::Result<
        std::result::Result<CreateDatamaskReply, ExistError<Resource>>,
        MetaError,
    > {
        meta_api.create_data_mask(req).await
    }

    async fn drop_data_mask(
        &self,
        meta_api: Arc<MetaStore>,
        req: DropDatamaskReq,
    ) -> Result<Option<u64>> {
        let dropped = meta_api.drop_data_mask(&req.name).await??;
        match dropped {
            Some((seq_id, _)) => Ok(Some(*seq_id.data)),
            None => {
                if req.if_exists {
                    Ok(None)
                } else {
                    Err(AppError::from(req.name.unknown_error("drop data mask")).into())
                }
            }
        }
    }

    async fn get_data_mask(
        &self,
        meta_api: Arc<MetaStore>,
        tenant: &Tenant,
        name: String,
    ) -> Result<DatamaskMeta> {
        let name_ident = DataMaskNameIdent::new(tenant, name);
        let seq_meta = meta_api
            .get_data_mask(&name_ident)
            .await
            .map_err(meta_service_error)?
            .ok_or_else(|| AppError::from(name_ident.unknown_error("get data mask")))?;
        Ok(seq_meta.data)
    }

    async fn get_data_mask_by_id(
        &self,
        meta_api: Arc<MetaStore>,
        tenant: &Tenant,
        policy_id: u64,
    ) -> Result<SeqV<DatamaskMeta>> {
        let res = meta_api
            .get_data_mask_by_id(tenant, policy_id)
            .await
            .map_err(meta_service_error)?
            .ok_or_else(|| {
                databend_common_exception::ErrorCode::UnknownMaskPolicy(format!(
                    "Unknown mask policy {}",
                    policy_id
                ))
            })?;
        Ok(res)
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
