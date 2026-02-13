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
use databend_common_meta_api::RowAccessPolicyApi;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::row_access_policy::CreateRowAccessPolicyReply;
use databend_common_meta_app::row_access_policy::CreateRowAccessPolicyReq;
use databend_common_meta_app::row_access_policy::DropRowAccessPolicyReq;
use databend_common_meta_app::row_access_policy::RowAccessPolicyId;
use databend_common_meta_app::row_access_policy::RowAccessPolicyMeta;
use databend_common_meta_app::row_access_policy::RowAccessPolicyNameIdent;
use databend_common_meta_app::row_access_policy::row_access_policy_name_ident::Resource;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_common_meta_store::MetaStore;
use databend_enterprise_row_access_policy_feature::row_access_policy_handler::RowAccessPolicyHandler;
use databend_enterprise_row_access_policy_feature::row_access_policy_handler::RowAccessPolicyHandlerWrapper;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;

use crate::meta_service_error;

pub struct RealRowAccessPolicyHandler {}

#[async_trait::async_trait]
impl RowAccessPolicyHandler for RealRowAccessPolicyHandler {
    async fn create_row_access_policy(
        &self,
        meta_api: Arc<MetaStore>,
        req: CreateRowAccessPolicyReq,
    ) -> std::result::Result<
        std::result::Result<CreateRowAccessPolicyReply, ExistError<Resource>>,
        MetaError,
    > {
        meta_api.create_row_access_policy(req).await
    }

    async fn drop_row_access_policy(
        &self,
        meta_api: Arc<MetaStore>,
        req: DropRowAccessPolicyReq,
    ) -> Result<()> {
        let dropped = meta_api.drop_row_access_policy(&req.name).await??;
        if dropped.is_none() {
            return Err(AppError::from(req.name.unknown_error("drop row policy")).into());
        }

        Ok(())
    }

    async fn get_row_access_policy(
        &self,
        meta_api: Arc<MetaStore>,
        tenant: &Tenant,
        name: String,
    ) -> Result<(SeqV<RowAccessPolicyId>, SeqV<RowAccessPolicyMeta>)> {
        let name_ident = RowAccessPolicyNameIdent::new(tenant, name);
        let res = meta_api
            .get_row_access_policy(&name_ident)
            .await
            .map_err(meta_service_error)?
            .ok_or_else(|| AppError::from(name_ident.unknown_error("get row policy")))?;
        Ok(res)
    }

    async fn get_row_access_policy_by_id(
        &self,
        meta_api: Arc<MetaStore>,
        tenant: &Tenant,
        policy_id: u64,
    ) -> Result<SeqV<RowAccessPolicyMeta>> {
        let res = meta_api
            .get_row_access_policy_by_id(tenant, policy_id)
            .await
            .map_err(meta_service_error)?
            .ok_or_else(|| {
                databend_common_exception::ErrorCode::UnknownRowAccessPolicy(format!(
                    "Unknown row access policy {}",
                    policy_id
                ))
            })?;
        Ok(res)
    }
}

impl RealRowAccessPolicyHandler {
    pub fn init() -> Result<()> {
        let rm = RealRowAccessPolicyHandler {};
        let wrapper = RowAccessPolicyHandlerWrapper::new(Box::new(rm));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }
}
