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

use std::sync::Arc;

use common_exception::Result;
use common_functions::udfs::UDFFactory;
use common_management::StageMgr;
use common_management::StageMgrApi;
use common_management::UdfMgr;
use common_management::UdfMgrApi;
use common_management::UserMgr;
use common_management::UserMgrApi;
use common_meta_api::KVApi;

use crate::common::MetaClientProvider;
use crate::configs::Config;

pub struct UserApiProvider {
    user_api_provider: Arc<dyn UserMgrApi>,
    stage_api_provider: Arc<dyn StageMgrApi>,
    udf_api_provider: Arc<dyn UdfMgrApi>,
}

impl UserApiProvider {
    async fn create_kv_client(cfg: &Config) -> Result<Arc<dyn KVApi>> {
        match MetaClientProvider::new(cfg.meta.to_flight_client_config())
            .try_get_kv_client()
            .await
        {
            Ok(client) => Ok(client),
            Err(cause) => Err(cause.add_message_back("(while create user api).")),
        }
    }

    pub async fn create_global(cfg: Config) -> Result<Arc<UserApiProvider>> {
        let tenant_id = &cfg.query.tenant_id;
        let client = UserApiProvider::create_kv_client(&cfg).await?;

        Ok(Arc::new(UserApiProvider {
            user_api_provider: Arc::new(UserMgr::new(client.clone(), tenant_id)),
            stage_api_provider: Arc::new(StageMgr::new(client.clone(), tenant_id)),
            udf_api_provider: Arc::new(UdfMgr::new(client, tenant_id)),
        }))
    }

    pub fn get_user_api_client(&self) -> Arc<dyn UserMgrApi> {
        self.user_api_provider.clone()
    }

    pub fn get_stage_api_client(&self) -> Arc<dyn StageMgrApi> {
        self.stage_api_provider.clone()
    }

    pub fn get_udf_api_client(&self) -> Arc<dyn UdfMgrApi> {
        self.udf_api_provider.clone()
    }

    pub async fn load_udfs(&self, cfg: Config) -> Result<()> {
        let udfs = self.get_udf_api_client().get_udfs().await?;

        for udf in udfs.iter() {
            UDFFactory::register(
                &cfg.query.tenant_id,
                udf.name.as_str(),
                udf.definition.as_str(),
            )?;
        }

        Ok(())
    }
}
