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

use databend_common_exception::ErrorCode;
use databend_common_exception::ErrorCodeResultExt;
use databend_common_exception::Result;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;

use crate::UserApiProvider;

/// user stage operations.
impl UserApiProvider {
    // Add a new stage.
    #[async_backtrace::framed]
    pub async fn add_stage(
        &self,
        tenant: &Tenant,
        info: StageInfo,
        create_option: &CreateOption,
    ) -> Result<()> {
        let stage_api_provider = self.stage_api(tenant);
        stage_api_provider.add_stage(info, create_option).await
    }

    // Get one stage from by tenant.
    #[async_backtrace::framed]
    pub async fn get_stage(&self, tenant: &Tenant, stage_name: &str) -> Result<StageInfo> {
        let stage_api_provider = self.stage_api(tenant);
        stage_api_provider.get_stage(stage_name).await
    }

    #[async_backtrace::framed]
    pub async fn exists_stage(&self, tenant: &Tenant, stage_name: &str) -> Result<bool> {
        Ok(self
            .get_stage(tenant, stage_name)
            .await
            .or_unknown_stage()?
            .is_some())
    }

    // Get the tenant all stage list.
    #[async_backtrace::framed]
    pub async fn get_stages(&self, tenant: &Tenant) -> Result<Vec<StageInfo>> {
        let stage_api_provider = self.stage_api(tenant);
        let get_stages = stage_api_provider.get_stages();

        match get_stages.await {
            Err(e) => Err(e.add_message_back(" (while get stages)")),
            Ok(seq_stages_info) => Ok(seq_stages_info),
        }
    }

    // Drop a stage by name.
    #[async_backtrace::framed]
    pub async fn drop_stage(&self, tenant: &Tenant, name: &str, if_exists: bool) -> Result<()> {
        let stage_api_provider = self.stage_api(tenant);
        let drop_stage = stage_api_provider.drop_stage(name);
        match drop_stage.await {
            Ok(res) => Ok(res),
            Err(e) => {
                if if_exists && e.code() == ErrorCode::UNKNOWN_STAGE {
                    Ok(())
                } else {
                    Err(e.add_message_back(" (while drop stage)"))
                }
            }
        }
    }
}
