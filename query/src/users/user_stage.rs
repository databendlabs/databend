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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::UserStageInfo;

use crate::users::UserApiProvider;

/// user stage operations.
impl UserApiProvider {
    // Add a new stage.
    pub async fn add_stage(
        &self,
        tenant: &str,
        info: UserStageInfo,
        if_not_exists: bool,
    ) -> Result<u64> {
        let stage_api_provider = self.get_stage_api_client(tenant)?;
        let add_stage = stage_api_provider.add_stage(info);
        match add_stage.await {
            Ok(res) => Ok(res),
            Err(e) => {
                if if_not_exists && e.code() == ErrorCode::stage_already_exists_code() {
                    Ok(u64::MIN)
                } else {
                    Err(e)
                }
            }
        }
    }

    // Get one stage from by tenant.
    pub async fn get_stage(&self, tenant: &str, stage_name: &str) -> Result<UserStageInfo> {
        let stage_api_provider = self.get_stage_api_client(tenant)?;
        let get_stage = stage_api_provider.get_stage(stage_name, None);
        Ok(get_stage.await?.data)
    }

    // Get the tenant all stage list.
    pub async fn get_stages(&self, tenant: &str) -> Result<Vec<UserStageInfo>> {
        let stage_api_provider = self.get_stage_api_client(tenant)?;
        let get_stages = stage_api_provider.get_stages();

        match get_stages.await {
            Err(e) => Err(e.add_message_back("(while get stages).")),
            Ok(seq_stages_info) => Ok(seq_stages_info),
        }
    }

    // Drop a stage by name.
    pub async fn drop_stage(&self, tenant: &str, name: &str, if_exists: bool) -> Result<()> {
        let stage_api_provider = self.get_stage_api_client(tenant)?;
        let drop_stage = stage_api_provider.drop_stage(name, None);
        match drop_stage.await {
            Ok(res) => Ok(res),
            Err(e) => {
                if if_exists && e.code() == ErrorCode::unknown_stage_code() {
                    Ok(())
                } else {
                    Err(e.add_message_back("(while drop stage)"))
                }
            }
        }
    }
}
