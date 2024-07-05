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
use databend_common_exception::Result;
use databend_common_pipeline_core::PlanProfile;

use crate::sessions::SessionManager;

pub static GET_PROFILE: &str = "/actions/get_profile";

pub async fn get_profile(query_id: String) -> Result<Option<Vec<PlanProfile>>> {
    match SessionManager::instance().get_query_profiles(&query_id) {
        Ok(profiles) => Ok(Some(profiles)),
        Err(cause) => match cause.code() == ErrorCode::UNKNOWN_QUERY {
            true => Ok(None),
            false => Err(cause),
        },
    }
}
