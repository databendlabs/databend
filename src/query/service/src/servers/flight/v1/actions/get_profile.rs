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

use databend_common_exception::Result;
use databend_common_pipeline_core::PlanProfile;

use crate::servers::flight::v1::actions::create_session;

pub static GET_PROFILE: &str = "/actions/get_profile";

pub async fn get_profile(query_id: String) -> Result<Option<Vec<PlanProfile>>> {
    let session = create_session()?;
    let query_context = Arc::new(session).create_query_context().await?;
    match query_context.get_session_by_id(&query_id) {
        Some(session) => Ok(session.get_profile()),
        None => Ok(None),
    }
}
