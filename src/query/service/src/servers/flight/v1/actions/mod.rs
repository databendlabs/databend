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

mod flight_actions;
mod get_profile;
mod init_query_env;
mod init_query_fragments;
mod kill_query;
mod set_priority;
mod start_prepared_query;
mod system_action;
mod truncate_table;

use std::sync::Arc;

use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_settings::Settings;
pub use flight_actions::flight_actions;
pub use flight_actions::FlightActions;
pub use get_profile::GET_PROFILE;
pub use init_query_env::INIT_QUERY_ENV;
pub use init_query_fragments::init_query_fragments;
pub use init_query_fragments::INIT_QUERY_FRAGMENTS;
pub use kill_query::KILL_QUERY;
pub use set_priority::SET_PRIORITY;
pub use start_prepared_query::START_PREPARED_QUERY;
pub use system_action::SYSTEM_ACTION;
pub use truncate_table::TRUNCATE_TABLE;

use crate::sessions::Session;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;

pub(crate) fn create_session() -> Result<Arc<Session>> {
    let config = GlobalConfig::instance();
    let settings = Settings::create(config.query.tenant_id.clone());
    match SessionManager::instance().create_with_settings(SessionType::FlightRPC, settings) {
        Err(cause) => Err(cause),
        Ok(session) => Ok(Arc::new(session)),
    }
}
