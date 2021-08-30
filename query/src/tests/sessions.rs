// Copyright 2020 Datafuse Labs.
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

use std::env;

use common_exception::Result;

use crate::clusters::Cluster;
use crate::configs::Config;
use crate::sessions::SessionManager;
use crate::sessions::SessionManagerRef;

pub fn try_create_session_mgr(max_active_sessions: Option<u64>) -> Result<SessionManagerRef> {
    let mut conf = Config::default();
    // Setup log dir to the tests directory.
    conf.log.log_dir = env::current_dir()?
        .join("../tests/data/logs")
        .display()
        .to_string();
    // Set max active session number if have.
    if let Some(max) = max_active_sessions {
        conf.query.max_active_sessions = max;
    }

    SessionManager::from_conf(conf, Cluster::empty())
}
