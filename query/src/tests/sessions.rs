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

pub fn try_create_sessions() -> Result<SessionManagerRef> {
    let mut config = Config::default();
    let cluster = Cluster::empty();

    // Setup log dir to the tests directory.
    config.log.log_dir = env::current_dir()?
        .join("../tests/data/logs")
        .display()
        .to_string();

    SessionManager::from_conf(config, cluster)
}
