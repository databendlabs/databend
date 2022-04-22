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

use std::env;
use std::sync::Arc;

use common_base::tokio::runtime::Runtime;
use common_base::Thread;
use common_exception::Result;
use databend_query::configs::Config;
use databend_query::sessions::SessionManager;

async fn async_create_sessions(config: Config) -> Result<Arc<SessionManager>> {
    let sessions = SessionManager::from_conf(config.clone()).await?;

    let cluster_discovery = sessions.get_cluster_discovery();
    cluster_discovery.register_to_metastore(&config).await?;
    Ok(sessions)
}

fn sync_create_sessions(config: Config) -> Result<Arc<SessionManager>> {
    let runtime = Runtime::new()?;
    runtime.block_on(async_create_sessions(config))
}

pub struct SessionManagerBuilder {
    config: Config,
}

impl SessionManagerBuilder {
    pub fn create() -> SessionManagerBuilder {
        let mut conf = crate::tests::ConfigBuilder::create().config();
        if conf.query.num_cpus == 0 {
            conf.query.num_cpus = num_cpus::get() as u64;
        }
        SessionManagerBuilder::create_with_conf(conf).log_dir_with_relative("../tests/data/logs")
    }

    pub fn create_with_conf(config: Config) -> SessionManagerBuilder {
        SessionManagerBuilder { config }
    }

    pub fn log_dir_with_relative(self, path: impl Into<String>) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.log.log_dir = env::current_dir()
            .unwrap()
            .join(path.into())
            .display()
            .to_string();

        SessionManagerBuilder::create_with_conf(new_config)
    }

    pub fn build(self) -> Result<Arc<SessionManager>> {
        let config = self.config;
        let handle = Thread::spawn(move || sync_create_sessions(config));
        handle.join().unwrap()
    }
}
