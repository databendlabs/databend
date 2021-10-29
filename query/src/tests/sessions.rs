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

use common_base::tokio::runtime::Runtime;
use common_base::Thread;
use common_exception::Result;

use crate::configs::Config;
use crate::sessions::SessionManager;
use crate::sessions::SessionManagerRef;

async fn async_try_create_sessions(config: Config) -> Result<SessionManagerRef> {
    let sessions = SessionManager::from_conf(config.clone()).await?;

    let cluster_discovery = sessions.get_cluster_discovery();
    cluster_discovery.register_to_metastore(&config).await?;
    Ok(sessions)
}

fn sync_try_create_sessions(config: Config) -> Result<SessionManagerRef> {
    let runtime = Runtime::new()?;
    runtime.block_on(async_try_create_sessions(config))
}

pub struct SessionManagerBuilder {
    config: Config,
}

impl SessionManagerBuilder {
    pub fn create() -> SessionManagerBuilder {
        SessionManagerBuilder::inner_create(Config::default())
            .log_dir_with_relative("../tests/data/logs")
    }

    fn inner_create(config: Config) -> SessionManagerBuilder {
        SessionManagerBuilder { config }
    }

    pub fn max_sessions(self, max_sessions: u64) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.query.max_active_sessions = max_sessions;
        SessionManagerBuilder::inner_create(new_config)
    }

    pub fn rpc_tls_server_key(self, value: impl Into<String>) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.query.rpc_tls_server_key = value.into();
        SessionManagerBuilder::inner_create(new_config)
    }

    pub fn rpc_tls_server_cert(self, value: impl Into<String>) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.query.rpc_tls_server_cert = value.into();
        SessionManagerBuilder::inner_create(new_config)
    }

    pub fn api_tls_server_key(self, value: impl Into<String>) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.query.api_tls_server_key = value.into();
        SessionManagerBuilder::inner_create(new_config)
    }

    pub fn api_tls_server_cert(self, value: impl Into<String>) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.query.api_tls_server_cert = value.into();
        SessionManagerBuilder::inner_create(new_config)
    }

    pub fn api_tls_server_root_ca_cert(self, value: impl Into<String>) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.query.api_tls_server_root_ca_cert = value.into();
        SessionManagerBuilder::inner_create(new_config)
    }

    pub fn disk_storage_path(self, path: String) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.storage.disk.data_path = path;
        SessionManagerBuilder::inner_create(new_config)
    }

    pub fn log_dir_with_relative(self, path: impl Into<String>) -> SessionManagerBuilder {
        let mut new_config = self.config;
        new_config.log.log_dir = env::current_dir()
            .unwrap()
            .join(path.into())
            .display()
            .to_string();

        SessionManagerBuilder::inner_create(new_config)
    }

    pub fn build(self) -> Result<SessionManagerRef> {
        let config = self.config;
        let handle = Thread::spawn(move || sync_try_create_sessions(config));
        handle.join().unwrap()
    }
}
