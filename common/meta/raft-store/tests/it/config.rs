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

use common_meta_raft_store::config::RaftConfig;
use common_meta_types::MetaError;

#[test]
fn test_raft_config() -> anyhow::Result<()> {
    {
        let raft_config = &RaftConfig {
            single: true,
            join: vec!["j1".to_string()],
            ..Default::default()
        };
        let r = raft_config.check();

        assert_eq!(
            r,
            Err(MetaError::InvalidConfig(String::from(
                "--join and --single can not be both set",
            )))
        )
    }

    {
        let addr: String = "127.0.0.1".to_string();
        let port = 22222;
        let raft_config = &RaftConfig {
            single: false,
            raft_listen_host: addr.clone(),
            raft_api_port: port,
            join: vec![format!("{}:{}", addr, port)],
            ..Default::default()
        };
        let r = raft_config.check();

        assert_eq!(
            r,
            Err(MetaError::InvalidConfig(String::from(
                "--join must not be set to itself",
            )))
        )
    }

    Ok(())
}
