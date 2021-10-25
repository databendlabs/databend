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

use databend_meta::configs::Config as MetaConfig;
use databend_query::configs::Config as QueryConfig;
use tempfile::tempdir;

use crate::cmds::config::GithubMirror;
use crate::cmds::config::MirrorAsset;
use crate::cmds::queries::query::build_query_endpoint;
use crate::cmds::status::LocalMetaConfig;
use crate::cmds::status::LocalQueryConfig;
use crate::cmds::Config;
use crate::cmds::Status;
use crate::error::Result;

macro_rules! build_status {
    ($conf: expr, $http_port: expr) => {
        let mut status = Status::read($conf)?;
        let mut meta_config = LocalMetaConfig {
            config: MetaConfig::default(),
            pid: Some(123),
            path: Some("./".to_string()),
            log_dir: Some("./".to_string()),
        };
        meta_config.config.admin_api_address = format!("127.0.0.1:{}", 123);
        Status::save_local_config(
            &mut status,
            "meta".parse().unwrap(),
            "meta_1.yaml".to_string(),
            &meta_config,
        )
        .unwrap();
        let mut query_config = LocalQueryConfig {
            config: QueryConfig::default(),
            pid: Some(123),
            path: Some("./".to_string()),
            log_dir: Some("./".to_string()),
        };
        query_config.config.query.http_handler_host = "0.0.0.0".to_string();
        query_config.config.query.http_handler_port = $http_port;
        query_config.config.query.http_api_address = format!("127.0.0.1:{}", 456);

        Status::save_local_config(
            &mut status,
            "query".parse().unwrap(),
            "query_1.yaml".to_string(),
            &query_config,
        )
        .unwrap();
        status.write()?;
    };
}

#[test]
fn test_generate_query_probe() -> Result<()> {
    let mut conf = Config {
        group: "foo".to_string(),
        databend_dir: "/tmp/.databend".to_string(),
        mirror: GithubMirror {}.to_mirror(),
        clap: Default::default(),
    };
    let t = tempdir()?;
    conf.databend_dir = t.path().to_str().unwrap().to_string();
    // test on default bahavior
    {
        build_status!(conf.clone(), 8888);
        let status = Status::read(conf).unwrap();
        let (_, query_url) = build_query_endpoint(&status).unwrap();
        assert_eq!(query_url, "http://0.0.0.0:8888/v1/statement".to_string());
    }
    Ok(())
}
