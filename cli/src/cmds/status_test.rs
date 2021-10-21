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

use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use common_base::tokio;
use databend_meta::configs::Config as MetaConfig;
use databend_query::configs::Config as QueryConfig;
use httpmock::Method::GET;
use httpmock::MockServer;
use tempfile::tempdir;

use crate::cmds::config::GithubMirror;
use crate::cmds::config::MirrorAsset;
use crate::cmds::status::LocalMetaConfig;
use crate::cmds::status::LocalQueryConfig;
use crate::cmds::status::LocalRuntime;
use crate::cmds::Config;
use crate::cmds::Status;
use crate::error::Result;

#[test]
fn test_status() -> Result<()> {
    let mut conf = Config {
        group: "foo".to_string(),
        databend_dir: "/tmp/.databend".to_string(),
        mirror: GithubMirror {}.to_mirror(),
        clap: Default::default(),
    };
    let t = tempdir()?;
    conf.databend_dir = t.path().to_str().unwrap().to_string();
    // empty profile
    {
        let mut status = Status::read(conf.clone())?;
        status.version = "xx".to_string();
        status.write()?;
        // should have empty profile with set version
        if let Ok(status) = Status::read(conf.clone()) {
            assert_eq!(status.version, "xx".to_string());
            assert_eq!(status.local_configs, HashMap::new());
        }
    }

    // create basic local profile
    {
        let mut status = Status::read(conf.clone())?;
        status.version = "default".to_string();
        status.local_configs = HashMap::new();
        status.write()?;
        // should have empty profile with set version
        if let Ok(status) = Status::read(conf.clone()) {
            assert_eq!(status.version, "default".to_string());
            assert_eq!(status.local_configs, HashMap::new());
        }
    }

    // update query component on local
    {
        let mut status = Status::read(conf.clone())?;
        let query_config = LocalQueryConfig {
            config: QueryConfig::default(),
            pid: Some(123),
            path: None,
            log_dir: Some("./".to_string()),
        };
        Status::save_local_config(
            &mut status,
            "query".parse().unwrap(),
            "query_1.yaml".to_string(),
            &query_config,
        )
        .unwrap();
        status.version = "default".to_string();
        status.write()?;
        let mut status = Status::read(conf.clone()).unwrap();
        assert_eq!(status.version, "default");
        assert_eq!(status.get_local_query_configs().len(), 1);
        assert_eq!(
            status.get_local_query_configs().get(0).unwrap().clone().1,
            query_config.clone()
        );
        assert!(status.has_local_configs());
        let meta_config = LocalMetaConfig {
            config: MetaConfig::empty(),
            pid: Some(123),
            path: Some("String".to_string()),
            log_dir: Some("dir".to_string()),
        };
        Status::save_local_config(
            &mut status,
            "meta".parse().unwrap(),
            "meta_1.yaml".to_string(),
            &meta_config,
        )
        .unwrap();
        let query_config2 = LocalQueryConfig {
            config: QueryConfig::default(),
            pid: None,
            path: None,
            log_dir: None,
        };
        Status::save_local_config(
            &mut status,
            "query".parse().unwrap(),
            "query_2.yaml".to_string(),
            &query_config2,
        )
        .unwrap();
        status.current_profile = Some("local".to_string());
        status.write()?;
        let status = Status::read(conf.clone()).unwrap();
        assert_eq!(status.version, "default");
        assert_eq!(status.get_local_query_configs().len(), 2);
        assert_eq!(
            status.get_local_query_configs().get(0).unwrap().clone().1,
            query_config
        );
        assert_eq!(
            status.get_local_query_configs().get(1).unwrap().clone().1,
            query_config2
        );
        assert_eq!(status.get_local_meta_config().unwrap().1, meta_config);
        assert_eq!(status.current_profile, Some("local".to_string()));
        assert!(status.has_local_configs());
        // delete status
        let mut status = Status::read(conf.clone()).unwrap();
        let (fs, _) = status.clone().get_local_meta_config().unwrap();
        Status::delete_local_config(&mut status, "meta".to_string(), fs).unwrap();
        for (fs, _) in status.clone().get_local_query_configs() {
            Status::delete_local_config(&mut status, "query".to_string(), fs).unwrap();
        }
        status.current_profile = None;
        status.write()?;
        let status = Status::read(conf).unwrap();
        assert_eq!(status.get_local_query_configs().len(), 0);
        assert_eq!(status.get_local_meta_config(), None);
        assert_eq!(status.current_profile, None);
        assert!(!status.has_local_configs());
    }
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_verify() -> Result<()> {
    let mut conf = Config {
        group: "foo".to_string(),
        databend_dir: "/tmp/.databend".to_string(),
        clap: Default::default(),
        mirror: GithubMirror {}.to_mirror(),
    };
    let t = tempdir()?;
    conf.databend_dir = t.path().to_str().unwrap().to_string();
    // Start a lightweight mock server.
    let server = MockServer::start();
    // Create a mock on the server.
    let _ = server.mock(|when, then| {
        when.method(GET).path("/v1/health");
        then.status(200)
            .header("content-type", "text/html")
            .body("health")
            .delay(Duration::from_millis(100));
    });

    let mut meta_config = LocalMetaConfig {
        config: MetaConfig::default(),
        pid: Some(123),
        path: Some("./".to_string()),
        log_dir: Some("./".to_string()),
    };
    meta_config.config.admin_api_address = format!("127.0.0.1:{}", server.port());
    let mut query_config = LocalQueryConfig {
        config: QueryConfig::default(),
        pid: Some(123),
        path: Some("./".to_string()),
        log_dir: Some("./".to_string()),
    };
    query_config.config.query.http_api_address = format!("127.0.0.1:{}", server.port());
    let begin = Instant::now();
    let mut handles = Vec::with_capacity(2);
    handles.push(tokio::spawn(async move { meta_config.verify().await }));
    handles.push(tokio::spawn(async move { query_config.verify().await }));
    for handle in handles {
        handle.await.unwrap()?;
    }
    let elapsed = begin.elapsed();
    // elasped < mock_server.delay * (meta_num + query_num)
    assert!(elapsed.as_millis() < 200);
    Ok(())
}
