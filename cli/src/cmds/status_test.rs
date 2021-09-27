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

use std::cell::RefCell;

use databend_dfs::configs::Config as StoreConfig;
use databend_query::configs::Config as QueryConfig;
use tempfile::tempdir;

use crate::cmds::status::LocalConfig;
use crate::cmds::status::LocalQueryConfig;
use crate::cmds::status::LocalStoreConfig;
use crate::cmds::Config;
use crate::cmds::Status;
use crate::error::Result;

macro_rules! default_local_config {
    () => {
        LocalConfig {
            query_configs: vec![LocalQueryConfig {
                pid: Some(123),
                config: QueryConfig::default(),
                path: Some("~/.databend/test/databend-query".to_string())
            }],
            store_configs: Some(LocalStoreConfig {
                pid: Some(234),
                config: StoreConfig::empty(),
                path: Some("~/.databend/test/databend-store".to_string())
            }),
            meta_configs: Some(LocalStoreConfig {
                pid: Some(345),
                config: StoreConfig::empty(),
                path: Some("~/.databend/test/databend-store".to_string())
            })
        }
    };
}
#[test]
fn test_status() -> Result<()> {
    let mut conf = Config {
        group: "foo".to_string(),
        databend_dir: "/tmp/.databend".to_string(),
        download_url: "".to_string(),
        tag_url: "".to_string(),
        clap: RefCell::new(Default::default()),
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
            assert_eq!(status.local_configs, LocalConfig::empty());
        }
    }

    // create basic local profile
    {
        let mut status = Status::read(conf.clone())?;
        status.version = "default".to_string();
        status.local_configs = default_local_config!();
        status.write()?;
        // should have empty profile with set version
        if let Ok(status) = Status::read(conf.clone()) {
            assert_eq!(status.version, "default".to_string());
            assert_eq!(status.local_configs, default_local_config!());
        }
    }

    // update query component on local
    {
        let mut status = Status::read(conf.clone())?;
        let mut local_config = default_local_config!();
        local_config.query_configs.push(LocalQueryConfig {
            config: QueryConfig::default(),
            pid: Some(123),
            path: None
        });
        status.version = "default".to_string();
        status.local_configs = local_config;
        status.write()?;

        let mut expected_config = default_local_config!();
        expected_config.query_configs.push(LocalQueryConfig {
            config: QueryConfig::default(),
            pid: Some(123),
            path: None
        });
        // should have empty profile with set version
        if let Ok(status) = Status::read(conf.clone()) {
            assert_eq!(status.version, "default".to_string());
            assert_eq!(status.local_configs, expected_config);
        }
    }
    Ok(())
}
