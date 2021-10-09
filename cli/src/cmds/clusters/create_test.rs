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

use tempfile::tempdir;

use crate::cmds::clusters::create::LocalBinaryPaths;
use crate::cmds::Config;
use crate::cmds::CreateCommand;
use crate::error::Result;

#[test]
fn test_generate_local_meta_config() -> Result<()> {
    let mut conf = Config {
        group: "foo".to_string(),
        databend_dir: "/tmp/.databend".to_string(),
        download_url: "".to_string(),
        tag_url: "".to_string(),
        clap: RefCell::new(Default::default()),
    };
    let t = tempdir()?;
    conf.databend_dir = t.path().to_str().unwrap().to_string();
    let args = CreateCommand::generate();
    // test on default bahavior
    {
        let matches = args
            .clone()
            .get_matches_from(&["create", "--profile", "local"]);
        let create = CreateCommand::create(conf.clone());
        let mock_bin = LocalBinaryPaths {
            query: format!("{}/meta/databend-query", conf.databend_dir),
            meta: format!("{}/meta/databend-meta", conf.databend_dir),
        };
        let config = create.generate_local_meta_config(&matches, mock_bin);
        assert!(config.is_some());
        // logs for std out and std err
        assert_eq!(
            config.as_ref().unwrap().log_dir,
            Some(format!("{}/logs", conf.databend_dir))
        );
        // logs for meta service
        assert_eq!(
            config.as_ref().unwrap().config.log_dir,
            format!("{}/logs/local_meta_log", conf.databend_dir)
        );
        // raft_dir store meta service data
        assert_eq!(
            config.as_ref().unwrap().config.raft_config.raft_dir,
            format!("{}/logs/local_raft_dir", conf.databend_dir)
        );

        assert_eq!(config.as_ref().unwrap().config.log_level, "INFO");
    }

    // test on customized meta service
    {
        let matches = args.get_matches_from(&[
            "create",
            "--profile",
            "local",
            "--meta-address",
            "0.0.0.0:7777",
            "--log-level",
            "DEBUG",
            "--version",
            "v0.4.111-nightly",
        ]);
        let create = CreateCommand::create(conf.clone());
        let mock_bin = LocalBinaryPaths {
            query: format!("{}/meta/databend-query", conf.databend_dir),
            meta: format!("{}/meta/databend-meta", conf.databend_dir),
        };
        let config = create.generate_local_meta_config(&matches, mock_bin);
        assert!(config.is_some());
        // logs for std out and std err
        assert_eq!(
            config.as_ref().unwrap().log_dir,
            Some(format!("{}/logs", conf.databend_dir))
        );
        // logs for meta service
        assert_eq!(
            config.as_ref().unwrap().config.log_dir,
            format!("{}/logs/local_meta_log", conf.databend_dir)
        );
        // raft_dir store meta service data
        assert_eq!(
            config.as_ref().unwrap().config.raft_config.raft_dir,
            format!("{}/logs/local_raft_dir", conf.databend_dir)
        );

        assert_eq!(config.as_ref().unwrap().config.log_level, "DEBUG");
        assert_eq!(
            config.as_ref().unwrap().config.flight_api_address,
            "0.0.0.0:7777"
        );
    }
    Ok(())
}
