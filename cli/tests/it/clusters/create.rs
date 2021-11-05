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

use std::fs;

use bendctl::cmds::clusters::create::generate_local_log_dir;
use bendctl::cmds::clusters::create::generate_local_query_config;
use bendctl::cmds::clusters::create::LocalBinaryPaths;
use bendctl::cmds::config::GithubMirror;
use bendctl::cmds::config::MirrorAsset;
use bendctl::cmds::config::Mode;
use bendctl::cmds::Command;
use bendctl::cmds::Config;
use bendctl::cmds::CreateCommand;
use bendctl::error::Result;
use tempfile::tempdir;

#[test]
fn test_generate_local_meta_config() -> Result<()> {
    let mut conf = Config {
        group: "foo".to_string(),
        mode: Mode::Sql,
        databend_dir: "/tmp/.databend".to_string(),
        mirror: GithubMirror {}.to_mirror(),
        clap: Default::default(),
    };
    let t = tempdir()?;
    conf.databend_dir = t.path().to_str().unwrap().to_string();
    let args = CreateCommand::create(conf.clone()).clap();
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
            Some(format!("{}/logs/local_meta_log", conf.databend_dir))
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
            Some(format!("{}/logs/local_meta_log", conf.databend_dir))
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

#[test]
fn test_generate_local_query_config() -> Result<()> {
    let mut conf = Config {
        group: "foo".to_string(),
        mode: Mode::Sql,
        databend_dir: "/tmp/.databend".to_string(),
        mirror: GithubMirror {}.to_mirror(),
        clap: Default::default(),
    };
    let t = tempdir()?;
    conf.databend_dir = t.path().to_str().unwrap().to_string();
    let args = CreateCommand::create(conf.clone()).clap();
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
        let meta_config = create.generate_local_meta_config(&matches, mock_bin.clone());
        assert!(meta_config.is_some());
        let log_dir = generate_local_log_dir(&conf, "local_query_log_0");
        let query_config = generate_local_query_config(
            conf.clone(),
            &matches,
            mock_bin,
            &meta_config.unwrap(),
            log_dir,
        );

        assert_eq!(
            query_config.as_ref().unwrap().log_dir,
            Some(format!("{}/logs/local_query_log_0", conf.databend_dir))
        );
        // logs for query service
        assert_eq!(
            query_config.as_ref().unwrap().config.log.log_dir,
            format!("{}/logs/local_query_log_0", conf.databend_dir)
        );
        // clickhouse endpoint default settings
        assert_eq!(
            query_config
                .as_ref()
                .unwrap()
                .config
                .query
                .clickhouse_handler_host,
            "127.0.0.1"
        );
        assert_eq!(
            query_config
                .as_ref()
                .unwrap()
                .config
                .query
                .clickhouse_handler_port,
            9000
        );
        assert_eq!(
            query_config
                .as_ref()
                .unwrap()
                .config
                .query
                .mysql_handler_port,
            3307
        );
        assert_eq!(query_config.as_ref().unwrap().config.query.tenant, "test");
        assert_eq!(
            query_config.as_ref().unwrap().config.query.namespace,
            "test_cluster"
        );
        assert_eq!(
            query_config.as_ref().unwrap().config.storage.storage_type,
            "disk"
        );
        assert_eq!(
            query_config.as_ref().unwrap().config.storage.disk.data_path,
            fs::canonicalize(&format!("{}/data", conf.databend_dir))
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
        );
        assert_eq!(query_config.as_ref().unwrap().config.log.log_level, "INFO");
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
            "--num-cpus",
            "2",
            "--query-namespace",
            "customized_test",
            "--query-tenant",
            "customized_tenant",
            "--mysql-handler-port",
            "3309",
            "--clickhouse-handler-port",
            "9002",
            "--storage-type",
            "disk",
            "--disk-path",
            "/tmp",
        ]);
        let create = CreateCommand::create(conf.clone());
        let mock_bin = LocalBinaryPaths {
            query: format!("{}/meta/databend-query", conf.databend_dir),
            meta: format!("{}/meta/databend-meta", conf.databend_dir),
        };
        let meta_config = create.generate_local_meta_config(&matches, mock_bin.clone());
        assert!(meta_config.is_some());
        let log_dir = generate_local_log_dir(&conf, "local_query_log_0");

        let query_config = generate_local_query_config(
            conf.clone(),
            &matches,
            mock_bin,
            &meta_config.unwrap(),
            log_dir,
        );
        assert_eq!(query_config.as_ref().unwrap().config.query.num_cpus, 2);
        assert_eq!(
            query_config.as_ref().unwrap().log_dir,
            Some(format!("{}/logs/local_query_log_0", conf.databend_dir))
        );
        // logs for query service
        assert_eq!(
            query_config.as_ref().unwrap().config.log.log_dir,
            format!("{}/logs/local_query_log_0", conf.databend_dir)
        );
        // clickhouse endpoint default settings
        assert_eq!(
            query_config
                .as_ref()
                .unwrap()
                .config
                .query
                .clickhouse_handler_host,
            "127.0.0.1"
        );
        assert_eq!(
            query_config
                .as_ref()
                .unwrap()
                .config
                .query
                .clickhouse_handler_port,
            9002
        );
        assert_eq!(
            query_config
                .as_ref()
                .unwrap()
                .config
                .query
                .mysql_handler_port,
            3309
        );
        assert_eq!(
            query_config.as_ref().unwrap().config.query.tenant,
            "customized_tenant"
        );
        assert_eq!(
            query_config.as_ref().unwrap().config.query.namespace,
            "customized_test"
        );
        assert_eq!(
            query_config.as_ref().unwrap().config.storage.storage_type,
            "disk"
        );
        assert_eq!(
            query_config.as_ref().unwrap().config.storage.disk.data_path,
            fs::canonicalize("/tmp")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
        );
        assert_eq!(query_config.as_ref().unwrap().config.log.log_level, "DEBUG");
    }
    Ok(())
}
