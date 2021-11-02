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

use std::time::Duration;

use bendctl::cmds::clusters::view::HealthStatus;
use bendctl::cmds::clusters::view::ViewCommand;
use bendctl::cmds::config::GithubMirror;
use bendctl::cmds::config::MirrorAsset;
use bendctl::cmds::config::Mode;
use bendctl::cmds::status::LocalMetaConfig;
use bendctl::cmds::status::LocalQueryConfig;
use bendctl::cmds::Config;
use bendctl::cmds::Status;
use bendctl::error::Result;
use comfy_table::Cell;
use comfy_table::Color;
use comfy_table::Table;
use common_base::tokio;
use databend_meta::configs::Config as MetaConfig;
use databend_query::configs::Config as QueryConfig;
use httpmock::Method::GET;
use httpmock::MockServer;
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_build_table() -> Result<()> {
    let mut conf = Config {
        group: "foo".to_string(),
        mode: Mode::Sql,
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
    {
        let mut status = Status::read(conf)?;
        let mut meta_config = LocalMetaConfig {
            config: MetaConfig::default(),
            pid: Some(123),
            path: Some("./".to_string()),
            log_dir: Some("./".to_string()),
        };
        meta_config.config.admin_api_address = format!("127.0.0.1:{}", server.port());
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
        query_config.config.query.http_api_address = format!("127.0.0.1:{}", server.port());

        Status::save_local_config(
            &mut status,
            "query".parse().unwrap(),
            "query_1.yaml".to_string(),
            &query_config,
        )
        .unwrap();
        status.version = "build_table".to_string();
        status.write()?;
        let table = ViewCommand::build_local_table(
            &status,
            Some(2),
            Option::from(Duration::from_millis(100)),
        )
        .await;
        let (meta_file, _) = status.get_local_meta_config().unwrap();
        let query_configs = status.get_local_query_configs();
        assert!(table.is_ok());
        let table = table.unwrap();
        let mut expected = Table::new();
        expected.load_preset("||--+-++|    ++++++");
        // Title.
        expected.set_header(vec![
            Cell::new("Name"),
            Cell::new("Profile"),
            Cell::new("Health"),
            Cell::new("Tls"),
            Cell::new("Config"),
        ]);
        expected.add_row(vec![
            Cell::new("meta_1"),
            Cell::new("local"),
            Cell::new(format!("{}", HealthStatus::Ready)).fg(Color::Green),
            Cell::new("disabled"),
            Cell::new(meta_file),
        ]);
        expected.add_row(vec![
            Cell::new("query_1"),
            Cell::new("local"),
            Cell::new(format!("{}", HealthStatus::Ready)).fg(Color::Green),
            Cell::new("disabled"),
            Cell::new(query_configs.get(0).unwrap().0.clone()),
        ]);
        assert_eq!(table.to_string(), expected.to_string())
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_build_table_fail() -> Result<()> {
    let mut conf = Config {
        group: "foo".to_string(),
        mode: Mode::Sql,
        databend_dir: "/tmp/.databend".to_string(),
        clap: Default::default(),
        mirror: GithubMirror {}.to_mirror(),
    };
    let t = tempdir()?;
    conf.databend_dir = t.path().to_str().unwrap().to_string();
    // Start a lightweight mock server.
    let server = MockServer::start();

    let server2 = MockServer::start();
    // Create a mock on the server.
    let _ = server.mock(|when, then| {
        when.method(GET).path("/v1/health");
        then.status(200)
            .header("content-type", "text/html")
            .body("health")
            .delay(Duration::from_millis(100));
    });
    {
        let mut status = Status::read(conf)?;
        let mut meta_config = LocalMetaConfig {
            config: MetaConfig::default(),
            pid: Some(123),
            path: Some("./".to_string()),
            log_dir: Some("./".to_string()),
        };
        meta_config.config.admin_api_address = format!("127.0.0.1:{}", server.port());
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
        query_config.config.query.http_api_address = format!("127.0.0.1:{}", server2.port());

        Status::save_local_config(
            &mut status,
            "query".parse().unwrap(),
            "query_1.yaml".to_string(),
            &query_config,
        )
        .unwrap();
        let mut query_config2 = LocalQueryConfig {
            config: QueryConfig::default(),
            pid: Some(123),
            path: Some("./".to_string()),
            log_dir: Some("./".to_string()),
        };
        query_config2.config.query.http_api_address = format!("127.0.0.1:{}", server2.port());

        Status::save_local_config(
            &mut status,
            "query".parse().unwrap(),
            "query_2.yaml".to_string(),
            &query_config2,
        )
        .unwrap();
        status.version = "build_table".to_string();
        status.write()?;
        let table = ViewCommand::build_local_table(
            &status,
            Some(1),
            Option::from(Duration::from_millis(100)),
        )
        .await;
        let (meta_file, _) = status.get_local_meta_config().unwrap();
        let query_configs = status.get_local_query_configs();
        assert!(table.is_ok());
        let table = table.unwrap();
        let mut expected = Table::new();
        expected.load_preset("||--+-++|    ++++++");
        // Title.
        expected.set_header(vec![
            Cell::new("Name"),
            Cell::new("Profile"),
            Cell::new("Health"),
            Cell::new("Tls"),
            Cell::new("Config"),
        ]);
        expected.add_row(vec![
            Cell::new("meta_1"),
            Cell::new("local"),
            Cell::new(format!("{}", HealthStatus::Ready)).fg(Color::Green),
            Cell::new("disabled"),
            Cell::new(meta_file),
        ]);
        expected.add_row(vec![
            Cell::new("query_1"),
            Cell::new("local"),
            Cell::new(format!("{}", HealthStatus::UnReady)).fg(Color::Red),
            Cell::new("disabled"),
            Cell::new(query_configs.get(0).unwrap().0.clone()),
        ]);
        expected.add_row(vec![
            Cell::new("query_2"),
            Cell::new("local"),
            Cell::new(format!("{}", HealthStatus::UnReady)).fg(Color::Red),
            Cell::new("disabled"),
            Cell::new(query_configs.get(1).unwrap().0.clone()),
        ]);
        assert_eq!(table.to_string(), expected.to_string())
    }
    Ok(())
}
