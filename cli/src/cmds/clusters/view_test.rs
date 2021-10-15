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

use comfy_table::Cell;
use comfy_table::Color;
use comfy_table::Table;
use databend_meta::configs::Config as MetaConfig;
use databend_query::configs::Config as QueryConfig;
use httpmock::Method::GET;
use httpmock::MockServer;
use tempfile::tempdir;

use crate::cmds::clusters::view::HealthStatus;
use crate::cmds::clusters::view::ViewCommand;
use crate::cmds::status::LocalMetaConfig;
use crate::cmds::status::LocalQueryConfig;
use crate::cmds::Config;
use crate::cmds::Status;
use crate::error::Result;

#[test]
fn test_build_table() -> Result<()> {
    let mut conf = Config {
        group: "foo".to_string(),
        databend_dir: "/tmp/.databend".to_string(),
        download_url: "".to_string(),
        tag_url: "".to_string(),
        clap: RefCell::new(Default::default()),
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
            .body("health");
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
        let table = ViewCommand::build_local_table(&status);
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
