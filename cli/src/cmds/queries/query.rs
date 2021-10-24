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

use std::borrow::Borrow;
use std::path::Path;
use std::process::Stdio;
use std::str::FromStr;

use async_trait::async_trait;
use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;
use nix::NixPath;
use serde::Deserialize;
use serde::Serialize;

use crate::cmds::clusters::cluster::ClusterProfile;
use crate::cmds::command::Command;
use crate::cmds::Config;
use crate::cmds::Status;
use crate::cmds::Writer;
use crate::error::CliError;
use crate::error::Result;
use std::net::SocketAddr;
use comfy_table::{Table, Cell, Color};

pub const CLI_QUERY_CLIENT: &str = "CLI_QUERY_CLIENT";

#[derive(Clone)]
pub struct QueryCommand {
    #[allow(dead_code)]
    conf: Config,
    clap: App<'static>,
}

// Supported clients to run queries
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryClient {
    Clickhouse,
    Mysql,
}

// Implement the trait
impl FromStr for QueryClient {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<QueryClient, &'static str> {
        match s {
            "mysql" => Ok(QueryClient::Mysql),
            "clickhouse" => Ok(QueryClient::Clickhouse),
            _ => Err("no match for client"),
        }
    }
}

impl QueryCommand {
    pub fn create(conf: Config) -> Self {
        let clap = QueryCommand::generate();
        QueryCommand { conf, clap }
    }
    pub fn generate() -> App<'static> {
        let app = App::new("query")
            .setting(AppSettings::DisableVersionFlag)
            .about("Query on databend cluster")
            .arg(
                Arg::new("profile")
                    .long("profile")
                    .about("Profile to run queries")
                    .required(false)
                    .possible_values(&["local"])
                    .default_value("local"),
            )
            .arg(
                Arg::new("query")
                    .about("Query statements to run")
                    .takes_value(true)
                    .required(true),
            );
        app
    }

    pub(crate) async fn exec_match(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        match args {
            Some(matches) => {
                let profile = matches.value_of_t("profile");
                match profile {
                    Ok(ClusterProfile::Local) => {
                        return self.local_exec_match(writer, matches).await;
                    }
                    Ok(ClusterProfile::Cluster) => {
                        todo!()
                    }
                    Err(_) => writer.write_err("currently profile only support cluster or local"),
                }
            }
            None => {
                println!("none ");
            }
        }
        Ok(())
    }

    async fn local_exec_match(&self, writer: &mut Writer, args: &ArgMatches) -> Result<()> {
        match self.local_exec_precheck(args) {
            Ok(_) => {
                writer.write_ok("Query precheck passed!");
                let status = Status::read(self.conf.clone())?;
                    if let Some(query) = args.value_of("query") {
                        let res = build_query_endpoint(&status);
                        if let Ok((cli, url)) = res {
                            writer.write_ok(format!("Execute query {} on {}", query, url).as_str());
                            match execute_query(
                                cli,
                                url,
                                query.parse().unwrap(),
                            ).await {
                                Ok(res) => {
                                    writer.writeln(res.trim_fmt().as_str());
                                }
                                Err(e) => {
                                    writer.write_err(format!("Query command error: cannot execute query with error: {:?}", e).as_str());
                                }
                            }
                            return Ok(());
                        } else {
                            writer.write_err(
                                format!(
                                    "Query command error: cannot parse query url with error: {:?}",
                                    res.unwrap_err()
                                )
                                .as_str(),
                            );
                        }
                    } else {
                        writer.write_err("Query command error: cannot find SQL argument!");
                        return Ok(());
                    }

                Ok(())
            }
            Err(e) => {
                writer.write_err(&*format!("Query command precheck failed, error {:?}", e));
                Ok(())
            }
        }
    }

    /// precheck whether current local profile applicable for local host machine
    fn local_exec_precheck(&self, _args: &ArgMatches) -> Result<()> {
        let status = Status::read(self.conf.clone())?;
        if !status.has_local_configs() {
            return Err(CliError::Unknown(format!(
                "Query command error: cannot find local configs in {}, please run `bendctl cluster create --profile local` to create a new local cluster",
                status.local_config_dir
            )));
        }
        if status.query_path.is_none() || Path::new(status.query_path.unwrap().as_str()).is_empty()
        {
            return Err(CliError::Unknown(
                "Query command error: cannot find local query binary path, please run `bendctl package fetch` to install it".to_string(),
            ));
        }
        Ok(())
    }
}

// TODO(zhihanz) mTLS support
pub fn build_query_endpoint(status: &Status) -> Result<(reqwest::Client, String)> {
    let query_configs = status.get_local_query_configs();

    let (_, query) = query_configs.get(0).expect("cannot find query configs");
    let client = reqwest::Client::builder()
        .build()
        .expect("Cannot build query client");

    let url = {
        if query.config.query.api_tls_server_key.is_empty() || query.config.query.api_tls_server_cert.is_empty(){
            let address = format!("{}:{}", query.config.query.http_handler_host, query.config.query.http_handler_port).parse::<SocketAddr>().expect("cannot build query socket address");
            format!("http://{}:{}/v1/statement", address.ip(), address.port())
        } else {
            todo!()
        }
    };
    Ok((client, url))
}

async fn execute_query(cli: reqwest::Client, url: String, query: String) -> Result<Table> {
   let ans = cli.post(url).body(query).send().await.expect("cannot post to http handler").json::<databend_query::servers::http::v1::statement::HttpQueryResult>().await;
    if let Err(e) = ans {
        return Err(CliError::Unknown(format!("Cannot retrieve query result: {:?}", e)))
    } else {
        let ans = ans.unwrap();
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");
        if let Some(column) = ans.columns {
            table.set_header(column.fields().iter().map(|field| Cell::new(field.name().as_str()).fg(Color::Green)));
        }
        if let Some(rows) = ans.data {
            for row in rows {
                table.add_row(row.iter().map(|elem| Cell::new(elem.to_string())));
            }
        }
        Ok(table)
    }
}

#[async_trait]
impl Command for QueryCommand {
    fn name(&self) -> &str {
        "query"
    }

    fn about(&self) -> &str {
        "Query on databend cluster"
    }

    fn is(&self, s: &str) -> bool {
        s.contains(self.name())
    }

    async fn exec(&self, writer: &mut Writer, args: String) -> Result<()> {
        let words = shellwords::split(args.as_str());
        if words.is_err() {
            writer.write_err("cannot parse words");
            return Ok(());
        }
        match self.clap.clone().try_get_matches_from(words.unwrap()) {
            Ok(matches) => {
                return self.exec_match(writer, Some(matches.borrow())).await;
            }
            Err(err) => {
                println!("Cannot get subcommand matches: {}", err);
            }
        }

        Ok(())
    }
}
