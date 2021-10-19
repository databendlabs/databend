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
                Arg::new("client")
                    .long("client")
                    .about(
                        "Set the query client to run query, support mysql and clickshouse client",
                    )
                    .takes_value(true)
                    .possible_values(&["mysql", "clickhouse"])
                    .default_value("mysql")
                    .env(CLI_QUERY_CLIENT),
            )
            .arg(
                Arg::new("query")
                    .about("Query statements to run")
                    .takes_value(true)
                    .required(true),
            )
            .arg(
                Arg::new("file")
                    .short('f')
                    .long("file")
                    .about("execute query commands from file")
                    .takes_value(true)
                    .required(false),
            );
        app
    }

    pub(crate) fn exec_match(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
        match args {
            Some(matches) => {
                let profile = matches.value_of_t("profile");
                match profile {
                    Ok(ClusterProfile::Local) => {
                        return self.local_exec_match(writer, matches);
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

    fn local_exec_match(&self, writer: &mut Writer, args: &ArgMatches) -> Result<()> {
        match self.local_exec_precheck(args) {
            Ok(_) => {
                writer.write_ok("Query precheck passed!");
                let status = Status::read(self.conf.clone())?;
                if args.value_of("file").is_none() {
                    if let Some(query) = args.value_of("query") {
                        let url = build_query_url(args, &status);
                        if let Ok(url) = url {
                            writer.write_ok(format!("Execute query {} on {}", query, url).as_str());
                            match execute_query(
                                status.query_path.unwrap(),
                                url,
                                query.parse().unwrap(),
                            ) {
                                Ok(res) => {
                                    writer.write_ok(res.as_str());
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
                                    url.unwrap_err()
                                )
                                .as_str(),
                            );
                        }
                    } else {
                        writer.write_err("Query command error: cannot find SQL argument!");
                        return Ok(());
                    }
                } else {
                    todo!()
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

pub fn build_query_url(matches: &ArgMatches, status: &Status) -> Result<String> {
    let client = matches.value_of_t("client");
    let query_configs = status.get_local_query_configs();

    let (_, query) = query_configs.get(0).expect("cannot find query configs");
    if let Ok(client) = client {
        let url = match client {
            QueryClient::Mysql => {
                let scheme = "mysql";
                format!(
                    "{}://{}:{}@{}:{}",
                    scheme,
                    query.config.meta.meta_username,
                    query.config.meta.meta_password,
                    query.config.query.mysql_handler_host,
                    query.config.query.mysql_handler_port
                )
            }
            QueryClient::Clickhouse => {
                let scheme = "clickhouse";
                format!(
                    "{}://{}:{}@{}:{}",
                    scheme,
                    query.config.meta.meta_username,
                    query.config.meta.meta_password,
                    query.config.query.clickhouse_handler_host,
                    query.config.query.clickhouse_handler_port
                )
            }
        };
        Ok(url)
    } else {
        Err(CliError::Unknown(
            "Query command error: cannot get query client".to_string(),
        ))
    }
}

fn execute_query(bin_path: String, url: String, query: String) -> Result<String> {
    let mut command = std::process::Command::new(bin_path);
    command.args([url.as_str(), "-c", query.as_str()]);
    // Tell the OS to record the command's output
    command.stdout(Stdio::piped()).stderr(Stdio::null());
    // execute the command, wait for it to complete, then capture the output
    return match command.output() {
        Ok(output) => {
            let stdout = String::from_utf8(output.stdout).unwrap().trim().to_string();
            Ok(stdout)
        }
        Err(e) => Err(CliError::Unknown(format!(
            "Query command error: cannot execute query, error : {:?}",
            e
        ))),
    };
}

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

    fn exec(&self, writer: &mut Writer, args: String) -> Result<()> {
        let words = shellwords::split(args.as_str());
        if words.is_err() {
            writer.write_err("cannot parse words");
            return Ok(());
        }
        match self.clap.clone().try_get_matches_from(words.unwrap()) {
            Ok(matches) => {
                return self.exec_match(writer, Some(matches.borrow()));
            }
            Err(err) => {
                println!("Cannot get subcommand matches: {}", err);
            }
        }

        Ok(())
    }
}
