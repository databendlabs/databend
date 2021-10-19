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
use std::str::FromStr;

use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;
use serde::Deserialize;
use serde::Serialize;

use crate::cmds::command::Command;
use crate::cmds::Config;
use crate::cmds::Writer;
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
                    .possible_values(&["local"]),
            )
            .arg(
                Arg::new("client")
                    .long("client")
                    .about("Set the query client to run query, support clickshouse client")
                    .takes_value(true)
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
        writer.write_ok(&*format!("{:?}", args.clone()));
        writer.write_ok(&*format!("{:?}", args.unwrap().value_of("query")));

        Ok(())
    }
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
        match self.clap.clone().try_get_matches_from(args.split(' ')) {
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
