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
use std::io::Read;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;
use comfy_table::Cell;
use comfy_table::Color;
use comfy_table::Table;
use common_base::ProgressValues;
use common_datavalues::prelude::*;
use lexical_util::num::AsPrimitive;
use num_format::Locale;
use num_format::ToFormattedString;
use serde_json::Value;

use crate::cmds::clusters::cluster::ClusterProfile;
use crate::cmds::command::Command;
use crate::cmds::status::LocalRuntime;
use crate::cmds::Config;
use crate::cmds::Status;
use crate::cmds::Writer;
use crate::error::CliError;
use crate::error::Result;

#[derive(Clone)]
pub struct QueryCommand {
    #[allow(dead_code)]
    conf: Config,
}

impl QueryCommand {
    pub fn create(conf: Config) -> Self {
        QueryCommand { conf }
    }

    pub fn default() -> Self {
        QueryCommand::create(Config::default())
    }

    async fn local_exec_match(&self, writer: &mut Writer, args: &ArgMatches) -> Result<()> {
        match self.local_exec_precheck(args).await {
            Ok(_) => {
                writer.write_ok("Query precheck passed!".to_string());
                let status = Status::read(self.conf.clone())?;
                let queries = match args.value_of("query") {
                    Some(val) => {
                        if Path::new(val).exists() {
                            let buffer =
                                std::fs::read(Path::new(val)).expect("cannot read query from file");
                            String::from_utf8_lossy(&*buffer).to_string()
                        } else if val.starts_with("http://") || val.starts_with("https://") {
                            let res = reqwest::get(val)
                                .await
                                .expect("cannot fetch query from url")
                                .text()
                                .await
                                .expect("cannot fetch response body");
                            res
                        } else {
                            val.to_string()
                        }
                    }
                    None => {
                        let mut buffer = String::new();
                        std::io::stdin()
                            .read_to_string(&mut buffer)
                            .expect("cannot read from stdin");
                        buffer
                    }
                };

                let res = build_query_endpoint(&status);

                if let Ok((cli, url)) = res {
                    for query in queries
                        .split(';')
                        .filter(|elem| !elem.trim().is_empty())
                        .map(|elem| format!("{};", elem))
                        .collect::<Vec<String>>()
                    {
                        writer.write_debug(format!("Execute query {} on {}", query.clone(), url));
                        if let Err(e) =
                            query_writer(&cli, url.as_str(), query.clone(), writer).await
                        {
                            writer.write_err(format!("Query {} execution error: {:?}", query, e));
                        }
                    }
                } else {
                    writer.write_err(format!(
                        "Query command error: cannot parse query url with error: {:?}",
                        res.unwrap_err()
                    ));
                }

                Ok(())
            }
            Err(e) => {
                writer.write_err(format!(
                "Query command precheck failed, error {:?}, please run `bendctl cluster create` to create a new local cluster or '\\admin' switch to the admin mode", e));
                Ok(())
            }
        }
    }

    /// precheck whether current local profile applicable for local host machine
    async fn local_exec_precheck(&self, _args: &ArgMatches) -> Result<()> {
        let status = Status::read(self.conf.clone())?;
        if status.current_profile.is_none() {
            return Err(CliError::Unknown(format!(
                "cannot find local configs in {}",
                status.local_config_dir
            )));
        }

        let query_configs = status.get_local_query_configs();
        let (_, query) = query_configs.first().expect("cannot find query configs");
        match query.verify(None, None).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub async fn exec(&self, writer: &mut Writer, args: String) -> Result<()> {
        match self
            .clap()
            .clone()
            .try_get_matches_from(vec!["query", args.as_str()])
        {
            Ok(matches) => {
                return self.exec_matches(writer, Some(matches.borrow())).await;
            }
            Err(err) => {
                println!("Cannot get subcommand matches: {}", err);
            }
        }

        Ok(())
    }
}

async fn query_writer(
    cli: &reqwest::Client,
    url: &str,
    query: String,
    writer: &mut Writer,
) -> Result<()> {
    let start = std::time::Instant::now();
    match execute_query(cli, url, query).await {
        Ok((res, stats)) => {
            let elapsed = start.elapsed();
            writer.writeln(res.as_str());
            if let Some(stat) = stats {
                let time = elapsed.as_millis() as f64 / 1000f64;
                let byte_per_sec = byte_unit::Byte::from_unit(
                    stat.read_bytes as f64 / time,
                    byte_unit::ByteUnit::B,
                )
                .expect("cannot parse byte")
                .get_appropriate_unit(false);
                writer.write_ok(
                    format!(
                        "read rows: {}, read bytes: {}, rows/sec: {} (rows/sec), bytes/sec: {} ({}/sec), time: {} sec",
                        stat.read_rows.to_formatted_string(&Locale::en),
                        byte_unit::Byte::from_bytes(stat.read_bytes as u128)
                            .get_appropriate_unit(false)
                            .to_string(),
                        (stat.read_rows as f64 / time).as_u128().to_formatted_string(&Locale::en),
                        byte_per_sec.get_value(),
                        byte_per_sec.get_unit().to_string(), time
                    )
                );
            }
        }
        Err(e) => {
            writer.write_err(format!(
                "Query command error: cannot execute query with error: {:?}",
                e
            ));
        }
    }
    Ok(())
}

// TODO(zhihanz) mTLS support
pub fn build_query_endpoint(status: &Status) -> Result<(reqwest::Client, String)> {
    let query_configs = status.get_local_query_configs();

    let (_, query) = query_configs.get(0).expect("cannot find query configs");
    let client = reqwest::Client::builder()
        .build()
        .expect("Cannot build query client");

    let url = {
        if query.config.query.api_tls_server_key.is_empty()
            || query.config.query.api_tls_server_cert.is_empty()
        {
            let address = format!(
                "{}:{}",
                query.config.query.http_handler_host, query.config.query.http_handler_port
            )
            .parse::<SocketAddr>()
            .expect("cannot build query socket address");
            format!("http://{}:{}/v1/statement", address.ip(), address.port())
        } else {
            todo!()
        }
    };
    Ok((client, url))
}

pub async fn execute_query_json(
    cli: &reqwest::Client,
    url: &str,
    query: String,
) -> Result<(
    Option<DataSchemaRef>,
    Option<Vec<Vec<Value>>>,
    Option<ProgressValues>,
)> {
    let ans = cli
        .post(url)
        .body(query.clone())
        .send()
        .await
        .expect("cannot post to http handler")
        .json::<databend_query::servers::http::v1::statement::HttpQueryResult>()
        .await;
    if let Err(e) = ans {
        return Err(CliError::Unknown(format!(
            "Cannot retrieve query result: {:?}",
            e
        )));
    } else {
        let ans = ans.unwrap();
        if ans.error.is_some() {
            return Err(CliError::Unknown(format!(
                "Query has error: {:?}",
                ans.error.unwrap()
            )));
        }
        Ok((ans.columns, ans.data, ans.stats))
    }
}

async fn execute_query(
    cli: &reqwest::Client,
    url: &str,
    query: String,
) -> Result<(String, Option<ProgressValues>)> {
    let (columns, data, stats) = execute_query_json(cli, url, query).await?;
    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");
    if let Some(column) = columns {
        table.set_header(
            column
                .fields()
                .iter()
                .map(|field| Cell::new(field.name().as_str()).fg(Color::Green)),
        );
    }
    if let Some(rows) = data {
        if rows.is_empty() {
            return Ok(("".to_string(), stats));
        } else {
            for row in rows {
                table.add_row(row.iter().map(|elem| Cell::new(elem.to_string())));
            }
            return Ok((table.trim_fmt(), stats));
        }
    }
    Ok(("".to_string(), stats))
}

#[async_trait]
impl Command for QueryCommand {
    fn name(&self) -> &str {
        "query"
    }

    fn clap(&self) -> App<'static> {
        App::new("query")
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
            )
    }

    fn about(&self) -> &'static str {
        "Query on databend cluster"
    }

    fn is(&self, s: &str) -> bool {
        s.contains(self.name())
    }

    fn subcommands(&self) -> Vec<Arc<dyn Command>> {
        vec![]
    }

    async fn exec_matches(&self, writer: &mut Writer, args: Option<&ArgMatches>) -> Result<()> {
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
                    Err(_) => writer
                        .write_err("Currently profile only support cluster or local".to_string()),
                }
            }
            None => {
                println!("none ");
            }
        }
        Ok(())
    }
}
