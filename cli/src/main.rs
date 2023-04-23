// Copyright 2023 Datafuse Labs.
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

#![allow(clippy::upper_case_acronyms)]

mod ast;
mod config;
mod display;
mod helper;
mod session;

use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use clap::{CommandFactory, Parser, ValueEnum};
use config::Config;

/// Supported file format and options:
/// https://databend.rs/doc/sql-reference/file-format-options
#[derive(ValueEnum, Clone, Debug, PartialEq)]
enum InputFormat {
    CSV,
    TSV,
    NDJSON,
    Parquet,
    XML,
}

impl InputFormat {
    fn get_options<'o>(&self, opts: &'o Vec<(String, String)>) -> BTreeMap<&'o str, &'o str> {
        let mut options = BTreeMap::new();
        match self {
            InputFormat::CSV => {
                options.insert("type", "CSV");
                options.insert("record_delimiter", "\n");
                options.insert("field_delimiter", ",");
                options.insert("quote", "\"");
                options.insert("escape", "\"");
                options.insert("skip_header", "0");
                options.insert("compression", "NONE");
            }
            InputFormat::TSV => {
                options.insert("record_delimiter", "\n");
                options.insert("field_delimiter", "\t");
                options.insert("compression", "NONE");
            }
            InputFormat::NDJSON => {
                options.insert("compression", "NONE");
            }
            InputFormat::XML => {
                options.insert("compression", "NONE");
                options.insert("row_tag", "row");
            }
            _ => {}
        }
        for (k, v) in opts {
            options.insert(k, v);
        }
        options
    }
}

#[derive(ValueEnum, Clone, Debug, PartialEq)]
enum OutputFormat {
    Table,
    CSV,
    TSV,
}

#[derive(Debug, Parser, PartialEq)]
// disable default help flag since it would conflict with --host
#[command(author, version, about, disable_help_flag = true)]
struct Args {
    #[clap(long, help = "Print help information")]
    help: bool,

    #[clap(long, help = "Using flight sql protocol")]
    flight: bool,

    #[clap(long, help = "Enable TLS")]
    tls: bool,

    #[clap(short = 'h', long, help = "Databend Server host, Default: 127.0.0.1")]
    host: Option<String>,

    #[clap(short = 'P', long, help = "Databend Server port, Default: 8000")]
    port: Option<u16>,

    #[clap(short = 'u', long, help = "Default: root")]
    user: Option<String>,

    #[clap(short = 'p', long, env = "BENDSQL_PASSWORD")]
    password: Option<String>,

    #[clap(short = 'D', long, help = "Database name")]
    database: Option<String>,

    #[clap(long, value_parser = parse_key_val::<String, String>, help = "Settings")]
    set: Vec<(String, String)>,

    #[clap(long, env = "BENDSQL_DSN", help = "Data source name")]
    dsn: Option<String>,

    #[clap(short = 'n', long, help = "Force non-interactive mode")]
    non_interactive: bool,

    #[clap(short = 'q', long, help = "Query to execute")]
    query: Option<String>,

    #[clap(short = 'd', long, help = "Data to load, @file or @- for stdin")]
    data: Option<String>,

    #[clap(short = 'f', long, default_value = "csv", help = "Data format to load")]
    format: InputFormat,

    #[clap(long, value_parser = parse_key_val::<String, String>, help = "Data format options")]
    format_opt: Vec<(String, String)>,

    #[clap(short = 'o', long, default_value = "table", help = "Output format")]
    output: OutputFormat,

    #[clap(long, help = "Show progress for data loading in stderr")]
    progress: bool,
}

/// Parse a single key-value pair
fn parse_key_val<T, U>(
    s: &str,
) -> std::result::Result<(T, U), Box<dyn std::error::Error + Send + Sync + 'static>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: std::error::Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

struct ConnectionArgs {
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    database: Option<String>,
    tls: bool,
    flight: bool,
    args: BTreeMap<String, String>,
}

impl ConnectionArgs {
    fn get_dsn(self) -> Result<String> {
        let mut dsn = url::Url::parse("databend://")?;
        dsn.set_host(Some(&self.host))?;
        _ = dsn.set_port(Some(self.port));
        _ = dsn.set_username(&self.user);
        if let Some(password) = self.password {
            _ = dsn.set_password(Some(&password))
        };
        if let Some(database) = self.database {
            dsn.set_path(&database);
        }
        if self.flight {
            _ = dsn.set_scheme("databend+flight");
        } else if self.tls {
            _ = dsn.set_scheme("databend+https");
        }
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        if !self.args.is_empty() {
            for (k, v) in self.args {
                query.append_pair(&k, &v);
            }
        }
        if self.tls {
            query.append_pair("sslmode", "enable");
        } else {
            query.append_pair("sslmode", "disable");
        }
        dsn.set_query(Some(&query.finish()));
        Ok(dsn.to_string())
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let mut config = Config::load();

    let args = Args::parse();
    let mut cmd = Args::command();
    if args.help {
        cmd.print_help()?;
        return Ok(());
    }
    let dsn = match args.dsn {
        Some(dsn) => dsn,
        None => {
            if let Some(host) = args.host {
                config.connection.host = host;
            }
            if let Some(port) = args.port {
                config.connection.port = port;
            }
            if let Some(user) = args.user {
                config.connection.user = user;
            }
            if args.database.is_some() {
                config.connection.database = args.database;
            }
            for (k, v) in args.set {
                config.connection.args.insert(k, v);
            }
            let conn_args = ConnectionArgs {
                host: config.connection.host.clone(),
                port: config.connection.port,
                user: config.connection.user.clone(),
                password: args.password,
                database: config.connection.database.clone(),
                tls: args.tls,
                flight: args.flight,
                args: config.connection.args.clone(),
            };
            conn_args.get_dsn()?
        }
    };

    let is_repl = atty::is(atty::Stream::Stdin) && !args.non_interactive && args.query.is_none();
    let mut session = session::Session::try_new(dsn, config.settings, is_repl).await?;

    if is_repl {
        session.handle().await;
        return Ok(());
    }

    match args.query {
        None => {
            if args.non_interactive {
                return Err(anyhow!("no query specified"));
            }
            session.handle_stdin().await
        }
        Some(query) => match args.data {
            None => {
                if let Err(e) = session.handle_query(false, &query).await {
                    eprintln!("{}", e);
                }
            }
            Some(data) => {
                let options = args.format.get_options(&args.format_opt);
                if data.starts_with('@') {
                    match data.strip_prefix('@') {
                        Some("-") => session.stream_load_stdin(&query, options).await?,
                        Some(fname) => {
                            let path = std::path::Path::new(fname);
                            if !path.exists() {
                                return Err(anyhow!("file not found: {}", fname));
                            }
                            session.stream_load_file(&query, path, options).await?
                        }
                        None => {
                            return Err(anyhow!("invalid data input: {}", data));
                        }
                    }
                } else {
                    // TODO: should we allow passing data directly?
                    return Err(anyhow!("invalid data input: {}", data));
                }
            }
        },
    }
    Ok(())
}
