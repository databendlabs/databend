// Copyright 2021 Datafuse Labs
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

mod args;
mod ast;
mod config;
mod display;
mod helper;
mod session;
mod trace;

use std::{
    collections::BTreeMap,
    io::{stdin, IsTerminal},
};

use anyhow::{anyhow, Result};
use clap::{ArgAction, CommandFactory, Parser, ValueEnum};
use databend_client::auth::SensitiveString;
use log::info;
use once_cell::sync::Lazy;

use crate::{
    args::ConnectionArgs,
    config::{Config, OutputFormat, OutputQuoteStyle, Settings, TimeOption},
};

static VERSION: Lazy<String> = Lazy::new(|| {
    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("unknown");
    let sha = option_env!("VERGEN_GIT_SHA").unwrap_or("dev");
    let timestamp = option_env!("VERGEN_BUILD_TIMESTAMP").unwrap_or("");
    match option_env!("BENDSQL_BUILD_INFO") {
        Some(info) => format!("{}-{}", version, info),
        None => format!("{}-{}({})", version, sha, timestamp),
    }
});

/// Supported file format and options:
/// https://databend.rs/doc/sql-reference/file-format-options
#[derive(ValueEnum, Clone, Debug, PartialEq)]
pub enum InputFormat {
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
                options.insert("skip_header", "0");
            }
            InputFormat::TSV => {
                options.insert("type", "TSV");
                options.insert("record_delimiter", "\n");
                options.insert("field_delimiter", "\t");
            }
            InputFormat::NDJSON => {
                options.insert("type", "NDJSON");
                options.insert("null_field_as", "NULL");
                options.insert("missing_field_as", "NULL");
            }
            InputFormat::Parquet => {
                options.insert("type", "Parquet");
            }
            InputFormat::XML => {
                options.insert("type", "XML");
                options.insert("row_tag", "row");
            }
        }
        for (k, v) in opts {
            // handle escaped newline chars in terminal for better usage
            let _ = match v.as_str() {
                "\\r\\n" => options.insert(k, "\r\n"),
                "\\r" => options.insert(k, "\r"),
                "\\n" => options.insert(k, "\n"),
                _ => options.insert(k, v),
            };
        }
        options
    }
}

#[derive(Debug, Parser, PartialEq)]
#[command(version = VERSION.as_str())]
// disable default help flag since it would conflict with --host
#[command(author, about, disable_help_flag = true)]
struct Args {
    #[clap(long, help = "Print help information")]
    help: bool,

    #[clap(long, help = "Using flight sql protocol, ignored when --dsn is set")]
    flight: bool,

    #[clap(long, help = "Enable TLS, ignored when --dsn is set")]
    tls: bool,

    #[clap(
        short = 'h',
        long,
        help = "Databend Server host, Default: 127.0.0.1, ignored when --dsn is set"
    )]
    host: Option<String>,

    #[clap(
        short = 'P',
        long,
        help = "Databend Server port, Default: 8000, ignored when --dsn is set"
    )]
    port: Option<u16>,

    #[clap(short = 'u', long, help = "Default: root, overrides username in DSN")]
    user: Option<String>,

    #[clap(
        short = 'p',
        long,
        env = "BENDSQL_PASSWORD",
        hide_env_values = true,
        help = "Password, overrides password in DSN"
    )]
    password: Option<SensitiveString>,

    #[clap(short = 'r', long, help = "Downgrade role name, overrides role in DSN")]
    role: Option<String>,

    #[clap(short = 'D', long, help = "Database name, overrides database in DSN")]
    database: Option<String>,

    #[clap(long, value_parser = parse_key_val::<String, String>, help = "Settings, overrides settings in DSN")]
    set: Vec<(String, String)>,

    #[clap(
        long,
        env = "BENDSQL_DSN",
        hide_env_values = true,
        help = "Data source name"
    )]
    dsn: Option<SensitiveString>,

    #[clap(short = 'n', long, help = "Force non-interactive mode")]
    non_interactive: bool,

    #[clap(long, help = "Check for server status and exit")]
    check: bool,

    #[clap(long, require_equals = true, help = "Query to execute")]
    query: Option<String>,

    #[clap(short = 'd', long, help = "Data to load, @file or @- for stdin")]
    data: Option<String>,

    #[clap(short = 'f', long, default_value = "csv", help = "Data format to load")]
    format: InputFormat,

    #[clap(long, value_parser = parse_key_val::<String, String>, help = "Data format options")]
    format_opt: Vec<(String, String)>,

    #[clap(short = 'o', long, help = "Output format")]
    output: Option<OutputFormat>,

    #[clap(
        long,
        help = "Output quote style, applies to `csv` and `tsv` output formats"
    )]
    quote_style: Option<OutputQuoteStyle>,

    #[clap(
        long,
        help = "Show progress for query execution in stderr, only works with output format `table` and `null`."
    )]
    progress: bool,

    #[clap(
        long,
        help = "Show stats after query execution in stderr, only works with non-interactive mode."
    )]
    stats: bool,

    #[clap(
        long,
        action = ArgAction::Set,
        num_args = 0..=1, require_equals = true, default_missing_value = "local",
        help = "Only show execution time without results, will implicitly set output format to `null`."
    )]
    time: Option<TimeOption>,

    #[clap(short = 'l', default_value = "info", long)]
    log_level: String,
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

#[tokio::main]
pub async fn main() -> Result<()> {
    let config = Config::load();

    let args = Args::parse();
    let mut cmd = Args::command();
    if args.help {
        cmd.print_help()?;
        return Ok(());
    }

    let mut conn_args = match args.dsn {
        Some(ref dsn) => {
            if args.host.is_some() {
                eprintln!("warning: --host is ignored when --dsn is set");
            }
            if args.port.is_some() {
                eprintln!("warning: --port is ignored when --dsn is set");
            }
            if !args.set.is_empty() {
                eprintln!("warning: --set is ignored when --dsn is set");
            }
            if args.tls {
                eprintln!("warning: --tls is ignored when --dsn is set");
            }
            if args.flight {
                eprintln!("warning: --flight is ignored when --dsn is set");
            }
            ConnectionArgs::from_dsn(dsn.inner())?
        }
        None => {
            let host = args.host.unwrap_or_else(|| config.connection.host.clone());
            let mut port = config.connection.port;
            if args.port.is_some() {
                port = args.port;
            }

            let user = args.user.unwrap_or_else(|| config.connection.user.clone());
            let password = args.password.unwrap_or_else(|| SensitiveString::from(""));

            ConnectionArgs {
                host,
                port,
                user,
                password,
                database: config.connection.database.clone(),
                flight: args.flight,
                args: config.connection.args.clone(),
            }
        }
    };

    // Override connection args with command line options
    {
        if args.database.is_some() {
            conn_args.database.clone_from(&args.database);
        }

        // override only if args.dsn is none
        if args.dsn.is_none() {
            if !args.tls {
                conn_args
                    .args
                    .insert("sslmode".to_string(), "disable".to_string());
            }

            // override args if specified in command line
            for (k, v) in args.set {
                conn_args.args.insert(k, v);
            }
        }

        // override role if specified in command line
        if let Some(role) = args.role {
            conn_args.args.insert("role".to_string(), role);
        }
    }

    let dsn = conn_args.get_dsn()?;
    let mut settings = Settings::default();
    let is_terminal = stdin().is_terminal();
    let is_repl = is_terminal && !args.non_interactive && !args.check && args.query.is_none();
    if is_repl {
        settings.display_pretty_sql = true;
        settings.show_progress = true;
        settings.show_stats = true;
        settings.output_format = OutputFormat::Table;
    } else {
        settings.output_format = OutputFormat::TSV;
    }

    settings.merge_config(config.settings);

    if let Some(output) = args.output {
        settings.output_format = output;
    }
    if let Some(quote_style) = args.quote_style {
        settings.quote_style = quote_style
    }
    if args.progress {
        settings.show_progress = true;
    }
    if args.stats {
        settings.show_stats = true;
    }
    if args.time.is_some() {
        settings.output_format = OutputFormat::Null;
    }
    settings.time = args.time;

    let mut session = session::Session::try_new(dsn, settings, is_repl).await?;

    let log_dir = format!(
        "{}/.bendsql",
        std::env::var("HOME").unwrap_or_else(|_| ".".to_string())
    );

    let _guards = trace::init_logging(&log_dir, &args.log_level).await?;
    info!("-> bendsql version: {}", VERSION.as_str());

    if args.check {
        session.check().await?;
        return Ok(());
    }

    if is_repl {
        session.handle_repl().await;
        return Ok(());
    }

    match args.query {
        None => {
            if args.non_interactive {
                return Err(anyhow!("no query specified"));
            }
            session.handle_reader(stdin().lock()).await?;
        }
        Some(query) => match args.data {
            None => {
                session.handle_reader(std::io::Cursor::new(query)).await?;
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
