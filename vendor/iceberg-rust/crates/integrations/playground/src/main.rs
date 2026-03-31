// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;

use clap::Parser;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_cli::exec;
use datafusion_cli::print_format::PrintFormat;
use datafusion_cli::print_options::{MaxRows, PrintOptions};
use iceberg_playground::{ICEBERG_PLAYGROUND_VERSION, IcebergCatalogList};

#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
struct Args {
    #[clap(
        short = 'r',
        long,
        help = "Parse catalog config instead of using ~/.icebergrc"
    )]
    rc: Option<String>,

    #[clap(long, value_enum, default_value_t = PrintFormat::Automatic)]
    format: PrintFormat,

    #[clap(
        short,
        long,
        help = "Reduce printing other than the results and work quietly"
    )]
    quiet: bool,

    #[clap(
        long,
        help = "The max number of rows to display for 'Table' format\n[possible values: numbers(0/10/...), inf(no limit)]",
        default_value = "40"
    )]
    maxrows: MaxRows,

    #[clap(long, help = "Enables console syntax highlighting")]
    color: bool,
}

#[tokio::main]
/// Calls [`main_inner`], then handles printing errors and returning the correct exit code
pub async fn main() -> ExitCode {
    tracing_subscriber::fmt::init();

    if let Err(e) = main_inner().await {
        println!("Error: {e}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

async fn main_inner() -> anyhow::Result<()> {
    let args = Args::parse();

    if !args.quiet {
        println!("ICEBERG PLAYGROUND v{ICEBERG_PLAYGROUND_VERSION}");
    }

    let session_config = SessionConfig::from_env()?.with_information_schema(true);

    let rt_builder = RuntimeEnvBuilder::new();

    let runtime_env = rt_builder.build_arc()?;

    // enable dynamic file query
    let ctx = SessionContext::new_with_config_rt(session_config, runtime_env).enable_url_table();
    ctx.refresh_catalogs().await?;

    let mut print_options = PrintOptions {
        format: args.format,
        quiet: args.quiet,
        maxrows: args.maxrows,
        color: args.color,
    };

    let rc = match args.rc {
        Some(file) => PathBuf::from_str(&file)?,
        None => dirs::home_dir()
            .map(|h| h.join(".icebergrc"))
            .ok_or_else(|| anyhow::anyhow!("cannot find home directory"))?,
    };

    let catalogs = Arc::new(IcebergCatalogList::parse(&rc).await?);
    ctx.register_catalog_list(catalogs);

    Ok(exec::exec_from_repl(&ctx, &mut print_options).await?)
}
