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

mod display;
mod helper;
mod session;
mod token;

use std::time::Duration;

use arrow::error::ArrowError;

use clap::Parser;
use tonic::transport::{ClientTlsConfig, Endpoint};

#[derive(Debug, Parser, PartialEq)]
#[command(disable_help_flag = true)]
struct Args {
    #[clap(short = 'u', long, default_value = "root", help = "User name")]
    user: String,

    #[clap(short = 'p', long, default_value = "", help = "User password")]
    password: String,

    #[clap(
        short = 'h',
        long,
        default_value = "127.0.0.1",
        help = "Flight SQL Server host"
    )]
    host: String,
    #[clap(
        short = 'P',
        long,
        default_value_t = 8900,
        help = "Flight SQL Server port"
    )]
    port: u16,

    #[clap(long)]
    tls: bool,

    #[clap(long, help = "Print help information")]
    help: bool,
}

#[tokio::main]
pub async fn main() -> Result<(), ArrowError> {
    let args = Args::parse();
    if args.help {
        print_usage();
        return Ok(());
    }

    let protocol = if args.tls { "https" } else { "http" };
    // Authenticate
    let url = format!("{protocol}://{}:{}", args.host, args.port);
    let endpoint = endpoint(&args, url)?;
    let is_repl = atty::is(atty::Stream::Stdin);

    let mut session =
        session::Session::try_new(endpoint, &args.user, &args.password, is_repl).await?;

    session.handle().await;
    Ok(())
}

fn print_usage() {
    let msg = r#"Usage: databend-cli <--user <USER>|--password <PASSWORD>|--host <HOST>|--port <PORT>|--tls>"#;
    println!("{}", msg);
}

fn endpoint(args: &Args, addr: String) -> Result<Endpoint, ArrowError> {
    let mut endpoint = Endpoint::new(addr)
        .map_err(|_| ArrowError::IoError("Cannot create endpoint".to_string()))?
        .connect_timeout(Duration::from_secs(20))
        .timeout(Duration::from_secs(20))
        .tcp_nodelay(true) // Disable Nagle's Algorithm since we don't want packets to wait
        .tcp_keepalive(Option::Some(Duration::from_secs(3600)))
        .http2_keep_alive_interval(Duration::from_secs(300))
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true);

    if args.tls {
        let tls_config = ClientTlsConfig::new();
        endpoint = endpoint
            .tls_config(tls_config)
            .map_err(|_| ArrowError::IoError("Cannot create TLS endpoint".to_string()))?;
    }

    Ok(endpoint)
}
