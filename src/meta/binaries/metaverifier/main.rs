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

#![allow(clippy::uninlined_format_args)]

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fs;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;
use std::time::Instant;

use anyhow::Result;
use anyhow::bail;
use clap::Parser;
use databend_common_tracing::FileConfig;
use databend_common_tracing::LogFormat;
use databend_common_tracing::StderrConfig;
use databend_common_tracing::init_logging;
use databend_common_version::METASRV_COMMIT_VERSION;
use databend_meta_client::ClientHandle;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MetaGrpcClient;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_runtime_api::SpawnApi;
use databend_meta_types::MatchSeq;
use databend_meta_types::Operation;
use databend_meta_types::UpsertKV;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use serde::Deserialize;
use serde::Serialize;

pub static VERIFIER_RESULT_FILE: &str = "/tmp/meta-verifier";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser)]
#[clap(about, version = METASRV_COMMIT_VERSION.as_str(), author)]
struct Config {
    /// The prefix of keys to write.
    #[clap(long, default_value = "0")]
    pub prefix: u64,

    #[clap(long, default_value = "10")]
    pub client: u64,

    /// run timeout, default 300s
    #[clap(long, default_value = "300")]
    pub time: u64,

    #[clap(long, default_value = "10")]
    pub remove_percent: u64,

    #[clap(long, default_value = "10000")]
    pub number: u64,

    #[clap(long, default_value = "warn,databend_=info")]
    pub log_level: String,

    #[clap(long, env = "METASRV_GRPC_API_ADDRESS", default_value = "")]
    pub grpc_api_address: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();

    let log_config = databend_common_tracing::Config {
        file: FileConfig {
            on: true,
            level: config.log_level.clone(),
            dir: "./.databend/logs".to_string(),
            format: LogFormat::Text,
            limit: 48,
            max_size: 4294967296,
        },
        stderr: StderrConfig {
            on: true,
            level: "WARN".to_string(),
            format: LogFormat::Text,
        },
        ..Default::default()
    };

    let guards = init_logging("databend-metaverifier", &log_config, BTreeMap::new());
    Box::new(guards).leak();

    println!("config: {:?}", config);
    if config.grpc_api_address.is_empty() {
        println!("grpc_api_address MUST not be empty!");
        bail!("grpc_api_address MUST not be empty!");
    }

    if config.remove_percent > 100 {
        println!("remove_percent MUST in [0, 100)!");
        bail!("remove_percent MUST in [0, 100)!");
    }

    let start = Instant::now();
    let mut client_num = 0;
    let (tx, rx) = mpsc::channel::<()>();
    let mut handles = Vec::new();

    // write a file as start
    fs::write(VERIFIER_RESULT_FILE, "START")?;

    while client_num < config.client {
        client_num += 1;
        let prefix = config.prefix;
        let addrs: Vec<_> = config
            .grpc_api_address
            .split(',')
            .map(|addr| addr.to_string())
            .collect();

        let handle = DatabendRuntime::spawn(
            async move {
                let client = MetaGrpcClient::<DatabendRuntime>::try_create(
                    addrs.clone(),
                    "root",
                    "xxx",
                    None,
                    None,
                    None,
                    DEFAULT_GRPC_MESSAGE_SIZE,
                );

                let client = match client {
                    Ok(client) => client,
                    Err(e) => {
                        fs::write(VERIFIER_RESULT_FILE, "ERROR")?;
                        eprintln!("Failed to create client: {}", e);
                        bail!("Failed to create client: {}", e);
                    }
                };

                verifier(
                    &client,
                    prefix,
                    config.number,
                    client_num,
                    config.remove_percent,
                )
                .await
            },
            None,
        );
        println!("verifier worker {} started..", client_num);
        handles.push(handle)
    }

    let waiter_handle = DatabendRuntime::spawn(
        async move {
            let result = rx.recv_timeout(Duration::from_secs(config.time));

            match result {
                Ok(_) => {
                    println!("verifier completed within the timeout.");
                }
                Err(e) => {
                    println!(
                        "verifier did not complete within the timeout: {:?}s, error: {:?}",
                        config.time, e
                    );
                    let _ = fs::write(VERIFIER_RESULT_FILE, "ERROR");
                    panic!("verifier did not complete within the timeout.")
                }
            }
        },
        None,
    );

    let wait_verifier_handle = DatabendRuntime::spawn(
        async move {
            for handle in handles {
                let ret = handle.await.unwrap();
                if let Err(e) = ret {
                    fs::write(VERIFIER_RESULT_FILE, "ERROR")?;
                    println!("verifier return error: {:?}", e);
                    return Err(e);
                }
            }
            tx.send(()).unwrap();

            Ok(())
        },
        None,
    );

    let _ret = wait_verifier_handle.await.unwrap();
    if let Err(e) = waiter_handle.await {
        let end = Instant::now();
        println!(
            "verifier client return error: {:?} in {} milliseconds",
            e,
            end.duration_since(start).as_millis()
        );
        fs::write(VERIFIER_RESULT_FILE, "ERROR")?;
        bail!("verifier client error: {}", e);
    }

    let end = Instant::now();
    println!(
        "verifier client({}) * number({}) in {} milliseconds",
        config.client,
        config.number,
        end.duration_since(start).as_millis()
    );

    // write a file as end
    fs::write(VERIFIER_RESULT_FILE, "END")?;

    Ok(())
}

async fn verifier(
    client: &Arc<ClientHandle<DatabendRuntime>>,
    prefix: u64,
    number: u64,
    client_num: u64,
    remove_percent: u64,
) -> Result<()> {
    let mut kv: HashSet<String> = HashSet::new();
    let mut rng = StdRng::from_entropy();
    let start = Instant::now();

    for i in 0..number {
        let node_key = format!("{}-{}-{}", prefix, client_num, i);

        let seq = MatchSeq::Any;
        let value = Operation::Update(node_key.as_bytes().to_vec());

        let _res = client
            .upsert_kv(UpsertKV::new(&node_key, seq, value, None))
            .await?;

        let n: u64 = rng.gen_range(0..=100);
        if n < remove_percent {
            let seq = MatchSeq::Any;
            let value = Operation::Delete;

            let _res = client
                .upsert_kv(UpsertKV::new(&node_key, seq, value, None))
                .await?;
        } else {
            kv.insert(node_key);
        }
    }

    for node_key in kv.iter() {
        let res = client.get_kv(node_key).await?;
        assert_eq!(res.unwrap().data, node_key.as_bytes().to_vec());
    }

    let end = Instant::now();
    println!(
        "client-{} * number({}) in {} milliseconds",
        prefix,
        number,
        end.duration_since(start).as_millis()
    );

    Ok(())
}
