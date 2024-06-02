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
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use clap::Parser;
use databend_common_base::base::tokio;
use databend_common_base::runtime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::Operation;
use databend_common_tracing::init_logging;
use databend_common_tracing::FileConfig;
use databend_common_tracing::StderrConfig;
use databend_meta::version::METASRV_COMMIT_VERSION;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser)]
#[clap(about, version = &**METASRV_COMMIT_VERSION, author)]
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

    #[clap(long, default_value = "INFO")]
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
            format: "text".to_string(),
            limit: 48,
            prefix_filter: "databend_".to_string(),
        },
        stderr: StderrConfig {
            on: true,
            level: "WARN".to_string(),
            format: "text".to_string(),
        },
        ..Default::default()
    };

    let _guards = init_logging("databend-metaverifier", &log_config, BTreeMap::new());

    println!("config: {:?}", config);
    if config.grpc_api_address.is_empty() {
        println!("grpc_api_address MUST not be empty!");
        return Err(ErrorCode::MetaServiceError(
            "grpc_api_address MUST not be empty!".to_string(),
        ));
    }

    if config.remove_percent > 100 {
        println!("remove_percent MUST in [0, 100)!");
        return Err(ErrorCode::MetaServiceError(
            "remove_percent MUST in [0, 100)!".to_string(),
        ));
    }

    let start = Instant::now();
    let mut client_num = 0;
    let (tx, rx) = mpsc::channel::<()>();
    let mut handles = Vec::new();

    // write a file as start
    let file = "/tmp/meta-verifier".to_string();
    fs::write(&file, "START")?;

    while client_num < config.client {
        client_num += 1;
        let prefix = config.prefix;
        let addrs: Vec<_> = config
            .grpc_api_address
            .split(",")
            .map(|addr| addr.to_string())
            .collect();

        let handle = runtime::spawn(async move {
            let client = MetaGrpcClient::try_create(addrs.clone(), "root", "xxx", None, None, None);

            let client = match client {
                Ok(client) => client,
                Err(e) => {
                    eprintln!("Failed to create client: {}", e);
                    return Err(ErrorCode::MetaServiceError(e.to_string()));
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
        });
        println!("verifier worker {} started..", client_num);
        handles.push(handle)
    }

    let waiter_handle = runtime::spawn(async move {
        let result = rx.recv_timeout(Duration::from_secs(config.time));

        match result {
            Ok(_) => {
                println!("verifier completed within the timeout.");
            }
            Err(_) => {
                println!("verifier did not complete within the timeout.");
                panic!("verifier did not complete within the timeout.")
            }
        }
    });

    let wait_verifier_handle = runtime::spawn(async move {
        for handle in handles {
            let ret = handle.await.unwrap();
            if let Err(e) = ret {
                return Err(e);
            }
        }
        tx.send(()).unwrap();

        Ok(())
    });

    let _ret = wait_verifier_handle.await.unwrap();
    if let Err(e) = waiter_handle.await {
        let end = Instant::now();
        println!(
            "verifier client return error: {:?} in {} milliseconds",
            e,
            end.duration_since(start).as_millis()
        );
        return Err(ErrorCode::MetaServiceError(e.to_string()));
    }

    let end = Instant::now();
    println!(
        "verifier client({}) * number({}) in {} milliseconds",
        config.client,
        config.number,
        end.duration_since(start).as_millis()
    );

    // write a file as end
    fs::write(&file, "END")?;

    Ok(())
}

async fn verifier(
    client: &Arc<ClientHandle>,
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
            .upsert_kv(UpsertKVReq::new(&node_key, seq, value, None))
            .await?;

        let n: u64 = rng.gen_range(0..=100);
        if n < remove_percent {
            let seq = MatchSeq::Any;
            let value = Operation::Delete;

            let _res = client
                .upsert_kv(UpsertKVReq::new(&node_key, seq, value, None))
                .await?;
        } else {
            kv.insert(node_key);
        }
    }

    for node_key in kv.iter() {
        let res = client.get_kv(&node_key).await?;
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
