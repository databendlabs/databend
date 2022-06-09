// Copyright 2021 Datafuse Labs.
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

use std::time::Instant;

use clap::Parser;
use common_base::base::tokio;
use common_meta_api::KVApi;
use common_meta_grpc::MetaGrpcClient;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
use common_meta_types::UpsertKVReq;
use databend_meta::version::METASRV_COMMIT_VERSION;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser)]
#[clap(about, version = &**METASRV_COMMIT_VERSION, author)]
struct Config {
    #[clap(long, default_value = "10")]
    pub client: u32,

    #[clap(long, default_value = "10000")]
    pub number: u32,

    #[clap(long, default_value = "INFO")]
    pub log_level: String,

    #[clap(long, env = "METASRV_GRPC_API_ADDRESS", default_value = "")]
    pub grpc_api_address: String,
}

#[tokio::main]
async fn main() {
    let config = Config::parse();

    println!("config: {:?}", config);
    if config.grpc_api_address.is_empty() {
        println!("grpc_api_address MUST not be empty!");
        return;
    }

    let start = Instant::now();
    let mut client_num = 0;
    let mut handles = Vec::new();
    while client_num < config.client {
        client_num += 1;
        let addr = config.grpc_api_address.clone();

        let handle = tokio::spawn(async move {
            let client =
                MetaGrpcClient::try_create(vec![addr.to_string()], "root", "xxx", None, None);
            if client.is_err() {
                return ();
            }
            let client = client.unwrap();
            for i in 0..config.number {
                let node_key = format!("{}-{}", client_num, i);
                let seq = MatchSeq::Any;
                let value = Operation::Update(b"v3".to_vec());

                let _res = client
                    .upsert_kv(UpsertKVReq::new(&node_key, seq, value, None))
                    .await;
            }
            ()
        });
        handles.push(handle)
    }

    for handle in handles {
        handle.await.unwrap();
    }
    let end = Instant::now();
    println!(
        "benchmark client({}) * number({}) in {} milliseconds",
        config.client,
        config.number,
        end.duration_since(start).as_millis()
    );
}
