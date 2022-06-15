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

use std::fmt::Debug;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use common_base::base::tokio;
use common_meta_api::KVApi;
use common_meta_api::SchemaApi;
use common_meta_app::schema::CreateDatabaseReq;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::DatabaseNameIdent;
use common_meta_app::schema::GetTableReq;
use common_meta_app::schema::TableNameIdent;
use common_meta_app::schema::UpsertTableOptionReq;
use common_meta_grpc::ClientHandle;
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

    /// The RPC to benchmark: "upsert_kv": send kv-api upsert_kv, "table": create db, table and upsert_table_option;
    #[clap(long, default_value = "upsert_kv")]
    pub rpc: String,
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
        let typ = config.rpc.clone();

        let handle = tokio::spawn(async move {
            let client =
                MetaGrpcClient::try_create(vec![addr.to_string()], "root", "xxx", None, None);
            if client.is_err() {
                return;
            }
            let client = client.unwrap();

            for i in 0..config.number {
                if typ == "upsert_kv" {
                    benchmark_upsert(&client, client_num, i).await;
                } else if typ == "table" {
                    benchmark_table(&client, client_num, i).await;
                } else {
                    unreachable!("Invalid config.rpc: {}", typ);
                }
            }
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

async fn benchmark_upsert(client: &Arc<ClientHandle>, client_num: u32, i: u32) {
    let node_key = format!("{}-{}", client_num, i);
    let seq = MatchSeq::Any;
    let value = Operation::Update(b"v3".to_vec());

    let res = client
        .upsert_kv(UpsertKVReq::new(&node_key, seq, value, None))
        .await;

    print_res(i, "upsert_kv", &res);
}

async fn benchmark_table(client: &Arc<ClientHandle>, client_num: u32, i: u32) {
    let res = client
        .create_database(CreateDatabaseReq {
            if_not_exists: false,
            name_ident: DatabaseNameIdent {
                tenant: format!("tenant-{}", client_num),
                db_name: format!("db-{}", client_num),
            },
            meta: Default::default(),
        })
        .await;

    print_res(i, "create_db", &res);

    let res = client
        .create_table(CreateTableReq {
            if_not_exists: true,
            name_ident: TableNameIdent {
                tenant: format!("tenant-{}", client_num),
                db_name: format!("db-{}", client_num),
                table_name: format!("table-{}", client_num),
            },
            table_meta: Default::default(),
        })
        .await;

    print_res(i, "create_table", &res);

    let res = client
        .get_table(GetTableReq::new(
            format!("tenant-{}", client_num),
            format!("db-{}", client_num),
            format!("table-{}", client_num),
        ))
        .await;

    print_res(i, "get_table", &res);

    let t = res.unwrap();

    let res = client
        .upsert_table_option(UpsertTableOptionReq {
            table_id: t.ident.table_id,
            seq: MatchSeq::GE(t.ident.seq),
            options: Default::default(),
        })
        .await;

    print_res(i, "upsert_table_option", &res);
}

fn print_res<D: Debug>(i: u32, typ: impl Display, res: &D) {
    if i % 100 == 0 {
        println!("{:>10}-th {} result: {:?}", i, typ, res);
    }
}
