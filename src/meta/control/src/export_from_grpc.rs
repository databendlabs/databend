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

use std::fs::File;
use std::io;
use std::io::Write;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;

use anyhow::anyhow;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MetaGrpcClient;
use databend_meta_raft_store::key_spaces::RaftStoreEntry;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_types::protobuf;
use tokio::net::TcpSocket;
use tokio_stream::StreamExt;

use crate::args::ExportArgs;

/// Dump metasrv data, raft-log, state machine etc in json to stdout.
pub async fn export_from_running_node(args: &ExportArgs) -> Result<(), anyhow::Error> {
    eprintln!();
    eprintln!("Export:");
    eprintln!("    From: online meta-service: {}", args.grpc_api_address);
    eprintln!("    Export To: {}", args.db);
    eprintln!("    Export Chunk Size: {:?}", args.chunk_size);

    let grpc_api_addr = get_available_socket_addr(args.grpc_api_address.as_str()).await?;
    let addr = grpc_api_addr.to_string();
    export_from_grpc(addr.as_str(), args.db.clone(), args.chunk_size).await?;
    Ok(())
}

/// if port is open, service is running
async fn is_service_running(addr: SocketAddr) -> Result<bool, io::Error> {
    let socket = TcpSocket::new_v4()?;
    let stream = socket.connect(addr).await;

    Ok(stream.is_ok())
}

/// try to get available grpc api socket address
async fn get_available_socket_addr(endpoint: &str) -> Result<SocketAddr, anyhow::Error> {
    let addrs_iter = endpoint.to_socket_addrs()?;
    for addr in addrs_iter {
        if is_service_running(addr).await? {
            return Ok(addr);
        }
        eprintln!("WARN: {} is not available", addr);
    }
    Err(anyhow!("no metasrv running on: {}", endpoint))
}

pub async fn export_from_grpc(
    addr: &str,
    save: String,
    chunk_size: Option<u64>,
) -> anyhow::Result<()> {
    let client = MetaGrpcClient::<DatabendRuntime>::try_create_with_features(
        vec![addr.to_string()],
        "root",
        "xxx",
        None,
        None,
        None,
        DEFAULT_GRPC_MESSAGE_SIZE,
    )?;

    let mut grpc_client = client.make_established_client().await?;

    // TODO: since 1.2.315, export_v1() is added, via which chunk size can be specified.
    let exported = if grpc_client.server_protocol_version() >= 1002315 {
        grpc_client
            .export_v1(protobuf::ExportRequest { chunk_size })
            .await?
    } else {
        grpc_client.export(protobuf::Empty {}).await?
    };

    let mut stream = exported.into_inner();

    let mut file: Option<File> = if !save.is_empty() {
        eprintln!("    To:   File: {}", save);
        Some(File::create(&save)?)
    } else {
        eprintln!("    To:   <stdout>");
        None
    };

    while let Some(chunk_res) = stream.next().await {
        let chunk = chunk_res?;

        for line in &chunk.data {
            // Check if the received line is a valid json string.
            let de_res: Result<(String, RaftStoreEntry), _> = serde_json::from_str(line);
            match de_res {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Invalid json string: {:?}", line);
                    eprintln!("              Error: {}", e);
                    return Err(e.into());
                }
            }

            if let Some(ref mut f) = file {
                f.write_all(format!("{}\n", line).as_bytes())?;
            } else {
                println!("{}", line);
            }
        }
    }

    if let Some(ref f) = file {
        f.sync_all()?;
    }

    Ok(())
}
