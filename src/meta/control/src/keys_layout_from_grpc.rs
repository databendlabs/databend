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

use std::io;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;

use anyhow::anyhow;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MetaGrpcClient;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_types::protobuf;
use tokio::net::TcpSocket;
use tokio_stream::StreamExt;

use crate::args::KeysLayoutArgs;

/// Get snapshot keys layout from a running meta-service node
pub async fn keys_layout_from_running_node(args: &KeysLayoutArgs) -> Result<(), anyhow::Error> {
    eprintln!();
    eprintln!("Keys Layout:");
    eprintln!("    From: online meta-service: {}", args.grpc_api_address);
    eprintln!("    Depth: {:?}", args.depth);

    let grpc_api_addr = get_available_socket_addr(args.grpc_api_address.as_str()).await?;
    let addr = grpc_api_addr.to_string();
    keys_layout_from_grpc(addr.as_str(), args.depth).await?;
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

pub async fn keys_layout_from_grpc(addr: &str, depth: Option<u32>) -> anyhow::Result<()> {
    let client = MetaGrpcClient::<DatabendRuntime>::try_create(
        vec![addr.to_string()],
        "root",
        "xxx",
        None,
        None,
        None,
        DEFAULT_GRPC_MESSAGE_SIZE,
    )?;

    let mut grpc_client = client.make_established_client().await?;

    let request = protobuf::KeysLayoutRequest { depth };
    let response = grpc_client.snapshot_keys_layout(request).await?;

    let mut stream = response.into_inner();

    while let Some(keys_count_result) = stream.next().await {
        let keys_count = keys_count_result?;
        println!("{}", keys_count);
    }

    Ok(())
}
