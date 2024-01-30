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
use std::io::Write;

use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_raft_store::key_spaces::RaftStoreEntry;
use databend_common_meta_types::protobuf as pb;
use tokio_stream::StreamExt;

pub async fn export_meta(addr: &str, save: String, chunk_size: Option<u64>) -> anyhow::Result<()> {
    let client =
        MetaGrpcClient::try_create(vec![addr.to_string()], "root", "xxx", None, None, None)?;

    let mut grpc_client = client.make_established_client().await?;

    // TODO: since 1.2.315, export_v1() is added, via which chunk size can be specified.
    let exported = if grpc_client.server_protocol_version() >= 1002315 {
        grpc_client
            .export_v1(pb::ExportRequest { chunk_size })
            .await?
    } else {
        grpc_client.export(pb::Empty {}).await?
    };

    let mut stream = exported.into_inner();

    let file: Option<File> = if !save.is_empty() {
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

            if file.as_ref().is_none() {
                println!("{}", line);
            } else {
                file.as_ref()
                    .unwrap()
                    .write_all(format!("{}\n", line).as_bytes())?;
            }
        }
    }

    if file.as_ref().is_some() {
        file.as_ref().unwrap().sync_all()?;
    }

    Ok(())
}
