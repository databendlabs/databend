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

//! Receive a snapshot binary format from a remote node.
use std::fmt;
use std::fs::File;
use std::io;
use std::io::BufWriter;
use std::io::Write;

use databend_common_meta_types::protobuf::SnapshotChunkRequestV003;
use databend_common_meta_types::SnapshotMeta;
use databend_common_meta_types::Vote;
use log::debug;
use log::info;

pub struct Receiver {
    remote_addr: String,

    temp_path: String,

    temp_file: Option<BufWriter<File>>,

    /// Callback function when receiving a chunk.
    on_recv: Option<Box<dyn Fn(u64) + Send>>,

    /// number of bytes received.
    n_received: usize,

    /// number of bytes received.
    size_received: usize,
}

impl Receiver {
    /// Create a new snapshot receiver with an empty snapshot.
    pub(crate) fn new(remote_addr: impl ToString, temp_path: impl ToString, temp_f: File) -> Self {
        let remote_addr = remote_addr.to_string();
        info!("Begin receiving snapshot v2 stream from: {}", remote_addr);

        Receiver {
            remote_addr,
            temp_path: temp_path.to_string(),
            temp_file: Some(BufWriter::with_capacity(64 * 1024 * 1024, temp_f)),
            on_recv: None,
            n_received: 0,
            size_received: 0,
        }
    }

    pub fn set_on_recv_callback(&mut self, f: impl Fn(u64) + Send + 'static) {
        self.on_recv = Some(Box::new(f));
    }

    pub fn stat_str(&self) -> impl fmt::Display {
        format!(
            "received {} chunks, {} bytes from {}",
            self.n_received, self.size_received, self.remote_addr
        )
    }

    pub fn receive(
        &mut self,
        chunk: SnapshotChunkRequestV003,
    ) -> Result<Option<(String, Vote, SnapshotMeta, String /* path */)>, io::Error> {
        let remote_addr = self.remote_addr.clone();
        let temp_path = self.temp_path.clone();

        fn invalid_input<E>(e: E) -> io::Error
        where E: Into<Box<dyn std::error::Error + Send + Sync>> {
            io::Error::new(io::ErrorKind::InvalidInput, e)
        }

        // Add context info to io::Error
        let ctx = |e: io::Error, context: &str| -> io::Error {
            io::Error::new(
                e.kind(),
                format!(
                    "{} while:(Receiver::receive(): {}; remote_addr: {}; temp_path: {})",
                    e, context, remote_addr, temp_path
                ),
            )
        };

        // 1. update stat
        self.update_stat(&chunk);

        // 2. write chunk to local snapshot_data
        {
            let f = self.temp_file.as_mut().ok_or_else(|| {
                ctx(
                    invalid_input("snapshot_data is already shutdown"),
                    "take self.temp_file",
                )
            })?;

            f.write_all(&chunk.chunk)
                .map_err(|e| ctx(e, "write_chunk to temp_file"))?;
        }

        // 3. if it is the last chunk, finish and return the snapshot.
        {
            let end = self
                .load_finish_chunk(&chunk)
                .map_err(|e| ctx(invalid_input(e), "loading last chunk rpc_meta"))?;

            let Some((format, vote, snapshot_meta)) = end else {
                return Ok(None);
            };

            info!(
                "snapshot from {} is completely received, format: {}, vote: {:?}, meta: {:?}, size: {}; path: {}",
                self.remote_addr, format, vote, snapshot_meta, self.size_received, self.temp_path
            );

            if format != "rotbl::v001" {
                return Err(ctx(
                    invalid_input(format!(
                        "unsupported snapshot format: {}, expect: rotbl::v001",
                        format
                    )),
                    "check input format",
                ));
            }

            // Safe unwrap: snapshot_data is guaranteed to be Some in the above code.
            let mut buf_f = self.temp_file.take().unwrap();

            let f: Result<File, io::Error> = try {
                buf_f
                    .flush()
                    .map_err(|e| ctx(e, "flushing local temp snapshot_data"))?;

                let f = buf_f
                    .into_inner()
                    .map_err(|e| ctx(e.into_error(), "getting inner file"))?;
                f
            };

            f?.sync_all()
                .map_err(|e| ctx(e, "sync_all() for temp snapshot"))?;

            Ok(Some((format, vote, snapshot_meta, self.temp_path.clone())))
        }
    }

    fn update_stat(&mut self, chunk: &SnapshotChunkRequestV003) {
        let data_len = chunk.chunk.len();
        self.n_received += 1;
        self.size_received += data_len;

        debug!(
            len = data_len,
            total_len = self.size_received;
            "received {}-th snapshot chunk from {}; path: {}",
            self.n_received, self.remote_addr, self.temp_path
        );

        if self.n_received % 100 == 0 {
            info!(
                total_len = self.size_received;
                "received {}-th snapshot chunk from {}; path: {}",
                self.n_received, self.remote_addr, self.temp_path
            );
        }

        if let Some(f) = &self.on_recv {
            f(data_len as u64);
        }
    }

    /// Load meta data from the last chunk.
    fn load_finish_chunk(
        &self,
        chunk: &SnapshotChunkRequestV003,
    ) -> Result<Option<(String, Vote, SnapshotMeta)>, serde_json::Error> {
        let Some(meta) = &chunk.rpc_meta else {
            return Ok(None);
        };

        let (format, vote, snapshot_meta): (String, Vote, SnapshotMeta) =
            serde_json::from_str(meta)?;

        Ok(Some((format, vote, snapshot_meta)))
    }
}
