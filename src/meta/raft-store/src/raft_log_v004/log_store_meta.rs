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

use std::fmt;
use std::io;

use databend_common_meta_types::raft_types;
use raft_log::codeq::OffsetWriter;
use serde::Deserialize;
use serde::Serialize;

/// Stores non-Raft data in the log store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogStoreMeta {
    pub node_id: Option<raft_types::NodeId>,
}

impl fmt::Display for LogStoreMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LogStoreMeta{{ node_id: {:?} }}", self.node_id)
    }
}

impl raft_log::codeq::Encode for LogStoreMeta {
    fn encode<W: io::Write>(&self, mut w: W) -> Result<usize, io::Error> {
        let mut ow = OffsetWriter::new(&mut w);

        rmp_serde::encode::write_named(&mut ow, &self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let n = ow.offset();

        Ok(n)
    }
}

impl raft_log::codeq::Decode for LogStoreMeta {
    fn decode<R: io::Read>(r: R) -> Result<Self, io::Error> {
        let d = rmp_serde::decode::from_read(r)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(d)
    }
}
