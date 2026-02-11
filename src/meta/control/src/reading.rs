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

//! Supporting utilities for reading exported data.

use std::io::BufRead;
use std::io::Lines;
use std::iter::Peekable;

use databend_meta_raft_store::key_spaces::RaftStoreEntry;
use databend_meta_raft_store::ondisk::DATA_VERSION;
use databend_meta_raft_store::ondisk::DataVersion;
use databend_meta_raft_store::ondisk::TREE_HEADER;

/// Import from lines of exported data and Return the max log id that is found.
pub fn validate_version<B: BufRead + 'static>(
    lines: &mut Peekable<Lines<B>>,
) -> anyhow::Result<DataVersion> {
    #[allow(clippy::useless_conversion)]
    let first = lines
        .peek()
        .ok_or_else(|| anyhow::anyhow!("no data found"))?;

    let first_line = match first {
        Ok(l) => l,
        Err(e) => {
            return Err(anyhow::anyhow!("{}", e));
        }
    };

    // First line is the data header that containing version.
    let version = read_version(first_line)?;

    if !DATA_VERSION.is_compatible(version) {
        return Err(anyhow::anyhow!(
            "invalid data version: {:?}, This program version is {:?}; The latest compatible program version is: {:?}",
            version,
            DATA_VERSION,
            version.max_compatible_working_version(),
        ));
    }
    Ok(version)
}

pub fn read_version(first_line: &str) -> anyhow::Result<DataVersion> {
    let (tree_name, kv_entry): (String, RaftStoreEntry) = serde_json::from_str(first_line)?;

    let version = if tree_name == TREE_HEADER {
        // There is a explicit header.
        if let RaftStoreEntry::DataHeader { key, value } = &kv_entry {
            assert_eq!(key, "header", "The key can only be 'header'");
            value.version
        } else {
            unreachable!("The header tree can only contain DataHeader");
        }
    } else {
        // Without header, the data version is V0 by default.
        DataVersion::V0
    };

    Ok(version)
}
