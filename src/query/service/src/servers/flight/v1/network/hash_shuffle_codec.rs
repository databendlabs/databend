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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;

/// Lets the producer of a global hash shuffle exchange blocks in its own
/// in-memory form, paying for conversion only where blocks cross the network.
///
/// Blocks for the local node are delivered unchanged, and blocks for a remote
/// node are encoded before the wire format and decoded by the receiving node.
pub trait HashShuffleCodec: Send + Sync + 'static {
    /// Convert a block bound for a remote node into one the wire format can
    /// send: its meta is serializable, and its columns follow the exchange
    /// schema unless it has no rows.
    fn encode(&self, block: DataBlock) -> Result<DataBlock>;

    /// Restore a block produced by `encode` on a remote node.
    fn decode(&self, block: DataBlock) -> Result<DataBlock>;
}
