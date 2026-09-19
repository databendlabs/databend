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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;

/// Converts an operator's in-memory block representation to and from a
/// transport-safe block representation.
///
/// Local channels deliberately bypass this interface. The outbound side only
/// calls `encode` before Arrow Flight serialization, and the inbound side only
/// calls `decode` after Arrow Flight deserialization. This lets operators keep
/// process-local metadata (for example aggregate payloads) without teaching
/// the network layer about that metadata.
pub trait ExchangeDataCodec: Send + Sync + 'static {
    /// Converts one block before remote serialization. `Ok(None)` discards the
    /// block without sending a packet. Errors propagate to the sender.
    fn encode(&self, block: DataBlock) -> Result<Option<DataBlock>>;

    /// Converts one block after remote deserialization. `Ok(None)` skips this
    /// block, not the rest of the stream. Errors propagate to the receiver.
    fn decode(&self, block: DataBlock) -> Result<Option<DataBlock>>;
}

pub struct DefaultExchangeDataCodec;

impl DefaultExchangeDataCodec {
    pub fn create() -> Arc<dyn ExchangeDataCodec> {
        Arc::new(Self)
    }
}

impl ExchangeDataCodec for DefaultExchangeDataCodec {
    fn encode(&self, block: DataBlock) -> Result<Option<DataBlock>> {
        Ok(Some(block))
    }

    fn decode(&self, block: DataBlock) -> Result<Option<DataBlock>> {
        Ok(Some(block))
    }
}
