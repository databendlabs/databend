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

use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::Schema as ArrowSchema;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_settings::FlightCompression;
use futures::future::BoxFuture;

use super::super::wire_format::encode_block_for_thread;
use super::super::wire_format::make_ipc_options;
use super::block_sender::BlockSender;
use super::block_sender::LocalBlockSender;
use super::exchange_send_buffer::ExchangeSendBuffer;

/// Organizes delivery of already-partitioned blocks to one node's threads.
#[derive(Clone)]
pub enum HashShuffleDispatcher {
    Local {
        /// Each thread can be stopped independently; `None` once this writer stops sending.
        senders: Vec<Option<Arc<LocalBlockSender>>>,
    },
    Remote {
        dest_idx: usize,
        /// Release this writer's buffer reference when it stops sending to the node.
        buffer: Option<Arc<ExchangeSendBuffer>>,
        ipc_options: IpcWriteOptions,
        compression: Option<FlightCompression>,
        arrow_schema: Arc<ArrowSchema>,
    },
}

impl HashShuffleDispatcher {
    pub fn local(senders: Vec<Arc<LocalBlockSender>>) -> Self {
        Self::Local {
            senders: senders.into_iter().map(Some).collect(),
        }
    }

    pub fn remote(
        dest_idx: usize,
        buffer: Arc<ExchangeSendBuffer>,
        compression: Option<FlightCompression>,
        arrow_schema: Arc<ArrowSchema>,
    ) -> Result<Self> {
        Ok(Self::Remote {
            dest_idx,
            buffer: Some(buffer),
            ipc_options: make_ipc_options(None)?,
            compression,
            arrow_schema,
        })
    }

    /// Prepare delivery tasks now; the writer schedules and polls them.
    /// Blocks are indexed by destination thread and taken.
    pub fn prepare_send(
        &self,
        node: usize,
        blocks: &mut [Vec<DataBlock>],
        futures: &mut Vec<ShuffleSendFuture>,
    ) -> Result<()> {
        match self {
            Self::Local { senders } => {
                debug_assert_eq!(blocks.len(), senders.len());
                for (thread, blocks) in blocks.iter_mut().enumerate() {
                    let blocks = std::mem::take(blocks);
                    if blocks.is_empty() {
                        continue;
                    }

                    let Some(sender) = &senders[thread] else {
                        continue;
                    };
                    if sender.is_closed() {
                        continue;
                    }

                    let address = ShuffleSendAddress {
                        node,
                        thread: Some(thread),
                    };
                    for block in blocks {
                        let sender = sender.clone();
                        futures.push(Box::pin(async move {
                            (address, sender.send_block(block).await)
                        }));
                    }
                }
                Ok(())
            }
            Self::Remote {
                dest_idx,
                buffer,
                ipc_options,
                compression,
                arrow_schema,
            } => {
                let Some(buffer) = buffer else {
                    return Ok(());
                };
                if buffer.is_closed(*dest_idx) {
                    return Ok(());
                }

                // Encode now, so that only compressed bytes wait for the network.
                let mut items = Vec::with_capacity(blocks.len());
                for (thread, blocks) in blocks.iter_mut().enumerate() {
                    for block in std::mem::take(blocks) {
                        Profile::record_usize_profile(
                            ProfileStatisticsName::ExchangeRows,
                            block.num_rows(),
                        );
                        Profile::record_usize_profile(
                            ProfileStatisticsName::ExchangeBytes,
                            block.memory_size(),
                        );
                        items.push(encode_block_for_thread(
                            thread,
                            block,
                            arrow_schema,
                            ipc_options,
                            *compression,
                        )?);
                    }
                }
                if !items.is_empty() {
                    let buffer = buffer.clone();
                    let dest_idx = *dest_idx;
                    let address = ShuffleSendAddress { node, thread: None };
                    futures.push(Box::pin(async move {
                        (address, buffer.enqueue_items(dest_idx, items).await)
                    }));
                }
                Ok(())
            }
        }
    }

    /// Stop this writer's sends to one local thread, or the whole node for `None`.
    /// Other writers may still send through their shared references. Remote
    /// destinations always release the whole node's reference.
    pub fn stop_sending(&mut self, thread: Option<usize>) {
        // Release closed destinations' shared send buffer references so that
        // other destinations can receive EOF before the graph drops.
        match self {
            Self::Local { senders } => match thread {
                Some(thread) => {
                    senders[thread].take();
                }
                None => {
                    senders.fill(None);
                }
            },
            Self::Remote { buffer, .. } => {
                buffer.take();
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        match self {
            Self::Local { senders } => senders
                .iter()
                .all(|x| x.as_ref().is_none_or(|x| x.is_closed())),
            Self::Remote {
                dest_idx, buffer, ..
            } => buffer
                .as_ref()
                .is_none_or(|buffer| buffer.is_closed(*dest_idx)),
        }
    }

    pub fn closed_partitions(&self, num_partitions: usize) -> usize {
        match self {
            Self::Local { senders } => senders
                .iter()
                .filter(|x| x.as_ref().is_none_or(|x| x.is_closed()))
                .count(),
            Self::Remote {
                dest_idx, buffer, ..
            } => {
                if buffer
                    .as_ref()
                    .is_none_or(|buffer| buffer.is_closed(*dest_idx))
                {
                    num_partitions
                } else {
                    0
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ShuffleSendAddress {
    pub node: usize,
    /// Thread of a local node; remote nodes are sent to as a whole.
    pub thread: Option<usize>,
}

pub type ShuffleSendFuture = BoxFuture<'static, (ShuffleSendAddress, Result<()>)>;
