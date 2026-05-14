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

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

use async_channel::Receiver;
use async_channel::Sender;
use databend_common_expression::DataBlock;

#[derive(Default)]
pub struct BroadcastChannel {
    pub source_sender: Option<Sender<DataBlock>>,
    pub source_receiver: Option<Receiver<DataBlock>>,
    pub sink_sender: Option<Sender<DataBlock>>,
    pub sink_receiver: Option<Receiver<DataBlock>>,
}

#[derive(Default)]
pub struct BroadcastRegistry {
    next_id: AtomicU32,
    channels: Mutex<HashMap<u32, BroadcastChannel>>,
}

impl BroadcastRegistry {
    pub fn next_broadcast_id(&self) -> u32 {
        self.next_id.fetch_add(1, Ordering::Acquire)
    }

    pub fn reset_broadcast_id(&self) {
        self.next_id.store(0, Ordering::Release);
    }

    pub fn source_receiver(&self, broadcast_id: u32) -> Receiver<DataBlock> {
        let mut channels = self.channels.lock().unwrap();
        let entry = channels.entry(broadcast_id).or_default();
        match entry.source_receiver.take() {
            Some(receiver) => receiver,
            None => {
                let (sender, receiver) = async_channel::unbounded();
                entry.source_sender = Some(sender);
                receiver
            }
        }
    }

    pub fn source_sender(&self, broadcast_id: u32) -> Sender<DataBlock> {
        let mut channels = self.channels.lock().unwrap();
        let entry = channels.entry(broadcast_id).or_default();
        match entry.source_sender.take() {
            Some(sender) => sender,
            None => {
                let (sender, receiver) = async_channel::unbounded();
                entry.source_receiver = Some(receiver);
                sender
            }
        }
    }

    pub fn sink_receiver(&self, broadcast_id: u32) -> Receiver<DataBlock> {
        let mut channels = self.channels.lock().unwrap();
        let entry = channels.entry(broadcast_id).or_default();
        match entry.sink_receiver.take() {
            Some(receiver) => receiver,
            None => {
                let (sender, receiver) = async_channel::unbounded();
                entry.sink_sender = Some(sender);
                receiver
            }
        }
    }

    pub fn sink_sender(&self, broadcast_id: u32) -> Sender<DataBlock> {
        let mut channels = self.channels.lock().unwrap();
        let entry = channels.entry(broadcast_id).or_default();
        match entry.sink_sender.take() {
            Some(sender) => sender,
            None => {
                let (sender, receiver) = async_channel::unbounded();
                entry.sink_receiver = Some(receiver);
                sender
            }
        }
    }
}
