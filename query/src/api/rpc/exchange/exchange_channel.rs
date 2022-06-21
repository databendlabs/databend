// Copyright 2022 Datafuse Labs.
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

use std::ops::Deref;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::atomic::Ordering::Acquire;
use std::sync::Arc;

use async_channel::Sender;
use common_arrow::arrow_format::flight::data::FlightData;
use common_exception::Result;

use crate::api::rpc::packet::DataPacket;

// Different from async_channel::Sender
// It is allowed to close the channel when has one reference.
// In other words, we only record the number of cloned.
pub struct FragmentSender {
    is_track: bool,
    tx: Sender<DataPacket>,
    ref_count: Arc<AtomicUsize>,
}

impl FragmentSender {
    pub fn create_unrecorded(tx: Sender<DataPacket>) -> FragmentSender {
        FragmentSender {
            tx,
            is_track: false,
            ref_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Deref for FragmentSender {
    type Target = Sender<DataPacket>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl Clone for FragmentSender {
    fn clone(&self) -> Self {
        self.ref_count.fetch_add(1, Ordering::Relaxed);
        FragmentSender {
            is_track: true,
            tx: self.tx.clone(),
            ref_count: self.ref_count.clone(),
        }
    }
}

impl Drop for FragmentSender {
    fn drop(&mut self) {
        if self.is_track && 1 == self.ref_count.fetch_sub(1, Ordering::AcqRel) {
            std::sync::atomic::fence(Acquire);
            self.tx.close();
        }
    }
}

// Different from async_channel::Sender
// It is allowed to close the channel when has one reference.
// In other words, we only record the number of cloned.
pub struct FragmentReceiver {
    is_track: bool,
    tx: Sender<Result<FlightData>>,
    ref_count: Arc<AtomicUsize>,
}

impl FragmentReceiver {
    pub fn create_unrecorded(tx: Sender<Result<FlightData>>) -> FragmentReceiver {
        FragmentReceiver {
            tx,
            is_track: false,
            ref_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Deref for FragmentReceiver {
    type Target = Sender<Result<FlightData>>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl Clone for FragmentReceiver {
    fn clone(&self) -> Self {
        self.ref_count.fetch_add(1, Ordering::Relaxed);
        FragmentReceiver {
            is_track: true,
            tx: self.tx.clone(),
            ref_count: self.ref_count.clone(),
        }
    }
}

impl Drop for FragmentReceiver {
    fn drop(&mut self) {
        if self.is_track && 1 == self.ref_count.fetch_sub(1, Ordering::AcqRel) {
            std::sync::atomic::fence(Acquire);
            self.tx.close();
        }
    }
}
