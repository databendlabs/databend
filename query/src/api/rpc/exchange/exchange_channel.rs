use std::ops::Deref;
use async_channel::Sender;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::atomic::Ordering::Acquire;
use common_arrow::arrow_format::flight::data::FlightData;
use crate::api::rpc::packet::DataPacket;
use common_exception::Result;

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
        if self.is_track {
            if 1 == self.ref_count.fetch_sub(1, Ordering::AcqRel) {
                std::sync::atomic::fence(Acquire);
                self.tx.close();
            }
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
        if self.is_track {
            if 1 == self.ref_count.fetch_sub(1, Ordering::AcqRel) {
                std::sync::atomic::fence(Acquire);
                self.tx.close();
            }
        }
    }
}

