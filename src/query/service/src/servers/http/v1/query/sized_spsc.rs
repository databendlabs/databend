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

//! A channel bounded with the sum of sizes associate with items instead of count of items.
//!
//! other features:
//! 1. it is SPSC, enough for now.
//! 2. receive can check status of channel with fn is_empty().

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use databend_common_base::base::tokio::sync::Notify;

struct SizedChannelInner<T> {
    max_size: usize,
    values: VecDeque<(T, usize)>,
    is_recv_stopped: bool,
    is_send_stopped: bool,
}

struct Stopped {}

pub fn sized_spsc<T>(max_size: usize) -> (SizedChannelSender<T>, SizedChannelReceiver<T>) {
    let chan = Arc::new(SizedChannel::create(max_size));
    let cloned = chan.clone();
    (SizedChannelSender { chan }, SizedChannelReceiver {
        chan: cloned,
    })
}

impl<T> SizedChannelInner<T> {
    pub fn create(max_size: usize) -> Self {
        SizedChannelInner {
            max_size,
            values: Default::default(),
            is_recv_stopped: false,
            is_send_stopped: false,
        }
    }

    pub fn size(&self) -> usize {
        self.values.iter().map(|x| x.1).sum::<usize>()
    }

    pub fn try_send(&mut self, value: T, size: usize) -> Result<Option<T>, Stopped> {
        let current_size = self.size();
        if self.is_recv_stopped || self.is_send_stopped {
            Err(Stopped {})
        } else if current_size + size <= self.max_size || current_size == 0 {
            self.values.push_back((value, size));
            Ok(None)
        } else {
            Ok(Some(value))
        }
    }

    pub fn try_recv(&mut self) -> Result<Option<T>, Stopped> {
        let v = self.values.pop_front().map(|x| x.0);
        if v.is_none() && self.is_send_stopped {
            Err(Stopped {})
        } else {
            Ok(v)
        }
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty() && self.is_send_stopped
    }

    pub fn stop_send(&mut self) {
        self.is_send_stopped = true
    }

    pub fn stop_recv(&mut self) {
        self.is_recv_stopped = true
    }
}

struct SizedChannel<T> {
    inner: Mutex<SizedChannelInner<T>>,
    notify_on_sent: Notify,
    notify_on_recv: Notify,
}

impl<T> SizedChannel<T> {
    fn create(max_size: usize) -> Self {
        SizedChannel {
            inner: Mutex::new(SizedChannelInner::create(max_size)),
            notify_on_sent: Default::default(),
            notify_on_recv: Default::default(),
        }
    }

    fn try_send(&self, value: T, size: usize) -> Result<Option<T>, Stopped> {
        let mut guard = self.inner.lock().unwrap();
        guard.try_send(value, size)
    }

    pub fn try_recv(&self) -> Result<Option<T>, Stopped> {
        let mut guard = self.inner.lock().unwrap();
        guard.try_recv()
    }

    #[async_backtrace::framed]
    pub async fn send(&self, value: T, size: usize) -> bool {
        let mut to_send = value;
        loop {
            match self.try_send(to_send, size) {
                Ok(Some(v)) => {
                    to_send = v;
                    self.notify_on_recv.notified().await;
                }
                Ok(None) => {
                    self.notify_on_sent.notify_one();
                    return true;
                }
                Err(_) => return false,
            }
        }
    }

    #[async_backtrace::framed]
    pub async fn recv(&self) -> Option<T> {
        loop {
            match self.try_recv() {
                Ok(Some(v)) => {
                    self.notify_on_recv.notify_one();
                    return Some(v);
                }
                Ok(None) => {
                    self.notify_on_sent.notified().await;
                }
                Err(_) => return None,
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        let guard = self.inner.lock().unwrap();
        guard.is_empty()
    }

    pub fn stop_send(&self) {
        {
            let mut guard = self.inner.lock().unwrap();
            guard.stop_send()
        }
        self.notify_on_sent.notify_one();
    }

    pub fn stop_recv(&self) {
        {
            let mut guard = self.inner.lock().unwrap();
            guard.stop_recv()
        }
        self.notify_on_recv.notify_one();
    }
}

pub struct SizedChannelReceiver<T> {
    chan: Arc<SizedChannel<T>>,
}

impl<T> SizedChannelReceiver<T> {
    #[async_backtrace::framed]
    pub async fn recv(&self) -> Option<T> {
        self.chan.recv().await
    }

    pub fn try_recv(&self) -> Option<T> {
        self.chan.try_recv().unwrap_or_default()
    }

    pub fn close(&self) {
        self.chan.stop_recv()
    }

    pub fn is_empty(&self) -> bool {
        self.chan.is_empty()
    }
}

pub struct SizedChannelSender<T> {
    chan: Arc<SizedChannel<T>>,
}

impl<T> SizedChannelSender<T> {
    #[async_backtrace::framed]
    pub async fn send(&self, value: T, size: usize) -> bool {
        self.chan.send(value, size).await
    }

    pub fn close(&self) {
        self.chan.stop_send()
    }

    pub fn closer(&self) -> SizedChannelSenderCloser<T> {
        SizedChannelSenderCloser {
            chan: self.chan.clone(),
        }
    }
}

pub struct SizedChannelSenderCloser<T> {
    chan: Arc<SizedChannel<T>>,
}

impl<T> SizedChannelSenderCloser<T> {
    pub fn close(&self) {
        self.chan.stop_send()
    }
}
