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

use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use databend_common_exception::Result;
use futures::Stream;
#[cfg(not(target_os = "windows"))]
use tokio::signal::unix::signal;
#[cfg(not(target_os = "windows"))]
use tokio::signal::unix::Signal;
#[cfg(not(target_os = "windows"))]
use tokio::signal::unix::SignalKind;
#[cfg(target_os = "windows")]
use tokio::signal::windows::ctrl_c;
#[cfg(target_os = "windows")]
use tokio::signal::windows::CtrlC;

pub enum SignalType {
    Hangup,
    Sigint,
    Sigterm,
    Exit,
}

pub type SignalStream = Pin<Box<dyn Stream<Item = SignalType> + Send>>;

pub struct DummySignalStream {
    signal_type: SignalType,
}

impl DummySignalStream {
    pub fn create(signal_type: SignalType) -> SignalStream {
        Box::pin(DummySignalStream { signal_type })
    }
}

impl Stream for DummySignalStream {
    type Item = SignalType;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.signal_type {
            SignalType::Hangup => Poll::Ready(Some(SignalType::Hangup)),
            SignalType::Sigint => Poll::Ready(Some(SignalType::Sigint)),
            SignalType::Sigterm => Poll::Ready(Some(SignalType::Sigterm)),
            SignalType::Exit => Poll::Ready(Some(SignalType::Exit)),
        }
    }
}

#[cfg(not(target_os = "windows"))]
pub fn signal_stream() -> Result<SignalStream> {
    Ok(Box::pin(UnixShutdownSignalStream {
        hangup_signal: signal(SignalKind::hangup())?,
        sigint_signal: signal(SignalKind::interrupt())?,
        sigterm_signal: signal(SignalKind::terminate())?,
    }))
}

#[cfg(target_os = "windows")]
pub fn signal_stream() -> Result<SignalStream> {
    Ok(Box::pin(WindowsShutdownSignalStream { ctrl_c: ctrl_c()? }))
}

#[cfg(not(target_os = "windows"))]
struct UnixShutdownSignalStream {
    hangup_signal: Signal,
    sigint_signal: Signal,
    sigterm_signal: Signal,
}

#[cfg(target_os = "windows")]
struct WindowsShutdownSignalStream {
    ctrl_c: CtrlC,
}

#[cfg(not(target_os = "windows"))]
impl Stream for UnixShutdownSignalStream {
    type Item = SignalType;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut_self = self.get_mut();
        if let Poll::Ready(res) = mut_self.hangup_signal.poll_recv(cx) {
            return Poll::Ready(res.map(|_| SignalType::Hangup));
        }

        if let Poll::Ready(res) = mut_self.sigint_signal.poll_recv(cx) {
            return Poll::Ready(res.map(|_| SignalType::Sigint));
        }

        if let Poll::Ready(res) = mut_self.sigterm_signal.poll_recv(cx) {
            return Poll::Ready(res.map(|_| SignalType::Sigterm));
        }

        Poll::Pending
    }
}

#[cfg(target_os = "windows")]
impl Stream for WindowsShutdownSignalStream {
    type Item = SignalType;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .ctrl_c
            .poll_recv(cx)
            .map(|ready| ready.map(|_| SignalType::Sigint))
    }
}
