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

use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use futures::FutureExt;

pub fn drop_guard<F: FnOnce() -> R, R>(f: F) -> R {
    let panicking = std::thread::panicking();
    #[expect(clippy::disallowed_methods)]
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(res) => res,
        Err(panic) => {
            if panicking {
                eprintln!("double panic");

                let backtrace = std::backtrace::Backtrace::force_capture();
                eprintln!("double panic {:?}", backtrace);
                log::error!("double panic {:?}", backtrace);
            }

            std::panic::resume_unwind(panic)
        }
    }
}

pub fn catch_unwind<F: FnOnce() -> R, R>(f: F) -> Result<R> {
    #[expect(clippy::disallowed_methods)]
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(res) => Ok(res),
        Err(cause) => match cause.downcast_ref::<&'static str>() {
            None => match cause.downcast_ref::<String>() {
                None => Err(ErrorCode::PanicError("Sorry, unknown panic message")),
                Some(message) => Err(ErrorCode::PanicError(message.to_string())),
            },
            Some(message) => Err(ErrorCode::PanicError(message.to_string())),
        },
    }
}

pub struct CatchUnwindFuture<F: Future> {
    inner: Pin<Box<F>>,
}

impl<F: Future> CatchUnwindFuture<F> {
    pub fn create(f: F) -> CatchUnwindFuture<F> {
        CatchUnwindFuture::<F> { inner: Box::pin(f) }
    }
}

impl<F: Future> Future for CatchUnwindFuture<F> {
    type Output = Result<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = &mut self.inner.as_mut();

        match catch_unwind(move || inner.poll_unpin(cx)) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(cause) => Poll::Ready(Err(cause)),
        }
    }
}
