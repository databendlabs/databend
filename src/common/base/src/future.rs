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

use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use log::debug;
use log::info;
use pin_project_lite::pin_project;
use tokio::time::Instant;

pin_project! {
    /// A [`Future`] that tracks the time spent on a future.
    /// When the future is ready, the callback will be called with the total time and busy time.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct TimedFuture<'a, T, F>
    where F: Fn(Duration, Duration) ,
    F: 'a,
    {
        #[pin]
        inner: T,

        start: Option<Instant>,
        busy: Duration,
        callback: F,
        _p : PhantomData<&'a ()>,

    }
}

impl<'a, T, F> TimedFuture<'a, T, F>
where
    F: Fn(Duration, Duration),
    F: 'a,
{
    pub fn new(inner: T, callback: F) -> Self {
        Self {
            inner,
            start: None,
            busy: Duration::default(),
            callback,
            _p: Default::default(),
        }
    }
}

impl<'a, T: Future, F> Future for TimedFuture<'a, T, F>
where
    F: Fn(Duration, Duration),
    F: 'a,
{
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        if this.start.is_none() {
            *this.start = Some(Instant::now());
        }

        let t0 = Instant::now();

        let res = this.inner.poll(cx);

        *this.busy += t0.elapsed();

        if res.is_ready() {
            let total = this.start.unwrap().elapsed();
            (this.callback)(total, *this.busy);
        }

        res
    }
}

/// Enable timing for a future with `fu.with_timing(f)`.
pub trait TimedFutureExt {
    /// Wrap the future with a timing future.
    fn with_timing<'a, F>(self, f: F) -> TimedFuture<'a, Self, F>
    where
        F: Fn(Duration, Duration) + 'a,
        Self: Future + Sized;

    /// Wrap the future with a timing future,
    /// and call the callback if the total time exceeds the threshold.
    fn with_timing_threshold<'a, F>(
        self,
        threshold: Duration,
        f: F,
    ) -> TimedFuture<'a, Self, impl Fn(Duration, Duration)>
    where
        F: Fn(Duration, Duration) + 'a,
        Self: Future + Sized,
    {
        self.with_timing::<'a>(move |total, busy| {
            if total >= threshold {
                f(total, busy)
            }
        })
    }

    /// Log elapsed time(total and busy) in DEBUG level when the future is ready.
    fn log_elapsed_debug<'a>(
        self,
        ctx: impl fmt::Display + 'a,
    ) -> TimedFuture<'a, Self, impl Fn(Duration, Duration)>
    where
        Self: Future + Sized,
    {
        self.with_timing::<'a>(move |total, busy| {
            debug!("Elapsed: total: {:?}, busy: {:?}; {}", total, busy, ctx);
        })
    }

    /// Log elapsed time(total and busy) in info level when the future is ready.
    fn log_elapsed_info<'a>(
        self,
        ctx: impl fmt::Display + 'a,
    ) -> TimedFuture<'a, Self, impl Fn(Duration, Duration)>
    where
        Self: Future + Sized,
    {
        self.with_timing::<'a>(move |total, busy| {
            info!("Elapsed: total: {:?}, busy: {:?}; {}", total, busy, ctx);
        })
    }
}

impl<T> TimedFutureExt for T
where T: Future + Sized
{
    fn with_timing<'a, F>(self, f: F) -> TimedFuture<'a, Self, F>
    where
        F: Fn(Duration, Duration),
        F: 'a,
    {
        TimedFuture::new(self, f)
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;
    use std::time::Duration;

    use crate::future::TimedFuture;
    use crate::future::TimedFutureExt;

    struct BlockingSleep20ms {}

    impl Future for BlockingSleep20ms {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            std::thread::sleep(Duration::from_millis(20));
            Poll::Ready(())
        }
    }

    #[test]
    fn test_timing_future() -> anyhow::Result<()> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        // Blocking sleep

        let f = BlockingSleep20ms {};
        let f = TimedFuture::new(f, |total, busy| {
            // println!("total: {:?}, busy: {:?}", total, busy);
            assert!(total >= Duration::from_millis(20));
            assert!(total <= Duration::from_millis(50));

            assert!(busy >= Duration::from_millis(20));
            assert!(busy <= Duration::from_millis(50));
        });

        rt.block_on(f);

        // Async sleep

        let f = async move { tokio::time::sleep(Duration::from_millis(20)).await };
        let f = TimedFuture::new(f, |total, busy| {
            // println!("total: {:?}, busy: {:?}", total, busy);
            assert!(total >= Duration::from_millis(20));
            assert!(total <= Duration::from_millis(50));

            assert!(busy <= Duration::from_millis(10));
        });

        rt.block_on(f);

        Ok(())
    }

    #[test]
    fn test_time_future_ext() -> anyhow::Result<()> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        // Blocking sleep

        let f = BlockingSleep20ms {}.with_timing(|total, busy| {
            assert!(total >= Duration::from_millis(20));
            assert!(total <= Duration::from_millis(50));

            assert!(busy >= Duration::from_millis(20));
            assert!(busy <= Duration::from_millis(50));
        });

        rt.block_on(f);

        rt.block_on(BlockingSleep20ms {}.with_timing_threshold(
            Duration::from_millis(10),
            |_total, _busy| {
                // OK, triggered
            },
        ));
        rt.block_on(
            BlockingSleep20ms {}
                .with_timing_threshold(Duration::from_millis(100), |_total, _busy| {
                    unreachable!("should not be called")
                }),
        );

        Ok(())
    }
}
