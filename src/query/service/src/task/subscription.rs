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

// Logs from this module will show up as "[PRIVATE-TASKS] ...".
databend_common_tracing::register_module_tag!("[PRIVATE-TASKS]");

use std::future::Future;
use std::time::Duration;

use backon::BackoffBuilder;
use backon::ExponentialBackoff;
use backon::ExponentialBuilder;
use databend_common_exception::Result;
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use log::warn;
use tokio::time::sleep;

/// A subscription that subscribes again, after a delay, whenever subscribing fails or the
/// subscribed stream errors or ends.
///
/// A meta-service watch ends when its connection breaks, and when the meta-service leader
/// changes, since only the leader serves watches.
pub(crate) struct Subscription<S, T> {
    subscribe: S,
    stream: Option<BoxStream<'static, Result<T>>>,
    backoff: ExponentialBuilder,
    /// The delays before the next subscriptions, built anew by `backoff` once one succeeds.
    delays: ExponentialBackoff,
}

impl<S, F, T> Subscription<S, T>
where
    S: FnMut() -> F,
    F: Future<Output = Result<BoxStream<'static, Result<T>>>>,
{
    /// Each delay before subscribing again doubles, with jitter, from `min_delay` up to
    /// `max_delay`, and starts again from `min_delay` once a subscription succeeds.
    pub(crate) fn new(subscribe: S, min_delay: Duration, max_delay: Duration) -> Self {
        let backoff = ExponentialBuilder::new()
            .with_min_delay(min_delay)
            .with_max_delay(max_delay)
            .with_jitter()
            .without_max_times();
        Self {
            subscribe,
            stream: None,
            backoff,
            delays: backoff.build(),
        }
    }

    /// Return the next item of the subscribed stream, subscribing again as often as it takes.
    pub(crate) async fn next(&mut self) -> T {
        loop {
            let mut stream = match self.stream.take() {
                Some(stream) => stream,
                None => match (self.subscribe)().await {
                    Ok(stream) => {
                        self.delays = self.backoff.build();
                        stream
                    }
                    Err(err) => {
                        let delay = self.next_delay();
                        warn!(
                            "failed to subscribe: {}; subscribe again in {:?}",
                            err, delay
                        );
                        sleep(delay).await;
                        continue;
                    }
                },
            };

            let lost = match stream.next().await {
                Some(Ok(item)) => {
                    self.stream = Some(stream);
                    return item;
                }
                Some(Err(err)) => err.to_string(),
                None => "the stream ended".to_string(),
            };
            let delay = self.next_delay();
            warn!(
                "subscription lost: {}; subscribe again in {:?}",
                lost, delay
            );
            sleep(delay).await;
        }
    }

    fn next_delay(&mut self) -> Duration {
        // Built without a maximum of retries: there is always a next delay.
        self.delays.next().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::time::Duration;

    use databend_common_exception::ErrorCode;
    use databend_common_exception::Result;
    use futures_util::StreamExt;
    use futures_util::stream;
    use futures_util::stream::BoxStream;

    use super::Subscription;

    /// The subscription subscribes again when subscribing fails, and when the stream errors or
    /// ends. After an error, it drops the rest of the stream.
    #[tokio::test]
    async fn test_subscribe_again() {
        let mut attempts: VecDeque<Result<BoxStream<'static, Result<u64>>>> = VecDeque::from([
            Ok(stream::iter([Ok(1), Ok(2)]).boxed()),
            Err(ErrorCode::MetaServiceError("refused")),
            Ok(stream::iter([Ok(3), Err(ErrorCode::MetaServiceError("closed")), Ok(10)]).boxed()),
            Ok(stream::iter([Ok(4)]).chain(stream::pending()).boxed()),
        ]);
        let subscribe = move || {
            let attempt = attempts.pop_front().expect("no subscription left");
            async move { attempt }
        };
        let delay = Duration::from_millis(1);
        let mut subscription = Subscription::new(subscribe, delay, delay);

        for expected in [1, 2, 3, 4] {
            assert_eq!(subscription.next().await, expected);
        }
    }
}
