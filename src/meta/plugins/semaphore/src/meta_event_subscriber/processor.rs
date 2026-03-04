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

use codeq::Decode;
use databend_meta_types::SeqV;
use databend_meta_types::protobuf::WatchResponse;
use display_more::DisplaySliceExt;
use log::warn;
use tokio::sync::mpsc;

use crate::PermitEntry;
use crate::PermitKey;
#[cfg(doc)]
use crate::acquirer::Acquirer;
use crate::errors::AcquirerClosed;
use crate::errors::ConnectionClosed;
use crate::errors::ProcessorError;
use crate::queue::PermitEvent;
use crate::queue::SemaphoreQueue;

/// Process the watch response from the meta-service and emit [`PermitEvent`].
pub(crate) struct Processor {
    /// The local in-memory queue of [`PermitEvent`], indexed and sorted by the [`PermitKey`],
    /// which contains prefix in string and a sequence number.
    pub(crate) queue: SemaphoreQueue,

    /// The channel to send the [`PermitEvent`] to the [`Acquirer`].
    ///
    /// When the [`Acquirer`] acquired a [`Permit`], this channel is no longer used,
    /// because the receiving end may not be polled and sending to it may block forever.
    pub(crate) tx_to_acquirer: mpsc::Sender<Result<PermitEvent, ConnectionClosed>>,

    /// Contains descriptive information about the context of this watcher.
    pub(crate) watcher_name: String,
}

impl Processor {
    /// Create a new [`Processor`] instance with given permits capacity and the channel to send the [`PermitEvent`] to the [`Acquirer`].
    pub(crate) fn new(
        capacity: u64,
        tx_to_acquirer: mpsc::Sender<Result<PermitEvent, ConnectionClosed>>,
    ) -> Self {
        Self {
            queue: SemaphoreQueue::new(capacity),
            tx_to_acquirer,
            watcher_name: "".to_string(),
        }
    }

    /// Set the debugging context of this watcher.
    pub(crate) fn with_watcher_name(mut self, watcher_name: impl ToString) -> Self {
        self.watcher_name = watcher_name.to_string();
        self
    }

    /// Parse the watch response, feed key-value changes to local queue, emit semaphore events.
    pub(crate) async fn process_watch_response(
        &mut self,
        watch_response: WatchResponse,
    ) -> Result<(), ProcessorError> {
        let Some((key, prev, current)) =
            Self::decode_watch_response(watch_response, &self.watcher_name)?
        else {
            return Ok(());
        };

        self.process_kv_change(key, prev, current).await?;

        Ok(())
    }

    async fn process_kv_change(
        &mut self,
        sem_key: PermitKey,
        prev: Option<SeqV<PermitEntry>>,
        current: Option<SeqV<PermitEntry>>,
    ) -> Result<(), AcquirerClosed> {
        log::debug!(
            "{} processing kv change: {}: {:?} -> {:?}",
            self.watcher_name,
            sem_key,
            prev,
            current
        );
        // Update local queue to update the acquired/released state.
        let state_changes = match (prev, current) {
            (None, Some(entry)) => self.queue.insert(sem_key.seq, entry.data),
            (Some(_entry), None) => self.queue.remove(sem_key.seq),
            (Some(_), Some(_)) => {
                // An event of updating the lease.
                vec![]
            }
            (None, None) => {
                // both prev and current are None. not possible but just ignore
                warn!("semaphore loop: both prev and current are None");
                vec![]
            }
        };

        log::info!(
            "{} queue state: {}; new events: {}",
            self.watcher_name,
            self.queue,
            state_changes.display()
        );

        for event in state_changes {
            log::debug!("{} sending event: {}", self.watcher_name, event);

            self.tx_to_acquirer.send(Ok(event)).await.map_err(|e| {
                AcquirerClosed::new(format!("Semaphore-Watcher fail to send {}", e.0.unwrap()))
                    .context(&self.watcher_name)
            })?;
        }

        Ok(())
    }

    /// Decode the serialized key-value back to [`PermitKey`] and [`PermitEntry`].
    ///
    /// Returns a tuple of `(key, value_before, value_after)` where:
    /// - `key` is the decoded [`PermitKey`]
    /// - `value_before` is the previous state of the entry (if it existed)
    /// - `value_after` is the current state of the entry (if it exists)
    ///
    /// Returns `None` if the watch response contains no events.
    #[allow(clippy::type_complexity)]
    fn decode_watch_response(
        watch_response: WatchResponse,
        ctx: &str,
    ) -> Result<
        Option<(
            PermitKey,
            Option<SeqV<PermitEntry>>,
            Option<SeqV<PermitEntry>>,
        )>,
        ConnectionClosed,
    > {
        let Some((sem_key, prev, current)) = watch_response.unpack() else {
            warn!("unexpected WatchResponse with empty event",);
            return Ok(None);
        };

        let sem_key = PermitKey::parse_key(&sem_key).map_err(|x| {
            ConnectionClosed::new_io_error(x)
                .context(format!("parse semaphore key: {}", sem_key))
                .context(ctx)
        })?;

        let prev = Self::decode_option_seqv(prev)
            .map_err(|e| e.context("decode prev seqv").context(ctx))?;

        let current = Self::decode_option_seqv(current)
            .map_err(|e| e.context("decode current seqv").context(ctx))?;

        Ok(Some((sem_key, prev, current)))
    }

    fn decode_option_seqv(
        seqv: Option<SeqV>,
    ) -> Result<Option<SeqV<PermitEntry>>, ConnectionClosed> {
        match seqv {
            Some(seqv) => {
                let seqv = seqv.try_map(|data| PermitEntry::decode(&mut data.as_slice()))?;
                Ok(Some(seqv))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {

    use codeq::Encode;
    use pretty_assertions::assert_eq;

    use super::*;

    #[tokio::test]
    async fn test_process_kv_change() -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::channel(100);
        let mut processor = Processor::new(10, tx);

        let events = vec![
            (key(1), None, ent(5)),
            (key(2), None, ent(4)),
            (key(3), None, ent(3)),
            (key(4), None, ent(2)),
            (key(2), ent(4), None), // size = 10
            (key(5), None, ent(6)), // waiting
            (key(1), ent(5), None), // size = 5
            (key(6), None, ent(1)), // size = 5
            (key(4), ent(2), None), // size = 10
        ];

        for (sem_key, prev, current) in events {
            // fake seqv.seq
            let prev = prev.map(|x| SeqV::new(1, x));
            let current = current.map(|x| SeqV::new(2, x));
            processor.process_kv_change(sem_key, prev, current).await?;
        }

        drop(processor);

        // Drain the rx channel and collect into a Vec
        let mut received_events = Vec::new();
        while let Some(event) = rx.recv().await {
            received_events.push(event);
        }

        assert_eq!(
            received_events
                .into_iter()
                .map(|x| x.unwrap())
                .collect::<Vec<_>>(),
            vec![
                acquired(1, 5),
                acquired(2, 4),
                removed(2, 4),
                acquired(3, 3),
                acquired(4, 2), // size = 10
                removed(1, 5),
                removed(4, 2),  // size = 3
                acquired(5, 6), // size = 9
                acquired(6, 1), // size = 10
            ]
        );

        Ok(())
    }

    #[test]
    fn test_decode_watch_response() -> anyhow::Result<()> {
        // Non-empty watch response.

        let sem_key = PermitKey::new("test_key", 1);
        let prev = PermitEntry::new("a", 1);
        let current = PermitEntry::new("b", 2);

        let watch_response = WatchResponse::new_change_event(
            sem_key.format_key(),
            Some(SeqV::new(1, prev.encode_to_vec()?)),
            Some(SeqV::new(2, current.encode_to_vec()?)),
        );

        let got = Processor::decode_watch_response(watch_response, "test")?;
        let got = got.unwrap();

        assert_eq!(got.0, sem_key);
        assert_eq!(got.1, Some(SeqV::new(1, prev)));
        assert_eq!(got.2, Some(SeqV::new(2, current)));

        // Empty watch response.

        let watch_response = WatchResponse::default();
        let got = Processor::decode_watch_response(watch_response, "test")?;
        assert_eq!(got, None);

        Ok(())
    }

    fn key(seq: u64) -> PermitKey {
        PermitKey::new("foo".to_string(), seq)
    }

    /// Create a semaphore entry for testing.
    fn ent(value: u64) -> Option<PermitEntry> {
        Some(PermitEntry::new(format!("id-{}", value), value))
    }

    /// Create an acquired event.
    fn acquired(seq: u64, entry: u64) -> PermitEvent {
        PermitEvent::new_acquired(seq, ent(entry).unwrap())
    }

    /// Create a removed event.
    fn removed(seq: u64, entry: u64) -> PermitEvent {
        PermitEvent::new_removed(seq, ent(entry).unwrap())
    }
}
