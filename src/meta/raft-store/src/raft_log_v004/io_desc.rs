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

use std::error::Error;
use std::fmt;
use std::time::Duration;
use std::time::SystemTime;

use databend_common_meta_types::anyerror::AnyError;
use databend_common_meta_types::raft_types::ErrorSubject;
use databend_common_meta_types::raft_types::ErrorVerb;
use databend_common_meta_types::raft_types::StorageError;
use display_more::DisplayUnixTimeStampExt;
use log::debug;
use log::error;

use crate::raft_log_v004::io_phase::IOPhase;

/// Describe an IO operation.
#[derive(Clone)]
pub struct IODesc {
    pub subject: ErrorSubject,
    pub verb: ErrorVerb,
    pub ctx: String,

    /// Unix timestamp when this IO operation started.
    pub start_time: Duration,

    /// Unix timestamp when the flush request is sent.
    pub flush_time: Option<Duration>,

    /// Unix timestamp when the flush request is completed.
    pub flush_done_time: Option<Duration>,

    /// Unix timestamp when the IO operation is done.
    pub done_time: Option<Duration>,
}

impl fmt::Display for IODesc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IODesc({}): ({}-{:?})",
            self.ctx, self.verb, self.subject
        )?;

        let start_time = self.start_time;
        write!(f, "[start: {}", start_time.display_unix_timestamp_short())?;

        if let Some(flush_time) = self.flush_time {
            let elapsed = flush_time.saturating_sub(start_time);
            write!(f, ", flush: +{:?}", elapsed,)?;

            if let Some(flushed_time) = self.flush_done_time {
                let elapsed = flushed_time.saturating_sub(flush_time);
                write!(f, ", flushed: +{:?}", elapsed,)?;
            }
        }

        if let Some(done_time) = self.done_time {
            let elapsed = done_time.saturating_sub(self.start_time);
            write!(f, ", total: {:?}", elapsed)?;
        }

        write!(f, "]")
    }
}

impl IODesc {
    pub fn start(subject: ErrorSubject, verb: ErrorVerb, ctx: impl ToString) -> Self {
        let s = ctx.to_string();
        debug!("{}: ({}-{:?}): start", s, verb, subject);
        IODesc {
            subject,
            verb,
            ctx: s,
            start_time: Self::since_epoch(),
            flush_time: None,
            flush_done_time: None,
            done_time: None,
        }
    }

    pub fn set_flush_time(&mut self) {
        self.flush_time = Some(Self::since_epoch());
    }

    pub fn set_flush_done_time(&mut self) {
        self.flush_done_time = Some(Self::since_epoch());
    }

    pub fn set_done_time(&mut self) {
        self.done_time = Some(Self::since_epoch());
    }

    pub fn total_duration(&self) -> Option<Duration> {
        self.flush_done_time
            .map(|flushed_time| flushed_time.saturating_sub(self.start_time))
    }

    fn since_epoch() -> Duration {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
    }

    pub fn unknown(ctx: impl ToString) -> Self {
        Self::start(ErrorSubject::None, ErrorVerb::Write, ctx)
    }

    pub fn save_vote(ctx: impl ToString) -> Self {
        Self::start(ErrorSubject::Vote, ErrorVerb::Write, ctx)
    }

    pub fn save_committed(ctx: impl ToString) -> Self {
        Self::start(ErrorSubject::Store, ErrorVerb::Write, ctx)
    }

    pub fn append(ctx: impl ToString) -> Self {
        Self::start(ErrorSubject::Logs, ErrorVerb::Write, ctx)
    }

    pub fn truncate(ctx: impl ToString) -> Self {
        Self::start(ErrorSubject::Logs, ErrorVerb::Write, ctx)
    }

    pub fn purge(ctx: impl ToString) -> Self {
        Self::start(ErrorSubject::Logs, ErrorVerb::Write, ctx)
    }

    pub fn read_logs(ctx: impl ToString) -> Self {
        Self::start(ErrorSubject::Logs, ErrorVerb::Read, ctx)
    }

    pub fn to_storage_error(&self, phase: IOPhase, err: impl Error + 'static) -> StorageError {
        error!("{}: failed to {}: error: {}", self, phase, err);

        StorageError::new(
            self.subject.clone(),
            self.verb,
            AnyError::from(&err).add_context(|| self.ctx.clone()),
        )
    }

    fn ok_message(&self, phase: IOPhase) -> String {
        format!("{}: successfully {}", self, phase)
    }

    pub fn ok_submit(&self) -> String {
        self.ok_message(IOPhase::Submit)
    }

    pub fn ok_submit_flush(&self) -> String {
        self.ok_message(IOPhase::SubmitFlush)
    }

    pub fn ok_done(&self) -> String {
        self.ok_message(IOPhase::Done)
    }

    pub fn err_submit(&self, err: impl Error + 'static) -> StorageError {
        self.to_storage_error(IOPhase::Submit, err)
    }

    pub fn err_submit_flush(&self, err: impl Error + 'static) -> StorageError {
        self.to_storage_error(IOPhase::SubmitFlush, err)
    }

    pub fn err_await_flush(&self, err: impl Error + 'static) -> StorageError {
        self.to_storage_error(IOPhase::AwaitFlush, err)
    }

    pub fn err_recv_flush_cb(&self, err: impl Error + 'static) -> StorageError {
        self.to_storage_error(IOPhase::ReceiveFlushCallback, err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display() {
        let now_us = 1754708580036171;

        let mut desc = IODesc::start(ErrorSubject::Logs, ErrorVerb::Write, "test");
        assert!(desc.start_time.as_secs() > 0);
        desc.start_time = Duration::from_micros(now_us);

        assert_eq!(
            format!("{}", desc),
            "IODesc(test): (Write-Logs)[start: 2025-08-09T03:03:00.036]"
        );

        desc.flush_time = Some(Duration::from_micros(now_us + 1_000_000));
        assert_eq!(
            format!("{}", desc),
            "IODesc(test): (Write-Logs)[start: 2025-08-09T03:03:00.036, flush: +1s]"
        );

        desc.flush_done_time = Some(Duration::from_micros(now_us + 2_000_000));
        assert_eq!(
            format!("{}", desc),
            "IODesc(test): (Write-Logs)[start: 2025-08-09T03:03:00.036, flush: +1s, flushed: +1s]"
        );

        desc.done_time = Some(Duration::from_micros(now_us + 3_000_000));
        assert_eq!(
            format!("{}", desc),
            "IODesc(test): (Write-Logs)[start: 2025-08-09T03:03:00.036, flush: +1s, flushed: +1s, total: 3s]"
        );
    }
}
