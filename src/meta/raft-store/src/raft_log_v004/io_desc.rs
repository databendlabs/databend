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

use databend_common_meta_types::anyerror::AnyError;
use databend_common_meta_types::raft_types::ErrorSubject;
use databend_common_meta_types::raft_types::ErrorVerb;
use databend_common_meta_types::raft_types::StorageError;
use log::debug;
use log::error;

use crate::raft_log_v004::io_phase::IOPhase;

/// Describe an IO operation.
pub struct IODesc {
    pub subject: ErrorSubject,
    pub verb: ErrorVerb,
    pub ctx: String,
}

impl fmt::Display for IODesc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: ({}-{:?})", self.ctx, self.verb, self.subject)
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
        }
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
