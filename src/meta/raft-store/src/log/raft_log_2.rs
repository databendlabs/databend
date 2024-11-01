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
use std::io;
use std::ops::Deref;
use std::sync::mpsc::SyncSender;

use databend_common_meta_types::anyerror::AnyError;
use databend_common_meta_types::raft_types;
use databend_common_meta_types::raft_types::ErrorSubject;
use databend_common_meta_types::raft_types::ErrorVerb;
use databend_common_meta_types::raft_types::LogId;
use databend_common_meta_types::raft_types::StorageError;
use deepsize::DeepSizeOf;
use log::debug;
use log::error;
use log::info;
use log::warn;
use raft_log::api::raft_log_writer::RaftLogWriter;
use raft_log::codeq::OffsetWriter;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::oneshot;

use crate::key_spaces::RaftStoreEntry;
use crate::log::codec_wrapper::Cw;
use crate::state::RaftStateKey;
use crate::state_machine::LogMetaKey;

#[derive(PartialEq, Eq, Default, Clone, Debug)]
pub struct RaftLogTypes;

#[derive(Debug)]
pub enum IOPhase {
    Submit,
    SubmitFlush,
    AwaitFlush,
    ReceiveFlushCallback,
    Done,
}

impl fmt::Display for IOPhase {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IOPhase::Submit => write!(f, "submit io"),
            IOPhase::SubmitFlush => write!(f, "submit flush"),
            IOPhase::AwaitFlush => write!(f, "await flush"),
            IOPhase::ReceiveFlushCallback => write!(f, "receive flush callback"),
            IOPhase::Done => write!(f, "done"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("RaftLogIOError: {verb}-{subject:?}: {ctx}: failed to {phase}; error: {error}")]
pub struct RaftLogIOError {
    pub subject: ErrorSubject,
    pub verb: ErrorVerb,
    pub phase: IOPhase,
    pub error: io::Error,
    pub ctx: String,
}

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

pub struct Callback {
    context: String,
    data: CallbackData,
}

impl Callback {
    pub fn new_io_flushed(io_flushed: raft_types::IOFlushed, context: impl ToString) -> Self {
        Callback {
            context: context.to_string(),
            data: CallbackData::IOFlushed(io_flushed),
        }
    }

    pub fn new_oneshot(tx: oneshot::Sender<Result<(), io::Error>>, context: impl ToString) -> Self {
        Callback {
            context: context.to_string(),
            data: CallbackData::Oneshot(tx),
        }
    }

    pub fn new_sync_oneshot(tx: SyncSender<Result<(), io::Error>>, context: impl ToString) -> Self {
        Callback {
            context: context.to_string(),
            data: CallbackData::SyncOneshot(tx),
        }
    }
}

pub enum CallbackData {
    Oneshot(oneshot::Sender<Result<(), io::Error>>),
    SyncOneshot(SyncSender<Result<(), io::Error>>),
    IOFlushed(raft_types::IOFlushed),
}

impl raft_log::Callback for Callback {
    fn send(self, res: Result<(), io::Error>) {
        info!("{}: Callback is called with: {:?}", self.context, res);

        match self.data {
            CallbackData::Oneshot(tx) => {
                let send_res = tx.send(res);
                if send_res.is_err() {
                    warn!(
                        "{}: Callback failed to send Oneshot result back to caller",
                        self.context
                    );
                }
            }
            CallbackData::SyncOneshot(tx) => tx.send(res),
            CallbackData::IOFlushed(io_flushed) => io_flushed.io_completed(res),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogStoreMeta {
    pub node_id: Option<raft_types::NodeId>,
}

impl raft_log::codeq::Encode for LogStoreMeta {
    fn encode<W: io::Write>(&self, mut w: W) -> Result<usize, io::Error> {
        let mut ow = OffsetWriter::new(&mut w);

        rmp_serde::encode::write_named(&mut ow, &self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let n = ow.offset();

        Ok(n)
    }
}

impl raft_log::codeq::Decode for LogStoreMeta {
    fn decode<R: io::Read>(r: R) -> Result<Self, io::Error> {
        let d = rmp_serde::decode::from_read(r)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(d)
    }
}

impl raft_log::Types for RaftLogTypes {
    type LogId = Cw<raft_types::LogId>;
    type LogPayload = Cw<raft_types::EntryPayload>;
    type Vote = Cw<raft_types::Vote>;
    type Callback = Callback;
    type UserData = LogStoreMeta;

    fn log_index(log_id: &Self::LogId) -> u64 {
        log_id.index
    }

    fn payload_size(payload: &Self::LogPayload) -> u64 {
        let size = match payload.deref() {
            raft_types::EntryPayload::Blank => 0,
            raft_types::EntryPayload::Normal(log_entry) => log_entry.deep_size_of(),
            raft_types::EntryPayload::Membership(_) => size_of::<raft_types::Membership>(),
        };

        size as u64
    }
}

pub type RaftLog2 = raft_log::RaftLog<RaftLogTypes>;
pub type RaftLogConfig = raft_log::Config;

pub async fn blocking_flush(rl: &mut RaftLog2) -> Result<(), io::Error> {
    let (tx, rx) = oneshot::channel();
    let callback = Callback::new_oneshot(tx, "blocking_flush");

    rl.flush(callback)?;
    rx.await.map_err(|_e| {
        io::Error::new(io::ErrorKind::Other, "Failed to receive flush completion")
    })??;
    Ok(())
}

pub struct Importer {
    pub raft_log: RaftLog2,
    pub max_log_id: Option<LogId>,
}

impl Importer {
    pub fn new(raft_log: RaftLog2) -> Self {
        Importer {
            raft_log,
            max_log_id: None,
        }
    }

    pub async fn flush(mut self) -> Result<RaftLog2, io::Error> {
        blocking_flush(&mut self.raft_log).await?;
        Ok(self.raft_log)
    }

    pub fn import_raft_store_entry(&mut self, entry: RaftStoreEntry) -> Result<(), io::Error> {
        match entry {
            RaftStoreEntry::DataHeader { .. } => {
                // V004 RaftLog does not store DataHeader
            }
            RaftStoreEntry::Logs { key: _, value } => {
                let (log_id, payload) = (value.log_id, value.payload);

                self.raft_log.append([(Cw(log_id), Cw(payload))])?;
                self.max_log_id = std::cmp::max(self.max_log_id, Some(value.log_id));
            }
            RaftStoreEntry::RaftStateKV { key, value } => match key {
                RaftStateKey::Id => {
                    self.raft_log.save_user_data(Some(LogStoreMeta {
                        node_id: Some(value.node_id()),
                    }))?;
                }
                RaftStateKey::HardState => {
                    self.raft_log.save_vote(Cw(value.vote()))?;
                }
                RaftStateKey::StateMachineId => {
                    unreachable!("StateMachineId is removed");
                }
                RaftStateKey::Committed => {
                    if let Some(value) = value.committed() {
                        self.raft_log.commit(Cw(value))?;
                    }
                }
            },
            RaftStoreEntry::LogMeta { key, value } => match key {
                LogMetaKey::LastPurged => {
                    let purged = value.log_id();
                    self.raft_log.purge(Cw(purged))?;
                }
            },

            //////////////////////////////////////////////////////////////////
            RaftStoreEntry::StateMachineMeta { .. } => {
                unreachable!("StateMachineMeta should be written to snapshot");
            }
            RaftStoreEntry::Nodes { .. } => {
                unreachable!("Nodes should be written to snapshot");
            }
            RaftStoreEntry::Expire { .. } => {
                unreachable!("Expire should be written to snapshot");
            }
            RaftStoreEntry::GenericKV { .. } => {
                unreachable!("GenericKV should be written to snapshot");
            }
            RaftStoreEntry::Sequences { .. } => {
                unreachable!("Sequences should be written to snapshot");
            }
            RaftStoreEntry::ClientLastResps { .. } => {
                unreachable!("ClientLastResps should be written to snapshot");
            }
        }

        Ok(())
    }
}
