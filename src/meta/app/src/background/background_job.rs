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
use std::fmt::Display;
use std::fmt::Formatter;

use chrono::{DateTime};
use chrono::Utc;
use crate::background::BackgroundTaskType;
use crate::principal::UserIdentity;

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
    num_derive::FromPrimitive,
)]
pub enum BackgroundJobState {
    #[default]
    RUNNING = 0,
    FAILED = 1,
    SUSPENDED = 2,
}

impl Display for BackgroundJobState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BackgroundJobState::RUNNING => write!(f, "RUNNING"),
            BackgroundJobState::FAILED => write!(f, "FAILED"),
            BackgroundJobState::SUSPENDED => write!(f, "SUSPENDED"),
        }
    }
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
    num_derive::FromPrimitive,
)]
pub enum BackgroundJobType {
    #[default]
    ONESHOT = 0,
    INTERVAL = 1,
    CRON = 2,
}

impl Display for BackgroundJobType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BackgroundJobType::ONESHOT => write!(f, "ONESHOT"),
            BackgroundJobType::INTERVAL => write!(f, "INTERVAL"),
            BackgroundJobType::CRON => write!(f, "CRON"),
        }
    }
}

// Ident
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct BackgroundJobIdent {
    // The user this job belongs to
    pub tenant: String,

    pub name: String,
}

// Info
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct BackgroundJobInfo {

    pub job_type: BackgroundJobType,
    pub job_state: BackgroundJobState,
    pub task_type: BackgroundTaskType,

    pub last_updated: Option<DateTime<Utc>>,
    pub message: String,

    // Audit
    pub creator: Option<UserIdentity>,
    pub created_at: DateTime<Utc>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct BackgroundJobId {
    pub id: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateBackgroundJobReq {
    pub if_not_exists: bool,
    pub job_name: BackgroundJobIdent,
    pub job_info: BackgroundJobInfo,
}

impl Display for CreateBackgroundJobReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "create_background_job({}, {}, {}, {}, {}, {:?})",
            self.job_name.name, self.job_info.task_type, self.job_info.job_type, self.job_info.job_state, self.job_info.message, self.job_info.last_updated
        )
    }
}


#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateBackgroundJobReply {
    pub id: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetBackgroundJobReq {
    pub name: BackgroundJobIdent,
}

impl Display for GetBackgroundJobReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "get_background_job({})", self.name.name)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetBackgroundJobReply {
    pub id: u64,
    pub info: BackgroundJobInfo,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UpdateBackgroundJobReq {
    pub job_name: BackgroundJobIdent,
    pub info: BackgroundJobInfo,
}

impl Display for UpdateBackgroundJobReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "update_background_job({}, {}, {}, {}, {}, {:?})",
            self.job_name.name, self.info.task_type, self.info.job_type, self.info.job_state, self.info.message, self.info.last_updated
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UpdateBackgroundJobReply {
    pub id: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DeleteBackgroundJobReq {
    pub name: BackgroundJobIdent,
}

impl Display for DeleteBackgroundJobReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "delete_background_job({})", self.name.name)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DeleteBackgroundJobReply {}
// list
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ListBackgroundJobsReq {
    pub tenant: String,
}

impl Display for ListBackgroundJobsReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "list_background_job({})", self.tenant)
    }
}

mod kvapi_key_impl {
    use common_meta_kvapi::kvapi;
    use crate::background::background_job::{BackgroundJobId, BackgroundJobIdent};
    const PREFIX_BACKGROUND_JOB: &str = "__fd_background_job";
    const PREFIX_BACKGROUND_JOB_BY_ID: &str = "__fd_background_job_by_id";

    /// <prefix>/<tenant>/<background_job_ident> -> <id>
    impl kvapi::Key for BackgroundJobIdent {
        const PREFIX: &'static str = PREFIX_BACKGROUND_JOB;

        fn to_string_key(&self) -> String {
            kvapi::KeyBuilder::new_prefixed(Self::PREFIX)
                .push_str(&self.tenant)
                .push_str(&self.name)
                .done()
        }

        fn from_str_key(s: &str) -> Result<Self, kvapi::KeyError> {
            let mut p = kvapi::KeyParser::new_prefixed(s, Self::PREFIX)?;

            let tenant = p.next_str()?;
            let name = p.next_str()?;
            p.done()?;

            Ok(BackgroundJobIdent { tenant, name })
        }
    }

    impl kvapi::Key for BackgroundJobId {
        const PREFIX: &'static str = PREFIX_BACKGROUND_JOB_BY_ID;

        fn to_string_key(&self) -> String {
            kvapi::KeyBuilder::new_prefixed(Self::PREFIX)
                .push_u64(self.id)
                .done()
        }

        fn from_str_key(s: &str) -> Result<Self, kvapi::KeyError> {
            let mut p = kvapi::KeyParser::new_prefixed(s, Self::PREFIX)?;

            let id = p.next_u64()?;
            p.done()?;

            Ok(BackgroundJobId { id })
        }
    }
}