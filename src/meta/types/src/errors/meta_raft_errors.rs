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

pub use openraft::error::ChangeMembershipError;
pub use openraft::error::EmptyMembership;
pub use openraft::error::InProgress;
pub use openraft::error::InitializeError;

use crate::raft_types::ClientWriteError;
use crate::MetaDataError;
use crate::MetaOperationError;
use crate::RaftError;

// Collection of errors that occur when change membership on local raft node.
pub type RaftChangeMembershipError = ClientWriteError;

impl From<RaftChangeMembershipError> for MetaOperationError {
    fn from(e: RaftChangeMembershipError) -> Self {
        match e {
            RaftChangeMembershipError::ForwardToLeader(to_leader) => to_leader.into(),
            // TODO: change-membership-error is not a data error.
            RaftChangeMembershipError::ChangeMembershipError(c) => Self::DataError(c.into()),
        }
    }
}

impl From<RaftError<ClientWriteError>> for MetaOperationError {
    fn from(e: RaftError<ClientWriteError>) -> Self {
        match e {
            RaftError::APIError(cli_write_err) => cli_write_err.into(),
            RaftError::Fatal(f) => Self::DataError(MetaDataError::WriteError(f)),
        }
    }
}
