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

//! Defines meta-store errors and meta-store application errors.
//!
//! ## Error relations
//!
//! The overall relations between errors defined in meta-store components are:
//!
//! ```text
//! 
//!    MetaError
//!    |
//!    +-  MetaStorageError
//!    |
//!    +-  MetaClientError
//!    |   |
//!    |   +- MetaNetworkError
//!    |   +- HandshakeError
//!    |
//!    +-  MetaNetworkError
//!    |
//!    `-  MetaApiError
//!        |
//!        +- ForwardToLeader -.
//!        +- CanNotForward    |
//!        |                   |
//!        +- MetaNetworkError |
//!        |                   |
//!        +- MetaDataError ---|-----------.
//!        |                   |           |
//!        `- RemoteError   ---|-----------+
//!                            |           |
//!                            |           |
//!                            |           |     MetaOperationError
//!                            | .---------|----------'  |
//!                            | |         |  .----------'
//!                            v v         v  v
//!                    ForwardToLeader  MetaDataError
//!                                        |  |
//!                        .---------------'  '---------.
//!                        v                            v
//!           MetaDataReadError                       Raft*Error
//!           |                                       |
//!           +- MetaStorageError                     +- MetaStorageError
//! ```
//!
//! ## Bottom level errors:
//!
//! Bottom level errors are derived from non-meta-store layers.
//!
//! - `MetaStorageError`
//! - `MetaNetworkError`
//! - `MetaBytesError`
//!
//! ```text
//! MetaNetworkError
//! |
//! +- Connection errors
//! +- DNS errors
//! `- Payload codec error
//!
//! MetaStorageError
//! |
//! +- Sled errors
//! +- Txn conflict
//! `- Codec error:--.
//!                  |
//!                  v
//!            MetaBytesError
//! ```
//!
//! ## Application level errors
//!
//! ## `MetaError`
//!
//! `MetaError` defines every error could occur inside a meta-store implementation(an embedded meta-store
//! or a client-server meta-store service).
//! The sub errors are about both a local implementation and a remote meta-store implementation:
//!
//! For a remote meta-store service:
//!
//! - `MetaClientError`: is only returned with remote meta-store service: when creating a client to
//!   meta-store service. Since a client will send a handshake request, `MetaClientError` also
//!   includes a network sub error: `MetaNetworkError`.
//!
//! - `MetaNetworkError` is only returned with remote meta-store service: when sending a request to
//!    a remote meta-store service.
//!
//! - `MetaApiError` is only returned with remote meta-store service: it is all of the errors that
//!   could occur when a meta-store service handling a request.
//!
//! For a local meta-store:
//!
//! - `MetaStorageError`: meta-store is implemented directly upon storage layer.
//!
//! ## `MetaApiError`
//!
//! `MetaApiError` is defined for meta-store service RPC API handler:
//! It includes Raft related errors and errors that occurs when forwarding a request between
//! meta-store servers:
//!
//! - Errors informing request forwarding state:: `ForwardToLeader` or `CanNotForward`.
//! - Errors occurs when forwarding a request: `MetaNetworkError`.
//! - Errors occurs when reading/writing: `MetaDataError`. Because a request may be dealt with
//!   locally or dealt with remotely, via request forwarding, there are two variants for
//!   `MetaDataError`:
//!   - `MetaApiError::MetaDataError(MetaDataError)` is returned when a request is dealt with locally.
//!   - `MetaApiError::RemoteError(MetaDataError)` is returned when a request is dealt with on a
//!     remote noMetaDataError.
//!
//! ## `MetaDataError`
//!
//! It is the error that could occur when dealing with read/write request.
//!
//! In meta-store, data is written through Raft protocol, thus part of its sub errors are Raft
//! related: `RaftWriteError` or `RaftChangeMembershipError`.
//!
//! In meta-store reading data does not depend on Raft protocol, thus read errors is defined as
//! `MetaDataReadError` and is derived directly from MetaStorageError.
//!
//!
//! ## Other errors:
//!
//! - `MetaOperationError` is a intermediate error and a subset error of `MetaApiError` and is
//!   defined for a local request handler without forwarding. It is finally converted to `MetaApiError`.
//!
//! - `ForwardRPCError` is another intermediate error to wrap a result of a forwarded request.

pub mod meta_api_errors;
pub mod meta_client_errors;
pub mod meta_errors;
pub mod meta_handshake_errors;
pub mod meta_management_error;
pub mod meta_network_errors;
pub mod meta_raft_errors;
pub mod meta_startup_errors;
pub mod rpc_errors;

mod incomplete_stream;

pub use incomplete_stream::IncompleteStream;
