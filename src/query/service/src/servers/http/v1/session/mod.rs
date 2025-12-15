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

// Logs from this module will show up as "[HTTP-SESSION] ...".
databend_common_tracing::register_module_tag!("[HTTP-SESSION]");

mod client_session_manager;
mod consts;
pub mod login_handler;
pub(crate) mod logout_handler;
pub mod refresh_handler;
mod token;

pub use client_session_manager::ClientSessionManager;
pub(crate) use token::SessionClaim;
pub(crate) use token::unix_ts;
