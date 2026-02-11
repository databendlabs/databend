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

#![allow(
    clippy::cloned_ref_to_slice_refs,
    clippy::collapsible_if,
    clippy::iter_kv_map,
    clippy::uninlined_format_args
)]

extern crate core;

mod jwt;
mod network_policy;
mod password_policy;
mod role_mgr;
mod user;
mod user_api;
mod user_mgr;
mod user_stage;
mod user_udf;
mod visibility_checker;

pub mod builtin;
pub mod connection;
pub mod file_format;
pub mod role_cache_mgr;
pub mod role_util;

pub use databend_common_meta_app::principal::BUILTIN_ROLE_ACCOUNT_ADMIN;
pub use databend_common_meta_app::principal::BUILTIN_ROLE_PUBLIC;
pub use jwt::*;
pub use password_policy::*;
pub use role_cache_mgr::RoleCacheManager;
pub use user::CertifiedInfo;
pub use user_api::UserApiProvider;
pub use visibility_checker::GrantObjectVisibilityChecker;
pub use visibility_checker::Object;
pub use visibility_checker::check_table_visibility_with_roles;
pub use visibility_checker::has_table_name_grants;
pub use visibility_checker::is_role_owner;
pub use visibility_checker::is_table_visible_with_owner;

/// Convert a meta service error to an ErrorCode.
pub(crate) fn meta_service_error(
    e: databend_meta_types::MetaError,
) -> databend_common_exception::ErrorCode {
    databend_common_exception::ErrorCode::MetaServiceError(e.to_string())
}
