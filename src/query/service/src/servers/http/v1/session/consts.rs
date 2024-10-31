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

use std::time::Duration;

/// used for both client session id and refresh token TTL
pub const REFRESH_TOKEN_TTL: Duration = Duration::from_hours(4);

/// used for session token TTL
pub const SESSION_TOKEN_TTL: Duration = Duration::from_hours(1);

/// client start timing for TTL later then meta
pub const TTL_GRACE_PERIOD_META: Duration = Duration::from_secs(300);

/// query server quick check expire_at field in token, which may be set on another server
pub const TTL_GRACE_PERIOD_QUERY: Duration = Duration::from_secs(600);

/// in case of client retry, shorten the TTL instead of drop at once
/// only required for refresh token.
/// e.g. /session/refresh still need the token for auth when retrying.
pub const TOMBSTONE_TTL: Duration = Duration::from_secs(90);
pub const STATE_REFRESH_INTERVAL_META: Duration = Duration::from_secs(300);
pub const STATE_REFRESH_INTERVAL_MEMORY: Duration = Duration::from_secs(60);
