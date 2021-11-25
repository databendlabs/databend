// Copyright 2021 Datafuse Labs.
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

mod protocol_exception;
mod protocol_hello;
mod protocol_query;
mod protocol_type;

pub use protocol_exception::*;
pub use protocol_hello::*;
pub use protocol_query::*;
pub use protocol_type::*;

use crate::types::Block;

#[derive(Debug)]
pub enum Packet {
    Ping,
    Cancel,
    Hello(HelloRequest),
    Query(QueryRequest),
    Data(Block),
}

#[derive(Debug)]
pub enum Stage {
    Default = 0,
    InsertPrepare,
    InsertStarted,
    EOS,
}
impl Default for Stage {
    fn default() -> Self {
        Stage::Default
    }
}

pub const DBMS_MIN_REVISION_WITH_CLIENT_INFO: u64 = 54032;
pub const DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE: u64 = 54058;
pub const DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO: u64 = 54060;
pub const DBMS_MIN_REVISION_WITH_TABLES_STATUS: u64 = 54226;
pub const DBMS_MIN_REVISION_WITH_TIME_ZONE_PARAMETER_IN_DATETIME_DATA_TYPE: u64 = 54337;
pub const DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME: u64 = 54372;
pub const DBMS_MIN_REVISION_WITH_VERSION_PATCH: u64 = 54401;
pub const DBMS_MIN_REVISION_WITH_SERVER_LOGS: u64 = 54406;
pub const DBMS_MIN_REVISION_WITH_CLIENT_SUPPORT_EMBEDDED_DATA: u64 = 54415;
// Minimum revision with exactly the same set of aggregation methods and rules to select them.
// Two-level (bucketed) aggregation is incompatible if servers are inconsistent in these rules
// (keys will be placed in different buckets and result will not be fully aggregated).
pub const DBMS_MIN_REVISION_WITH_CURRENT_AGGREGATION_VARIANT_SELECTION_METHOD: u64 = 54431;
pub const DBMS_MIN_REVISION_WITH_COLUMN_DEFAULTS_METADATA: u64 = 54410;

pub const DBMS_MIN_REVISION_WITH_LOW_CARDINALITY_TYPE: u64 = 54405;
pub const DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO: u64 = 54420;

// Minimum revision supporting SettingsBinaryFormat::STRINGS.
pub const DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS: u64 = 54429;

// Minimum revision supporting OpenTelemetry
pub const DBMS_MIN_REVISION_WITH_OPENTELEMETRY: u64 = 54442;

// Minimum revision supporting interserver secret.
pub const DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET: u64 = 54441;

pub const DBMS_MIN_REVISION_WITH_X_FORWARDED_FOR_IN_CLIENT_INFO: u64 = 54443;
pub const DBMS_MIN_REVISION_WITH_REFERER_IN_CLIENT_INFO: u64 = 54447;
