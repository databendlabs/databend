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

// Name, version, revision, default DB
pub const CLIENT_HELLO: u64 = 0;
// Query id, query settings, stage up to which the query must be executed,
// whether the compression must be used,
// query text (without data for INSERTs).
// A block of data (compressed or not).
pub const CLIENT_QUERY: u64 = 1;
// A block of data (compressed or not).
pub const CLIENT_DATA: u64 = 2;
// Cancel the query execution.
pub const CLIENT_CANCEL: u64 = 3;
// Check that connection to the server is alive.
pub const CLIENT_PING: u64 = 4;
// Check status of tables on the server
pub const CLIENT_TABLES_STATUS_REQUEST: u64 = 5;
// Keep the connection alive
pub const CLIENT_KEEP_ALIVE: u64 = 6;
// A block of data (compressed or not)
pub const CLIENT_SCALAR: u64 = 7;
// List of unique parts ids to exclude from query processing
pub const CLIENT_INGORED_PART_UUIDS: u64 = 8;

pub const SERVER_HELLO: u64 = 0;
pub const SERVER_DATA: u64 = 1;
pub const SERVER_EXCEPTION: u64 = 2;
pub const SERVER_PROGRESS: u64 = 3;
pub const SERVER_PONG: u64 = 4;
pub const SERVER_END_OF_STREAM: u64 = 5;
pub const SERVER_PROFILE_INFO: u64 = 6;
pub const SERVER_TOTALS: u64 = 7;
pub const SERVER_EXTREMES: u64 = 8;

pub const NO_QUERY: u8 = 0;
pub const INITIAL_QUERY: u8 = 1;
pub const SECONDARY_QUERY: u8 = 2;
