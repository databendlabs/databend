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

use lazy_static::lazy_static;

use crate::register_counter;
use crate::register_gauge;
use crate::Counter;
use crate::Gauge;

lazy_static! {
    static ref SESSION_CONNECT_NUMBERS: Counter = register_counter("session_connect_numbers");
    static ref SESSION_CLOSE_NUMBERS: Counter = register_counter("session_close_numbers");
    static ref SESSION_ACTIVE_CONNECTIONS: Gauge = register_gauge("session_connections");
}

pub fn incr_session_connect_numbers() {
    SESSION_CONNECT_NUMBERS.inc();
}

pub fn incr_session_close_numbers() {
    SESSION_CLOSE_NUMBERS.inc();
}

pub fn set_session_active_connections(num: usize) {
    SESSION_ACTIVE_CONNECTIONS.set(num as i64);
}
