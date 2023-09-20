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

use common_metrics::register_counter;
use common_metrics::Counter;
use lazy_static::lazy_static;
use metrics::counter;

macro_rules! key {
    ($key: literal) => {
        concat!("transform_", $key)
    };
}

lazy_static! {
    static ref EXCHANGE_WRITE_COUNT: Counter = register_counter(key!("exchange_write_count"));
    static ref EXCHANGE_WRITE_BYTES: Counter = register_counter(key!("exchange_write_bytes"));
    static ref EXCHANGE_READ_COUNT: Counter = register_counter(key!("exchange_read_count"));
    static ref EXCHANGE_READ_BYTES: Counter = register_counter(key!("exchange_read_bytes"));
}

pub fn metrics_inc_exchange_write_count(v: usize) {
    counter!(key!("exchange_write_count"), v as u64);
    EXCHANGE_WRITE_COUNT.inc_by(v as u64);
}

pub fn metrics_inc_exchange_write_bytes(c: usize) {
    counter!(key!("exchange_write_bytes"), c as u64);
    EXCHANGE_WRITE_BYTES.inc_by(c as u64);
}

pub fn metrics_inc_exchange_read_count(v: usize) {
    counter!(key!("exchange_read_count"), v as u64);
    EXCHANGE_READ_COUNT.inc_by(v as u64);
}

pub fn metrics_inc_exchange_read_bytes(c: usize) {
    counter!(key!("exchange_read_bytes"), c as u64);
    EXCHANGE_READ_BYTES.inc_by(c as u64);
}
