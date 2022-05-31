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

//! Provides conversion from and to protobuf defined meta data, which is used for transport.
//!
//! Thus protobuf messages has the maximized compatibility.
//! I.e., a protobuf message is able to contain several different versions of metadata in one format.
//! This mod will convert protobuf message to the current version of meta data used in databend-query.

use std::cell::RefCell;

use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Utc;

thread_local! {
    static MOCK_UTC: RefCell<Option<DateTime<Utc>>> = RefCell::new(None);
}

pub fn utc_now() -> DateTime<Utc> {
    if cfg!(feature = "mock_utc") {
        MOCK_UTC.with(|cell| cell.borrow().as_ref().cloned().unwrap_or_else(Utc::now))
    } else {
        Utc::now()
    }
}

pub fn set_utc_mock_time(time: DateTime<Utc>) {
    if cfg!(feature = "mock_utc") {
        MOCK_UTC.with(|cell| *cell.borrow_mut() = Some(time));
    }
}

pub fn clear_utc_mock_time() {
    if cfg!(feature = "mock_utc") {
        MOCK_UTC.with(|cell| *cell.borrow_mut() = None);
    }
}
