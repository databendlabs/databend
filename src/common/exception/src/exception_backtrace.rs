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

// use std::backtrace::Backtrace;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::exception::ErrorCodeBacktrace;

// 0: not specified 1: disable 2: enable
pub static USER_SET_ENABLE_BACKTRACE: AtomicUsize = AtomicUsize::new(0);

pub fn set_backtrace(switch: bool) {
    if switch {
        USER_SET_ENABLE_BACKTRACE.store(2, Ordering::Relaxed);
    } else {
        USER_SET_ENABLE_BACKTRACE.store(1, Ordering::Relaxed);
    }
}


fn enable_rust_backtrace() -> bool {
    match USER_SET_ENABLE_BACKTRACE.load(Ordering::Relaxed) {
        0 => {}
        1 => return false,
        _ => return true,
    }

    let enabled = match std::env::var("RUST_LIB_BACKTRACE") {
        Ok(s) => s != "0",
        Err(_) => match std::env::var("RUST_BACKTRACE") {
            Ok(s) => s != "0",
            Err(_) => false,
        },
    };

    USER_SET_ENABLE_BACKTRACE.store(enabled as usize + 1, Ordering::Relaxed);
    enabled
}

enum BacktraceStyle {
    Symbols,
    Address,
}

fn backtrace_style() -> BacktraceStyle {
    static ENABLED: AtomicUsize = AtomicUsize::new(0);
    match ENABLED.load(Ordering::Relaxed) {
        1 => return BacktraceStyle::Address,
        2 => return BacktraceStyle::Symbols,
        _ => {}
    }

    let backtrace_style = match std::env::var("BACKTRACE_STYLE") {
        Ok(style) if style.eq_ignore_ascii_case("ADDRESS") => 1,
        _ => 2,
    };

    ENABLED.store(backtrace_style, Ordering::Relaxed);
    match backtrace_style {
        1 => BacktraceStyle::Address,
        _ => BacktraceStyle::Symbols,
    }
}

pub fn capture() -> Option<ErrorCodeBacktrace> {
    match enable_rust_backtrace() {
        false => None,
        true => match backtrace_style() {
            BacktraceStyle::Symbols => Some(ErrorCodeBacktrace::Symbols(Arc::new(
                backtrace::Backtrace::new(),
            ))),
            // TODO: get offset address(https://github.com/rust-lang/backtrace-rs/issues/434)
            BacktraceStyle::Address => Some(ErrorCodeBacktrace::Address(Arc::new(
                backtrace::Backtrace::new_unresolved(),
            ))),
        },
    }
}
