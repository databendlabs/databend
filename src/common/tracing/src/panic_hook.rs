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

use std::backtrace::Backtrace;
use std::panic::PanicInfo;
use std::sync::atomic::Ordering;

use databend_common_base::runtime::LimitMemGuard;
use databend_common_exception::USER_SET_ENABLE_BACKTRACE;
use log::error;

pub fn set_panic_hook() {
    // Set a panic hook that records the panic as a `tracing` event at the
    // `ERROR` verbosity level.
    //
    // If we are currently in a span when the panic occurred, the logged event
    // will include the current span, allowing the context in which the panic
    // occurred to be recorded.
    std::panic::set_hook(Box::new(|panic| {
        let _guard = LimitMemGuard::enter_unlimited();
        log_panic(panic);
    }));
}

fn should_backtrace() -> bool {
    // if user not specify or user set to enable, we should backtrace
    match USER_SET_ENABLE_BACKTRACE.load(Ordering::Relaxed) {
        0 => true,
        1 => false,
        _ => true,
    }
}

pub fn log_panic(panic: &PanicInfo) {
    let backtrace_str = if should_backtrace() {
        let backtrace = Backtrace::force_capture();
        format!("{:?}", backtrace)
    } else {
        String::new()
    };

    eprintln!("{}", panic);
    eprintln!("{}", backtrace_str);

    if let Some(location) = panic.location() {
        error!(
            backtrace = &backtrace_str,
            "panic.file" = location.file(),
            "panic.line" = location.line(),
            "panic.column" = location.column();
            "{}", panic,
        );
    } else {
        error!(backtrace = backtrace_str; "{}", panic);
    }
}
