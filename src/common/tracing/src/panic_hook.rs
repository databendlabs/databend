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

use std::panic::PanicInfo;
use std::sync::atomic::Ordering;

use backtrace::trace;
use backtrace::Backtrace;
use backtrace::BacktraceFrame;
use color_backtrace::BacktracePrinter;
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
    let backtrace_str = backtrace(50);

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

fn captures_frames(size: usize) -> Vec<BacktraceFrame> {
    let mut frames = Vec::with_capacity(size);
    trace(|frame| {
        frames.push(backtrace::BacktraceFrame::from(frame.clone()));
        frames.len() != frames.capacity()
    });

    frames
}

pub fn backtrace(frames: usize) -> String {
    if should_backtrace() {
        let frames = captures_frames(frames);
        let mut backtrace = Backtrace::from(frames);
        backtrace.resolve();

        let printer = BacktracePrinter::new()
            .message("")
            .lib_verbosity(color_backtrace::Verbosity::Full);
        let colored = printer
            .format_trace_to_string(&backtrace)
            .unwrap_or_default();
        String::from_utf8_lossy(&strip_ansi_escapes::strip(colored)).into_owned()
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use backtrace::BacktraceFrame;

    use crate::panic_hook::captures_frames;

    #[test]
    fn test_captures_frames() {
        fn recursion_f(i: usize, frames: usize) -> Vec<BacktraceFrame> {
            match i - 1 {
                0 => captures_frames(frames),
                x => recursion_f(x, frames),
            }
        }

        assert_eq!(recursion_f(100, 20).len(), 20);
        assert_eq!(recursion_f(100, 30).len(), 30);
        let frames_size = recursion_f(100, 1000).len();
        assert!(frames_size >= 100);
        assert!(frames_size < 1000);
    }
}
