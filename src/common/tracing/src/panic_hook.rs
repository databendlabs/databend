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

use std::borrow::Cow;
use std::panic::PanicHookInfo;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use backtrace::Backtrace;
use backtrace::BacktraceFrame;
use databend_common_base::runtime::LimitMemGuard;
use databend_common_exception::USER_SET_ENABLE_BACKTRACE;
use log::error;

const MEMORY_BACKTRACE_SKIPPED: &str =
    "Backtrace skipped because the panic was caused by memory allocation failure";

pub fn set_panic_hook(version: String) {
    // Set a panic hook that records the panic as a `tracing` event at the
    // `ERROR` verbosity level.
    //
    // If we are currently in a span when the panic occurred, the logged event
    // will include the current span, allowing the context in which the panic
    // occurred to be recorded.
    let version = Arc::new(version);
    std::panic::set_hook(Box::new(move |panic| {
        let _guard = LimitMemGuard::enter_unlimited();
        log_panic(panic, version.clone());
    }));
}

#[allow(dead_code)]
fn should_backtrace() -> bool {
    // if user not specify or user set to enable, we should backtrace
    match USER_SET_ENABLE_BACKTRACE.load(Ordering::Relaxed) {
        0 => true,
        1 => false,
        _ => true,
    }
}

pub fn log_panic(panic: &PanicHookInfo, version: Arc<String>) {
    let backtrace_str = if is_memory_allocation_panic(panic) {
        Cow::Borrowed(MEMORY_BACKTRACE_SKIPPED)
    } else {
        Cow::Owned(backtrace(50))
    };

    eprintln!("{}", panic);
    eprintln!("{}", backtrace_str);

    if let Some(location) = panic.location() {
        error!(
            version = version,
            backtrace = backtrace_str.as_ref(),
            "panic.file" = location.file(),
            "panic.line" = location.line(),
            "panic.column" = location.column();
            "{}", panic,
        );
    } else {
        error!(version=version, backtrace = backtrace_str.as_ref(); "{}", panic);
    }
}

fn is_memory_allocation_panic(panic: &PanicHookInfo) -> bool {
    panic_message(panic).is_some_and(is_memory_allocation_message)
}

fn panic_message<'a>(panic: &'a PanicHookInfo<'a>) -> Option<&'a str> {
    panic
        .payload()
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.payload().downcast_ref::<String>().map(String::as_str))
}

fn is_memory_allocation_message(message: &str) -> bool {
    (message.starts_with("memory allocation of ") && message.ends_with(" bytes failed"))
        || (message.starts_with("memory usage ") && message.contains(" exceeds limit "))
}

pub fn captures_frames(frames: &mut Vec<BacktraceFrame>) {
    backtrace::trace(|frame| {
        frames.push(BacktraceFrame::from(frame.clone()));
        frames.len() != frames.capacity()
    });
}

pub fn backtrace(frames: usize) -> String {
    let mut frames = Vec::with_capacity(frames);
    captures_frames(&mut frames);
    let mut backtrace = Backtrace::from(frames);
    backtrace.resolve();
    format!("{:?}", backtrace)
}

#[cfg(test)]
mod tests {
    use backtrace::BacktraceFrame;

    use crate::panic_hook::captures_frames;
    use crate::panic_hook::is_memory_allocation_message;

    #[test]
    fn test_captures_frames() {
        fn recursion_f(i: usize, frames: usize) -> Vec<BacktraceFrame> {
            match i - 1 {
                0 => {
                    let mut frames = Vec::with_capacity(frames);
                    captures_frames(&mut frames);
                    frames
                }
                x => recursion_f(x, frames),
            }
        }

        assert_eq!(recursion_f(100, 20).len(), 20);
        assert_eq!(recursion_f(100, 30).len(), 30);
        let frames_size = recursion_f(100, 1000).len();
        assert!(frames_size >= 100);
        assert!(frames_size < 1000);
    }

    #[test]
    fn test_memory_allocation_message_detection() {
        assert!(is_memory_allocation_message(
            "memory allocation of 4096 bytes failed"
        ));
        assert!(is_memory_allocation_message(
            "memory usage 2.00 GiB(2147483648) exceeds limit 1.00 GiB(1073741824)"
        ));
        assert!(!is_memory_allocation_message(
            "called `Option::unwrap()` on a `None` value"
        ));
    }
}
