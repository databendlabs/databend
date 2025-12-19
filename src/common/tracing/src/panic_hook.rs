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

use std::panic::PanicHookInfo;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use backtrace::Backtrace;
use backtrace::BacktraceFrame;
use databend_common_base::runtime::LimitMemGuard;
use databend_common_exception::USER_SET_ENABLE_BACKTRACE;
use log::error;

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
    let backtrace_str = backtrace(50);

    eprintln!("{}", panic);
    eprintln!("{}", backtrace_str);

    if let Some(location) = panic.location() {
        error!(
            version = version,
            backtrace = &backtrace_str,
            "panic.file" = location.file(),
            "panic.line" = location.line(),
            "panic.column" = location.column();
            "{}", panic,
        );
    } else {
        error!(version=version, backtrace = backtrace_str; "{}", panic);
    }
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
}
