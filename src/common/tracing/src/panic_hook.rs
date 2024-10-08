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

use std::fmt::Write;
use std::panic::PanicHookInfo;
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

pub fn log_panic(panic: &PanicHookInfo) {
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

pub fn captures_frames(size: usize) -> Vec<(String, String, u32)> {
    let mut frames = Vec::with_capacity(size);

    backtrace::trace(|frame| {
        backtrace::resolve_frame(frame, |symbol| {
            let mut n = String::from("<unknown>");
            let mut f = String::from("<unknown file>");
            let mut l = 0;
            let mut skip = false;
            if let Some(name) = symbol.name() {
                n = format!("{}", name);
                #[allow(unused_mut)]
                let mut only_skip_in_release = false;

                #[cfg(not(debug_assertions))]
                if n.starts_with("databend_common_tracing::") {
                    only_skip_in_release = true;
                }

                if only_skip_in_release
                    || n == "rust_begin_unwind"
                    || n == "__rust_try"
                    || n.starts_with("core::panicking::")
                    || n.starts_with("std::panicking::")
                    || n.starts_with("std::sys_common::backtrace::__rust_end_short_backtrace")
                    || n.starts_with("std::sys_common::backtrace::__rust_begin_short_backtrace::")
                    || n.starts_with("std::rt::")
                    || n.starts_with("tokio::")
                    || n.starts_with("<tokio::")
                    || n.starts_with("std::thread::local::")
                    || n.starts_with("<core::panic::unwind_safe::")
                    || n.starts_with("<core::pin::Pin<P> as core::future::")
                    || n.starts_with("std::panic::")
                    || n.starts_with("core::ops::function::FnOnce")
                    || n.starts_with("backtrace::backtrace::")
                {
                    skip = true;
                }
            }
            if let Some(filename) = symbol.filename() {
                f = filename.display().to_string();
            }
            if let Some(line_no) = symbol.lineno() {
                l = line_no;
            }

            if !skip {
                frames.push((n, f, l));
            }
        });

        // keep going to the next frame, except if the current function is 'main'
        !matches!(frames.last().map(|(s, _, _)| s.as_str()), Some("main")) && frames.len() != size
    });

    frames
}

pub fn backtrace(frames: usize) -> String {
    if should_backtrace() {
        let mut message = String::new();

        for (idx, (name, file, location)) in captures_frames(frames).into_iter().enumerate() {
            let has_hash_suffix = name.len() > 19
                && &name[name.len() - 19..name.len() - 16] == "::h"
                && name[name.len() - 16..]
                    .chars()
                    .all(|x| x.is_ascii_hexdigit());

            match has_hash_suffix {
                true => writeln!(&mut message, "{:4}: {}", idx, &name[..name.len() - 19]),
                false => writeln!(&mut message, "{:4}: {}", idx, name),
            }
            .unwrap();

            writeln!(&mut message, "             at {}:{}", file, location).unwrap();
        }

        message
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::panic_hook::captures_frames;

    #[test]
    fn test_captures_frames() {
        fn recursion_f(i: usize, frames: usize) -> Vec<(String, String, u32)> {
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
