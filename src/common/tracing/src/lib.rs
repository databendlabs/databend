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

#![feature(try_blocks)]
#![allow(clippy::uninlined_format_args)]

mod config;
mod init;
mod loggers;
mod panic_hook;

pub use crate::config::Config;
pub use crate::config::FileConfig;
pub use crate::config::OTLPConfig;
pub use crate::config::ProfileLogConfig;
pub use crate::config::QueryLogConfig;
pub use crate::config::StderrConfig;
pub use crate::config::TracingConfig;
pub use crate::init::init_logging;
pub use crate::init::inject_span_to_tonic_request;
pub use crate::init::start_trace_for_remote_request;
pub use crate::init::GlobalLogger;
pub use crate::panic_hook::log_panic;
pub use crate::panic_hook::set_panic_hook;

pub fn closure_name<F: std::any::Any>() -> &'static str {
    let full_name = std::any::type_name::<F>();
    full_name
        .rsplit("::")
        .find(|name| *name != "{{closure}}")
        .unwrap()
}

/// Returns the intended databend semver for Sentry as an `Option<Cow<'static, str>>`.
///
/// This can be used with `sentry::ClientOptions` to set the databend semver.
///
/// # Examples
///
/// ```
/// # #[macro_use] extern crate common_tracing;
/// # fn main() {
/// let _sentry = sentry::init(sentry::ClientOptions {
///     release: databend_common_tracing::databend_semver!(),
///     ..Default::default()
/// });
/// # }
/// ```
#[macro_export]
macro_rules! databend_semver {
    () => {{
        use std::sync::Once;
        static mut INIT: Once = Once::new();
        static mut RELEASE: Option<String> = None;
        unsafe {
            INIT.call_once(|| {
                RELEASE = option_env!("CARGO_PKG_NAME").and_then(|name| {
                    option_env!("DATABEND_GIT_SEMVER")
                        .map(|version| format!("{}@{}", name, version))
                });
            });
            RELEASE.as_ref().map(|x| {
                let release: &'static str = ::std::mem::transmute(x.as_str());
                ::std::borrow::Cow::Borrowed(release)
            })
        }
    }};
}
