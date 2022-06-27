// Copyright 2022 Datafuse Labs.
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
///     release: common_tracing::databend_semver!(),
///     ..Default::default()
/// });
/// # }
/// ```
#[macro_export]
macro_rules! databend_semver {
    () => {{
        use parking_lot::Once;
        static mut INIT: Once = Once::new();
        static mut RELEASE: Option<String> = None;
        unsafe {
            INIT.call_once(|| {
                RELEASE = option_env!("CARGO_PKG_NAME").and_then(|name| {
                    option_env!("VERGEN_GIT_SEMVER").map(|version| format!("{}@{}", name, version))
                });
            });
            RELEASE.as_ref().map(|x| {
                let release: &'static str = ::std::mem::transmute(x.as_str());
                ::std::borrow::Cow::Borrowed(release)
            })
        }
    }};
}
