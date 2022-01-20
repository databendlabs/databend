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

mod logging;
mod panic_hook;
mod tracing_to_jaeger;

pub use logging::init_default_ut_tracing;
pub use logging::init_global_tracing;
pub use logging::init_meta_ut_tracing;
pub use panic_hook::set_panic_hook;
pub use tracing;
pub use tracing_futures;
pub use tracing_to_jaeger::extract_remote_span_as_parent;
pub use tracing_to_jaeger::inject_span_to_tonic_request;

#[macro_export]
macro_rules! func_name {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        let n = &name[..name.len() - 3];
        let nn = n.replace("::{{closure}}", "");
        nn
    }};
}
