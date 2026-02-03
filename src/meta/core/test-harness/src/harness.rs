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

use std::sync::Once;

use databend_common_meta_runtime_api::SpawnApi;
use fastrace::prelude::*;

pub fn meta_service_test_harness<SP, F, Fut>(test: F)
where
    SP: SpawnApi,
    F: FnOnce() -> Fut + 'static,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    setup_test::<SP>();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .enable_all()
        .build()
        .unwrap();
    let root = Span::root(closure_name::<F>(), SpanContext::random());
    let test = test().in_span(root);
    rt.block_on(test).unwrap();

    shutdown_test();
}

#[allow(dead_code)]
pub fn meta_service_test_harness_sync<SP, F>(test: F)
where
    SP: SpawnApi,
    F: FnOnce() -> anyhow::Result<()> + 'static,
{
    setup_test::<SP>();

    let root = Span::root(closure_name::<F>(), SpanContext::random());
    let _guard = root.set_local_parent();

    test().unwrap();

    shutdown_test();
}

fn setup_test<SP: SpawnApi>() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let guards = SP::init_test_logging();
        Box::leak(Box::new(guards));
    });
}

fn shutdown_test() {
    fastrace::flush();
}

fn closure_name<F: std::any::Any>() -> &'static str {
    let func_path = std::any::type_name::<F>();
    func_path
        .rsplit("::")
        .find(|name| *name != "{{closure}}")
        .unwrap()
}
