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

use std::time::Duration;
use std::time::Instant;

use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;

/// Resource limits of in-process script UDF engines and of dependency
/// installation. `None` means unlimited.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScriptUdfLimits {
    pub js_memory_limit: Option<usize>,
    pub js_timeout: Option<Duration>,
    pub wasm_memory_limit: Option<usize>,
    pub pip_only_binary: bool,
}

impl ScriptUdfLimits {
    pub fn from_config(config: &InnerConfig) -> Self {
        let common = &config.query.common;
        // One memory budget is shared by the in-process engines; the struct
        // keeps them apart because each engine enforces it differently.
        let memory_limit = mib_to_bytes(common.udf_script_memory_limit_mb);
        Self {
            js_memory_limit: memory_limit,
            js_timeout: (common.udf_script_execution_timeout_secs != 0)
                .then(|| Duration::from_secs(common.udf_script_execution_timeout_secs))
                // A deadline beyond the clock range cannot fire: treat as unlimited.
                .filter(|timeout| Instant::now().checked_add(*timeout).is_some()),
            wasm_memory_limit: memory_limit,
            pip_only_binary: common.python_udf_pip_only_binary,
        }
    }

    /// Limits of the running node, or configuration defaults outside a node
    /// (unit tests and tools that never initialize `GlobalConfig`).
    pub fn current() -> Self {
        match GlobalConfig::try_get_instance() {
            Some(config) => Self::from_config(&config),
            None => Self::from_config(&InnerConfig::default()),
        }
    }
}

fn mib_to_bytes(mib: usize) -> Option<usize> {
    (mib != 0).then(|| mib.saturating_mul(1024 * 1024))
}

/// Bound a fresh JavaScript runtime before user code is evaluated. Top-level
/// module code runs during pipeline construction and must not hang it either,
/// so it gets one deadline covering initialization.
pub async fn limit_js_runtime_init(
    runtime: &arrow_udf_runtime::javascript::Runtime,
    limits: &ScriptUdfLimits,
) {
    // `set_memory_limit(None)` maps to a QuickJS limit of 0 bytes rather than
    // "unlimited"; a fresh runtime is already unlimited, so leave it alone.
    if let Some(limit) = limits.js_memory_limit {
        runtime.set_memory_limit(Some(limit)).await;
    }
    if let Some(deadline) = limits
        .js_timeout
        .and_then(|timeout| Instant::now().checked_add(timeout))
    {
        runtime
            .context()
            .runtime()
            .set_interrupt_handler(Some(Box::new(move || Instant::now() >= deadline)))
            .await;
    }
}

/// Switch from the initialization deadline to a per-call deadline.
pub async fn limit_js_runtime_calls(
    runtime: &mut arrow_udf_runtime::javascript::Runtime,
    limits: &ScriptUdfLimits,
) {
    runtime.set_timeout(limits.js_timeout).await;
}

pub fn wasm_runtime_config(limits: &ScriptUdfLimits) -> arrow_udf_runtime::wasm::Config {
    // `Config` is non-exhaustive: start from the default and assign fields.
    let mut config = arrow_udf_runtime::wasm::Config::default();
    config.memory_size_limit = limits.wasm_memory_limit;
    config
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::Int32Array;
    use arrow_array::RecordBatch;
    use arrow_schema::DataType;
    use arrow_schema::Field;
    use arrow_schema::Schema;
    use arrow_udf_runtime::javascript::FunctionOptions;
    use arrow_udf_runtime::javascript::Runtime;

    use super::*;

    fn limits(memory: Option<usize>, timeout: Option<Duration>) -> ScriptUdfLimits {
        ScriptUdfLimits {
            js_memory_limit: memory,
            js_timeout: timeout,
            wasm_memory_limit: None,
            pip_only_binary: true,
        }
    }

    async fn add(runtime: &mut Runtime, body: &str) -> anyhow::Result<()> {
        runtime
            .add_function(
                "f",
                Field::new("f", DataType::Int32, true),
                &format!("export function f(x) {{ {body} }}"),
                FunctionOptions::default().return_null_on_null_input(),
            )
            .await
    }

    async fn call(runtime: &Runtime) -> anyhow::Result<RecordBatch> {
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )?;
        runtime.call("f", &input).await
    }

    #[test]
    fn test_limits_from_config() {
        let mut config = InnerConfig::default();
        let defaults = ScriptUdfLimits::from_config(&config);
        assert_eq!(defaults.js_memory_limit, Some(1024 * 1024 * 1024));
        assert_eq!(defaults.js_timeout, Some(Duration::from_secs(300)));
        assert_eq!(defaults.wasm_memory_limit, Some(1024 * 1024 * 1024));
        assert!(defaults.pip_only_binary);

        config.query.common.udf_script_memory_limit_mb = 0;
        config.query.common.udf_script_execution_timeout_secs = 0;
        let unlimited = ScriptUdfLimits::from_config(&config);
        assert_eq!(unlimited.js_memory_limit, None);
        assert_eq!(unlimited.js_timeout, None);
        assert_eq!(unlimited.wasm_memory_limit, None);
        assert_eq!(
            wasm_runtime_config(&unlimited),
            arrow_udf_runtime::wasm::Config::default()
        );

        config.query.common.udf_script_memory_limit_mb = 64;
        let limited = ScriptUdfLimits::from_config(&config);
        assert_eq!(limited.js_memory_limit, Some(64 * 1024 * 1024));
        assert_eq!(limited.wasm_memory_limit, Some(64 * 1024 * 1024));

        config.query.common.udf_script_execution_timeout_secs = u64::MAX;
        assert_eq!(ScriptUdfLimits::from_config(&config).js_timeout, None);
    }

    // QuickJS evaluates synchronously: a missing interrupt hangs these tests
    // (caught by the test runner timeout) rather than failing an assertion.
    #[tokio::test]
    async fn test_js_top_level_code_is_interrupted() {
        let mut runtime = Runtime::new().await.unwrap();
        limit_js_runtime_init(&runtime, &limits(None, Some(Duration::from_millis(200)))).await;
        let start = Instant::now();
        // arrow-udf-runtime drops the module evaluation promise, so an
        // interrupted top level is not reported as an error. What matters is
        // that the loop ran until the deadline and pipeline construction returns.
        let _ = runtime
            .add_function(
                "f",
                Field::new("f", DataType::Int32, true),
                "while (true) {}\nexport function f(x) { return x; }",
                FunctionOptions::default().return_null_on_null_input(),
            )
            .await;
        assert!(start.elapsed() >= Duration::from_millis(200));
    }

    #[tokio::test]
    async fn test_js_call_is_interrupted() {
        let limits = limits(None, Some(Duration::from_millis(200)));
        let mut runtime = Runtime::new().await.unwrap();
        limit_js_runtime_init(&runtime, &limits).await;
        add(&mut runtime, "while (true) {}").await.unwrap();
        limit_js_runtime_calls(&mut runtime, &limits).await;
        assert!(call(&runtime).await.is_err());
        // Every call gets a fresh deadline on the same runtime.
        assert!(call(&runtime).await.is_err());
        // The initialization deadline must not leak into later calls.
        let mut healthy = Runtime::new().await.unwrap();
        limit_js_runtime_init(&healthy, &limits).await;
        add(&mut healthy, "return x;").await.unwrap();
        limit_js_runtime_calls(&mut healthy, &limits).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        call(&healthy).await.unwrap();
    }

    #[tokio::test]
    async fn test_js_memory_is_limited() {
        let limits = limits(Some(64 * 1024 * 1024), None);
        // Bounded so a broken limit fails the assertion instead of the host.
        for (iterations, fits) in [(1, true), (64, false)] {
            let mut runtime = Runtime::new().await.unwrap();
            limit_js_runtime_init(&runtime, &limits).await;
            add(
                &mut runtime,
                &format!(
                    "let a = []; for (let i = 0; i < {iterations}; i++) {{ a.push(new Array(1000000).fill(i)); }} return a.length;"
                ),
            )
            .await
            .unwrap();
            limit_js_runtime_calls(&mut runtime, &limits).await;
            assert_eq!(call(&runtime).await.is_ok(), fits, "{iterations} arrays");
        }
    }
}
