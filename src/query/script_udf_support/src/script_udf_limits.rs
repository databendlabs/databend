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

/// A JavaScript runtime with the node's limits applied. Create it before adding
/// functions: `arrow-udf-runtime` bounds every operation as a whole (argument
/// and result conversion, async continuations) and top-level module code
/// evaluated while functions are added.
pub async fn new_js_runtime(
    limits: &ScriptUdfLimits,
) -> anyhow::Result<arrow_udf_runtime::javascript::Runtime> {
    let mut runtime = arrow_udf_runtime::javascript::Runtime::new().await?;
    runtime.set_memory_limit(limits.js_memory_limit).await;
    runtime.set_timeout(limits.js_timeout).await;
    Ok(runtime)
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

    async fn runtime(limits: &ScriptUdfLimits, return_type: DataType, code: &str) -> Runtime {
        let mut runtime = new_js_runtime(limits).await.unwrap();
        runtime
            .add_function(
                "f",
                Field::new("f", return_type, true),
                code,
                FunctionOptions::default().return_null_on_null_input(),
            )
            .await
            .unwrap();
        runtime
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

    // The runtime's own tests cover each phase; these check that Databend's
    // runtimes get the limits. A missing interrupt hangs instead of failing.
    #[tokio::test]
    async fn test_js_timeout_is_applied() {
        let limits = limits(None, Some(Duration::from_millis(200)));
        let output = DataType::Struct(vec![Field::new("a", DataType::Int32, true)].into());
        // Result conversion runs the getter after the function returned.
        let runtime = runtime(
            &limits,
            output,
            "export function f(x) { return { get a() { while (true) {} } }; }",
        )
        .await;
        let start = Instant::now();
        assert!(call(&runtime).await.is_err());
        assert!(start.elapsed() >= Duration::from_millis(200));

        let runtime = runtime_unlimited_memory(&limits).await;
        call(&runtime).await.unwrap();
    }

    async fn runtime_unlimited_memory(limits: &ScriptUdfLimits) -> Runtime {
        // No memory limit configured: the runtime must still allocate.
        runtime(
            limits,
            DataType::Int32,
            "export function f(x) { new Array(1 << 20).fill(0); return x; }",
        )
        .await
    }

    #[tokio::test]
    async fn test_js_memory_limit_is_applied() {
        let limits = limits(Some(64 * 1024 * 1024), None);
        // Bounded so a broken limit fails the assertion instead of the host.
        for (iterations, fits) in [(1, true), (64, false)] {
            let runtime = runtime(
                &limits,
                DataType::Int32,
                &format!(
                    "export function f(x) {{ let a = []; for (let i = 0; i < {iterations}; i++) {{ a.push(new Array(1000000).fill(i)); }} return a.length; }}"
                ),
            )
            .await;
            assert_eq!(call(&runtime).await.is_ok(), fits, "{iterations} arrays");
        }
    }
}
