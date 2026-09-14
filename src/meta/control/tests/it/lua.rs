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

use databend_common_meta_control::lua_support::run_lua_script;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_test_harness::start_metasrv;

/// Directory holding the Lua unit-test scripts, relative to the crate manifest.
const LUA_TEST_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/it/lua");

/// Run every `*.lua` script under `tests/it/lua/` inside the `metactl` Lua
/// environment, against an in-process meta-service.
///
/// Each script runs with the full `metactl` namespace (see
/// `setup_lua_environment`) plus a global `TEST_GRPC_ADDRESS` string holding the
/// gRPC address of the in-process meta-service, so a script can connect with
/// `metactl.new_grpc_client(TEST_GRPC_ADDRESS)`. The runner waits for the
/// service to accept writes before running the first script, so scripts do not
/// need their own readiness retry.
///
/// The runner prints a banner before each script and each script prints its
/// own progress, so `cargo test` output shows which script is running and
/// which one a failure belongs to.
///
/// A script signals failure by raising a Lua error (e.g. via `assert`); it must
/// not call `os.exit`, which would abort the whole test binary.
///
/// Lua execution is `!Send` (mlua state on a `LocalSet`), so this uses a plain
/// multi-thread `tokio::test` rather than `meta_service_test_harness`, whose
/// `Send` bound the mlua future cannot satisfy.
#[tokio::test(flavor = "multi_thread")]
async fn test_lua_scripts() -> anyhow::Result<()> {
    let (_meta, grpc_address) = start_metasrv::<DatabendRuntime>().await?;

    // The single-node cluster may need a moment to elect itself leader; retry
    // the first write until the service accepts it.
    let readiness = format!(
        r#"
        local client = metactl.new_grpc_client({grpc_address:?})
        for _ = 1, 50 do
            local _, err = client:upsert("lua_it/ready", "1")
            if err == nil then return end
            metactl.sleep(0.2)
        end
        error("meta-service did not become ready for gRPC writes")
        "#
    );
    run_lua_script(&readiness).await?;

    let mut scripts = std::fs::read_dir(LUA_TEST_DIR)?
        .map(|entry| Ok(entry?.path()))
        .collect::<anyhow::Result<Vec<_>>>()?;
    scripts.retain(|path| path.extension().is_some_and(|ext| ext == "lua"));
    scripts.sort();

    assert!(
        !scripts.is_empty(),
        "no Lua test scripts found in {LUA_TEST_DIR}"
    );

    for path in &scripts {
        let name = path.file_name().unwrap().to_string_lossy();

        let body = std::fs::read_to_string(path)?;
        // Announce the script via Lua `print` so the banner lands in the same
        // output stream as the script's own prints (Rust `println!` is
        // captured by the test harness, Lua `print` is not), and inject the
        // in-process meta-service address as a global. Both statements share
        // one line so Lua error line numbers stay offset by exactly one from
        // the script file.
        let banner = format!("--- running Lua test: {name}");
        let script = format!("print({banner:?}) TEST_GRPC_ADDRESS = {grpc_address:?}\n{body}");

        run_lua_script(&script)
            .await
            .map_err(|e| anyhow::anyhow!("Lua test `{name}` failed: {e}"))?;

        println!("--- passed: {name}");
    }

    println!("all {} Lua test scripts passed", scripts.len());

    Ok(())
}
