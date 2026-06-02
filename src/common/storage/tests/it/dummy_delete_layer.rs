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

//! End-to-end tests for the optional `DummyDeleteLayer` gating.
//!
//! These exercise the real `init_operator` production path (the same one query
//! nodes use), so they require a reachable S3 endpoint. They default to the
//! local MinIO setup used by CI (`scripts/ci/ci-setup-minio.sh`) and are skipped
//! gracefully when that endpoint is not reachable, so plain `cargo test` runs
//! without object storage do not fail.
//!
//! Connection parameters can be overridden via `DUMMY_DELETE_S3_ENDPOINT`,
//! `DUMMY_DELETE_S3_BUCKET`, `DUMMY_DELETE_S3_ACCESS_KEY_ID` and
//! `DUMMY_DELETE_S3_SECRET_ACCESS_KEY`.

use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::sync::OnceLock;
use std::time::Duration;

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::storage::StorageS3Config;
use databend_common_storage::init_operator;
use opendal::ErrorKind;
use opendal::Operator;
use opendal::services;

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:9900";
const DEFAULT_BUCKET: &str = "testbucket";
const DEFAULT_ACCESS_KEY: &str = "minioadmin";
const DEFAULT_SECRET_KEY: &str = "minioadmin";

/// Environment switch read by `build_operator` to install `DummyDeleteLayer`.
const DISABLE_DELETE_ENV: &str = "_DATABEND_INTERNAL_DISABLE_DELETE";

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn endpoint() -> String {
    env_or("DUMMY_DELETE_S3_ENDPOINT", DEFAULT_ENDPOINT)
}

fn bucket() -> String {
    env_or("DUMMY_DELETE_S3_BUCKET", DEFAULT_BUCKET)
}

fn access_key() -> String {
    env_or("DUMMY_DELETE_S3_ACCESS_KEY_ID", DEFAULT_ACCESS_KEY)
}

fn secret_key() -> String {
    env_or("DUMMY_DELETE_S3_SECRET_ACCESS_KEY", DEFAULT_SECRET_KEY)
}

/// Initialize the process-global registries that `build_operator` relies on.
///
/// `init_operator` ultimately installs a `RuntimeLayer` backed by
/// `GlobalIORuntime::instance()`, which panics if the global registry has not
/// been set up. Initialize it once for the whole test binary.
fn init_globals() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        GlobalInstance::init_production();
        GlobalIORuntime::init(2).expect("init GlobalIORuntime");
    });
}

/// Best-effort reachability probe for the configured S3 endpoint.
///
/// Returns `false` (and the caller skips the test) when the endpoint cannot be
/// reached, so the suite stays green on machines without object storage.
fn endpoint_reachable(endpoint: &str) -> bool {
    let hostport = endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/');
    let hostport = match hostport.split_once('/') {
        Some((hp, _)) => hp,
        None => hostport,
    };

    let Ok(mut addrs) = hostport.to_socket_addrs() else {
        return false;
    };
    addrs.any(|addr| TcpStream::connect_timeout(&addr, Duration::from_millis(800)).is_ok())
}

/// Build a `StorageParams::S3` pointing at the test bucket under `root`.
fn s3_params(root: &str) -> StorageParams {
    StorageParams::S3(StorageS3Config {
        endpoint_url: endpoint(),
        bucket: bucket(),
        access_key_id: access_key(),
        secret_access_key: secret_key(),
        root: root.to_string(),
        // Explicit credentials only; never reach out to the ambient chain.
        allow_credential_chain: Some(false),
        ..Default::default()
    })
}

/// A raw operator (no databend layers) used purely for setup/teardown so that
/// cleanup is never affected by the `DummyDeleteLayer` under test.
fn raw_operator(root: &str) -> Operator {
    let builder = services::S3::default()
        .endpoint(&endpoint())
        .bucket(&bucket())
        .access_key_id(&access_key())
        .secret_access_key(&secret_key())
        .region("us-east-1")
        .root(root)
        .disable_config_load()
        .disable_ec2_metadata();
    Operator::new(builder).unwrap().finish()
}

/// By default (env var unset) deletes go through the normal path and succeed.
///
/// This is the core regression guard: `DummyDeleteLayer` must NOT be installed
/// unless explicitly requested, so real deletes against S3 work.
#[tokio::test(flavor = "multi_thread")]
async fn delete_enabled_by_default_on_s3() {
    let ep = endpoint();
    if !endpoint_reachable(&ep) {
        eprintln!("skipping: S3 endpoint {ep} not reachable");
        return;
    }
    init_globals();

    let root = "_dummy_delete_test/default";
    let path = "delete_enabled.txt";
    let content = b"delete should succeed by default".to_vec();

    // Make sure the env var is unset for this path, then build via the real
    // production operator path. The env var is read synchronously inside
    // `init_operator`, so the sync `with_vars` wrapper is sufficient.
    let op = temp_env::with_vars([(DISABLE_DELETE_ENV, None::<&str>)], || {
        init_operator(&s3_params(root)).expect("init operator")
    });

    op.write(path, content.clone()).await.expect("PUT");
    assert_eq!(op.read(path).await.expect("GET").to_vec(), content);

    // The whole point: delete actually removes the object.
    op.delete(path)
        .await
        .expect("DELETE should succeed by default");

    // Confirm it is gone.
    assert!(
        !op.exists(path).await.expect("stat"),
        "object should be deleted"
    );
}

/// With the env var enabled, `DummyDeleteLayer` is installed and rejects deletes
/// while leaving PUT/GET/LIST intact.
#[tokio::test(flavor = "multi_thread")]
async fn delete_disabled_when_env_set_on_s3() {
    let ep = endpoint();
    if !endpoint_reachable(&ep) {
        eprintln!("skipping: S3 endpoint {ep} not reachable");
        return;
    }
    init_globals();

    // Distinct root so the operator cache key differs from the default-path test.
    let root = "_dummy_delete_test/disabled";
    let path = "delete_disabled.txt";
    let content = b"delete should be rejected when disabled".to_vec();

    // Pre-seed and guarantee teardown via a raw operator unaffected by the layer.
    let cleanup_op = raw_operator(root);
    let _ = cleanup_op.delete(path).await;

    let op = temp_env::with_vars([(DISABLE_DELETE_ENV, Some("true"))], || {
        init_operator(&s3_params(root)).expect("init operator")
    });

    // PUT / GET / LIST unaffected.
    op.write(path, content.clone()).await.expect("PUT");
    assert_eq!(op.read(path).await.expect("GET").to_vec(), content);
    let listed = op.list("/").await.expect("LIST");
    assert!(
        listed.iter().any(|e| e.path().contains("delete_disabled")),
        "LIST should contain the test object"
    );

    // DELETE is rejected with Unsupported.
    let err = op
        .delete(path)
        .await
        .expect_err("DELETE should be rejected");
    assert_eq!(
        err.kind(),
        ErrorKind::Unsupported,
        "expected Unsupported, got: {err}"
    );

    // Object still exists because the delete was blocked.
    assert!(
        op.exists(path).await.expect("stat"),
        "object should still exist after a rejected delete"
    );

    // Teardown with the raw operator.
    cleanup_op.delete(path).await.expect("cleanup DELETE");
}
