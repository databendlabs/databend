// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use databend_common_catalog::session_type::SessionType;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license::LicenseInfo;
use databend_common_license::license_manager::LicenseManager;
use databend_common_meta_store::LocalMetaService;
use databend_common_settings::Settings;
use databend_common_storages_stream::stream_table::StreamTable;
use databend_common_version::BUILD_INFO;
use databend_enterprise_query::license::RealLicenseManager;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_meta_runtime::DatabendRuntime;
use databend_query::sessions::Session;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::execute_command;
use jwt_simple::algorithms::ES256KeyPair;
use jwt_simple::prelude::Claims;
use jwt_simple::prelude::Duration;
use jwt_simple::prelude::ECDSAP256KeyPairLike;
use jwt_simple::prelude::UnixTimeStamp;

fn license(key: &ES256KeyPair, features: Option<Vec<Feature>>, expired: bool) -> String {
    let mut claims = Claims::with_custom_claims(
        LicenseInfo {
            r#type: Some("enterprise".into()),
            org: None,
            tenants: None,
            features,
        },
        Duration::from_hours(1),
    );
    if expired {
        claims.expires_at = Some(UnixTimeStamp::new(1, 0));
    }
    key.sign(claims).unwrap()
}

async fn command(session: &Arc<Session>, sql: &str) -> Result<()> {
    execute_command(session.create_query_context(&BUILD_INFO).await?, sql).await
}

async fn set_license(session: &Arc<Session>, key: &str) -> Result<()> {
    session
        .get_settings()
        .set_global_setting("enterprise_license".into(), key.into())
        .await?;
    // Sandbox sessions retain their original settings tenant. Database APIs
    // resolve the sandbox tenant's settings independently of the session.
    Settings::create(session.get_current_tenant())
        .set_global_setting("enterprise_license".into(), key.into())
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_data_sharing_license_features() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let key = ES256KeyPair::generate();
    let manager = RealLicenseManager::new("test".into(), key.public_key().to_pem()?);
    for features in [None, Some(vec![Feature::DataSharing])] {
        let token = license(&key, features, false);
        for _ in 0..2 {
            manager.check_enterprise_enabled(token.clone(), Feature::DataSharing)?;
        }
    }
    for token in [
        String::new(),
        "invalid".into(),
        license(&key, Some(vec![Feature::Stream]), false),
    ] {
        assert!(
            manager
                .check_enterprise_enabled(token, Feature::DataSharing)
                .is_err()
        );
    }
    let error = manager
        .check_enterprise_enabled(license(&key, None, true), Feature::DataSharing)
        .unwrap_err();
    assert_eq!(ErrorCode::LICENSE_KEY_EXPIRED, error.code());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_data_sharing_existing_bindings_require_license() -> anyhow::Result<()> {
    let meta = LocalMetaService::new_testing::<DatabendRuntime>("sharing-license").await?;
    let key = ES256KeyPair::generate();
    let enabled = license(
        &key,
        Some(vec![
            Feature::DataSharing,
            Feature::Stream,
            Feature::TableRef,
        ]),
        false,
    );
    let mut setup = EESetup::new_with_key_pair(&key);
    setup.config_mut().meta.endpoints = meta.get_cached_endpoints().await?;
    setup
        .config_mut()
        .query
        .common
        .internal_enable_sandbox_tenant = true;
    setup.config_mut().query.common.databend_enterprise_license = Some(enabled.clone());
    let fixture = TestFixture::setup_with_custom(setup).await?;
    for sql in [
        "CREATE DATABASE provider",
        "CREATE TABLE provider.t(a INT) CHANGE_TRACKING = TRUE",
        "CREATE TABLE provider.other(a INT)",
        "INSERT INTO provider.t VALUES (1)",
        "CREATE CONNECTION conn STORAGE_TYPE = 'fs'",
        "CREATE SHARE s CONNECTION = conn",
        "GRANT USAGE ON DATABASE provider TO SHARE s",
        "GRANT SELECT ON TABLE provider.t TO SHARE s",
        "GRANT SELECT ON TABLE provider.other TO SHARE s",
        "ALTER SHARE s ADD ACCOUNTS = license_consumer",
    ] {
        fixture.execute_command(sql).await?;
    }
    let consumer = fixture.new_session_with_type(SessionType::Dummy).await?;
    consumer
        .get_settings()
        .set_setting("sandbox_tenant".into(), "license_consumer".into())?;
    command(&consumer, "SET enable_experimental_table_ref = 1").await?;
    command(
        &consumer,
        &format!(
            "CREATE DATABASE shared FROM SHARE {}.s",
            fixture.default_tenant().tenant_name()
        ),
    )
    .await?;
    command(&consumer, "CREATE DATABASE local_db").await?;
    command(
        &consumer,
        "CREATE STREAM local_db.changes ON TABLE shared.t",
    )
    .await?;
    fixture
        .execute_command("INSERT INTO provider.t VALUES (2)")
        .await?;
    command(&consumer, "SELECT * FROM shared.t").await?;
    command(&consumer, "SELECT * FROM local_db.changes").await?;
    command(&consumer, "SELECT * FROM fuse_snapshot('shared', 't')").await?;
    command(&consumer, "SELECT * FROM fuse_tag('shared', 't')").await?;
    let cached_ctx = consumer.create_query_context(&BUILD_INFO).await?;
    let cached = cached_ctx.get_table("default", "shared", "t").await?;
    let stream = cached_ctx
        .get_table("default", "local_db", "changes")
        .await?;
    let catalog = cached_ctx.get_default_catalog()?;
    let database = catalog
        .get_database(&consumer.get_current_tenant(), "shared")
        .await?;
    let batch_names = ["other", "missing", "t", "other"].map(String::from);
    let tables = database.mget_tables(&batch_names).await?;
    assert_eq!(
        vec!["other", "t", "other"],
        tables.iter().map(|table| table.name()).collect::<Vec<_>>()
    );
    assert!(
        tables
            .iter()
            .all(|table| table.get_table_info().is_shared())
    );
    assert!(database.mget_tables(&[]).await?.is_empty());
    for (token, expected_code) in [
        (
            license(&key, Some(vec![Feature::Stream, Feature::TableRef]), false),
            ErrorCode::LICENSE_KEY_INVALID,
        ),
        (license(&key, None, true), ErrorCode::LICENSE_KEY_EXPIRED),
    ] {
        set_license(&consumer, &token).await?;
        for sql in [
            "SELECT * FROM shared.t",
            "SELECT * FROM local_db.changes",
            "SELECT * FROM fuse_snapshot('shared', 't')",
            "SELECT * FROM fuse_tag('shared', 't')",
            "SHOW SHARES",
            "DESC SHARE s",
        ] {
            let error = command(&consumer, sql).await.expect_err(sql);
            assert_eq!(expected_code, error.code(), "{sql}: {error}");
        }
        assert_eq!(
            expected_code,
            database.list_tables_names().await.unwrap_err().code()
        );
        for names in [&batch_names[..], &[]] {
            assert_eq!(
                expected_code,
                database.mget_tables(names).await.err().unwrap().code()
            );
        }
        assert_eq!(
            expected_code,
            cached_ctx
                .get_table("default", "shared", "t")
                .await
                .err()
                .unwrap()
                .code()
        );
        assert_eq!(
            expected_code,
            cached_ctx
                .build_table_by_table_info(cached.get_table_info(), None)
                .err()
                .unwrap()
                .code()
        );
        assert_eq!(
            expected_code,
            cached
                .read_partitions(cached_ctx.clone(), None, false)
                .await
                .unwrap_err()
                .code()
        );
        assert_eq!(
            expected_code,
            StreamTable::try_from_table(stream.as_ref())?
                .source_table(cached_ctx.clone())
                .await
                .err()
                .unwrap()
                .code()
        );
        command(&consumer, "SHOW DATABASES").await?;
        command(&consumer, "SELECT 1").await?;
    }
    set_license(&consumer, &enabled).await?;
    command(&consumer, "SELECT * FROM shared.t").await?;
    command(&consumer, "SELECT * FROM local_db.changes").await?;
    // The provider is independently gated, including no-op commands and cleanup.
    let provider = fixture.default_session();
    set_license(
        &provider,
        &license(&key, Some(vec![Feature::Stream]), false),
    )
    .await?;
    for sql in [
        "CREATE SHARE IF NOT EXISTS s",
        "DROP SHARE IF EXISTS s",
        "ALTER SHARE IF EXISTS missing SET COMMENT = 'denied'",
        "REVOKE SELECT ON TABLE provider.t FROM SHARE s",
        "SHOW SHARES",
        "DESC SHARE s",
    ] {
        assert_eq!(
            ErrorCode::LICENSE_KEY_INVALID,
            fixture.execute_command(sql).await.expect_err(sql).code()
        );
    }
    set_license(&provider, &enabled).await?;
    command(&consumer, "SELECT * FROM shared.t").await?;
    Ok(())
}
