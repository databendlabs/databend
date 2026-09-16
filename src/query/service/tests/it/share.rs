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

use databend_common_exception::ErrorCode;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_app::data_share::ShareNameIdent;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_users::UserApiProvider;
use databend_query::sessions::TableContextTableAccess;
use databend_query::share::SHARE_ENGINE;
use databend_query::share::ShareDatabaseBinding;
use databend_query::test_kits::TestFixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_data_sharing_is_denied_in_oss() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture
        .execute_command("CREATE TABLE local_table(a INT)")
        .await?;
    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_default_catalog()?;
    // Simulate a binding created by an earlier release. Building the database
    // shell must still permit ordinary database listing and local cleanup.
    catalog
        .create_database(CreateDatabaseReq {
            override_existing: false,
            catalog_name: None,
            name_ident: DatabaseNameIdent::new(fixture.default_tenant(), "existing_shared"),
            meta: DatabaseMeta {
                engine: SHARE_ENGINE.to_string(),
                engine_options: ShareDatabaseBinding {
                    provider_tenant: "provider".into(),
                    share_name: "s".into(),
                    share_id: 1,
                    provider_database_id: 1,
                }
                .to_engine_options(),
                ..Default::default()
            },
        })
        .await?;
    for sql in [
        "CREATE SHARE s",
        "CREATE SHARE IF NOT EXISTS s",
        "CREATE OR REPLACE SHARE s",
        "DROP SHARE s",
        "DROP SHARE IF EXISTS s",
        "ALTER SHARE s SET COMMENT = 'denied'",
        "ALTER SHARE IF EXISTS missing SET COMMENT = 'denied'",
        "ALTER SHARE s ADD ACCOUNTS = consumer",
        "ALTER SHARE s REMOVE ACCOUNTS = consumer",
        "SHOW SHARES",
        "DESC SHARE s",
        "DESC SHARE provider.s",
        "GRANT USAGE ON DATABASE default TO SHARE s",
        "GRANT SELECT ON TABLE default.local_table TO SHARE s",
        "REVOKE USAGE ON DATABASE default FROM SHARE s",
        "REVOKE SELECT ON TABLE default.local_table FROM SHARE s",
        "CREATE DATABASE new_shared FROM SHARE provider.s",
        "CREATE DATABASE IF NOT EXISTS existing_shared FROM SHARE provider.s",
        "SELECT * FROM existing_shared.t",
    ] {
        let error = fixture.execute_command(sql).await.expect_err(sql);
        assert_eq!(
            ErrorCode::LICENSE_KEY_INVALID,
            error.code(),
            "{sql}: {error}"
        );
    }
    let meta = UserApiProvider::instance().get_meta_store_client();
    assert!(
        meta.get_pb(&ShareNameIdent::new(fixture.default_tenant(), "s"))
            .await?
            .is_none()
    );
    let db = catalog
        .get_database(&fixture.default_tenant(), "existing_shared")
        .await?;
    assert_eq!(
        ErrorCode::LICENSE_KEY_INVALID,
        db.list_tables_names().await.unwrap_err().code()
    );
    assert_eq!(
        ErrorCode::LICENSE_KEY_INVALID,
        db.mget_tables(&[]).await.err().unwrap().code()
    );
    fixture.execute_command("SHOW DATABASES").await?;
    fixture.execute_command("SELECT * FROM local_table").await?;
    fixture
        .execute_command("DROP DATABASE existing_shared")
        .await?;
    Ok(())
}
