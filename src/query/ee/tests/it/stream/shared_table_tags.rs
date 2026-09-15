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

use databend_common_catalog::session_type::SessionType;
use databend_common_catalog::table::NavigationPoint;
use databend_common_exception::ErrorCode;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::schema::ListTableTagsReq;
use databend_common_sql::plans::CreateTableTagPlan;
use databend_common_sql::plans::DropTableTagPlan;
use databend_common_version::BUILD_INFO;
use databend_enterprise_query::table_ref::RealTableRefHandler;
use databend_enterprise_table_ref_handler::TableRefHandler;
use databend_query::sessions::TableContextTableAccess;

use super::shared_stream::command;
use super::shared_stream::rows;
use super::shared_stream::setup;

#[tokio::test(flavor = "multi_thread")]
async fn test_shared_table_tags_are_read_only() -> anyhow::Result<()> {
    let (fixture, consumer, _meta) = setup().await?;
    RealTableRefHandler::init()?;
    for sql in [
        "SET enable_experimental_table_ref = 1",
        "INSERT INTO provider.t VALUES (73)",
        "ALTER TABLE provider.t CREATE TAG provider_tag",
    ] {
        fixture.execute_command(sql).await?;
    }

    let provider_ctx = fixture.new_query_ctx().await?;
    let table_id = provider_ctx
        .get_table("default", "provider", "t")
        .await?
        .get_id();
    let catalog = provider_ctx.get_catalog("default").await?;
    let list_tags = ListTableTagsReq {
        table_id,
        include_expired: true,
    };
    let original_tags = catalog.list_table_tags(list_tags.clone()).await?;
    assert_eq!(original_tags.len(), 1);
    assert_eq!(original_tags[0].0, "provider_tag");

    let mut alter_user = UserInfo::new_no_auth("tag_alter", "%");
    alter_user.grants.grant_privileges(
        &GrantObject::Table("default".into(), "shared".into(), "t".into()),
        UserPrivilegeType::Alter.into(),
    );
    let mut admin = UserInfo::new_no_auth("tag_admin", "%");
    admin.grants.grant_role("account_admin".to_string());

    for user in [alter_user, admin] {
        let session = fixture.new_session_with_type(SessionType::Dummy).await?;
        session
            .get_settings()
            .set_setting("sandbox_tenant".to_string(), "stream_consumer".to_string())?;
        session
            .get_settings()
            .set_setting("enable_experimental_table_ref".to_string(), "1".to_string())?;
        session.set_authed_user(user, None).await?;

        // ALTER privileges and account_admin must not bypass shared-table mutability.
        for sql in [
            "ALTER TABLE shared.t CREATE TAG consumer_tag",
            "ALTER TABLE shared.t CREATE TAG consumer_tag AT (TAG => provider_tag) RETAIN 1 DAYS",
            "ALTER TABLE shared.t DROP TAG provider_tag",
        ] {
            let error = command(&session, sql).await.unwrap_err();
            assert_eq!(error.code(), ErrorCode::INVALID_OPERATION, "{sql}: {error}");
            assert!(error.message().contains("READ ONLY"), "{sql}: {error}");
        }

        // The shared FuseTable carries the provider's ID. Handler callers must
        // reject it before snapshot navigation or tag metadata access as well.
        let ctx = session.create_query_context(&BUILD_INFO).await?;
        let shared_table = ctx.get_table("default", "shared", "t").await?;
        assert!(shared_table.get_table_info().is_shared());
        assert_eq!(shared_table.get_id(), table_id);
        for navigation in [
            None,
            Some(NavigationPoint::TableTag("provider_tag".to_string())),
            Some(NavigationPoint::TableTag("missing_tag".to_string())),
        ] {
            let plan = CreateTableTagPlan {
                tenant: ctx.get_tenant(),
                catalog: "default".to_string(),
                database: "shared".to_string(),
                table: "t".to_string(),
                name: "consumer_tag".to_string(),
                navigation,
                retain: None,
            };
            let error = RealTableRefHandler {}
                .do_create_table_tag(ctx.clone(), &plan)
                .await
                .unwrap_err();
            assert_eq!(error.code(), ErrorCode::INVALID_OPERATION, "{error}");
            assert!(error.message().contains("READ ONLY"), "{error}");
        }
        let plan = DropTableTagPlan {
            tenant: ctx.get_tenant(),
            catalog: "default".to_string(),
            database: "shared".to_string(),
            table: "t".to_string(),
            name: "provider_tag".to_string(),
        };
        let error = RealTableRefHandler {}
            .do_drop_table_tag(ctx, &plan)
            .await
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::INVALID_OPERATION, "{error}");
        assert!(error.message().contains("READ ONLY"), "{error}");

        assert_eq!(
            catalog.list_table_tags(list_tags.clone()).await?,
            original_tags
        );
    }

    assert_eq!(rows(&consumer, "SELECT a FROM shared.t").await?, 1);
    fixture
        .execute_command("ALTER TABLE provider.t DROP TAG provider_tag")
        .await?;
    assert!(catalog.list_table_tags(list_tags).await?.is_empty());
    Ok(())
}
