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

use chrono::Duration;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::schema::LeastVisibleTime;
use databend_common_meta_app::schema::SetSecurityPolicyAction;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReq;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_app::schema::least_visible_time_ident::LeastVisibleTimeIdent;
use databend_common_sql::Planner;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_meta_client::types::MatchSeq;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::TableContextTableAccess;
use databend_query::storages::fuse::FuseTable;
use databend_query::test_kits::TestFixture;
use databend_storages_common_table_meta::table::OPT_KEY_LEGACY_SNAPSHOT_LOC;
use databend_storages_common_table_meta::table::OPT_KEY_SEGMENT_FORMAT;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG;

#[tokio::test(flavor = "multi_thread")]
async fn test_clone_feature_gate_at_bind_and_execution() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let database = fixture.default_db_name();
    let source = fixture.default_table_name();
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    let sql = format!("CREATE TABLE {database}.{source}_clone CLONE {database}.{source}");

    // The Binder rejects CLONE while the experimental feature is disabled by default.
    let err = fixture.execute_command(&sql).await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::UNIMPLEMENTED);
    assert!(err.message().contains("enable_experimental_clone_table=1"));
    assert!(err.message().contains("mixed-version deployments"));

    // A plan built while enabled must still be rejected if the setting is disabled before
    // execution (for example, when a caller directly executes a previously constructed plan).
    fixture.enable_experimental_clone_table()?;
    let ctx = fixture.new_query_ctx().await?;
    let (plan, _) = Planner::new(ctx.clone()).plan_sql(&sql).await?;
    fixture.default_session().get_settings().set_setting(
        "enable_experimental_clone_table".to_string(),
        "0".to_string(),
    )?;
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let err = match interpreter.execute(ctx).await {
        Ok(_) => panic!("clone execution must recheck the experimental feature gate"),
        Err(err) => err,
    };
    assert_eq!(err.code(), ErrorCode::UNIMPLEMENTED);
    assert!(err.message().contains("enable_experimental_clone_table=1"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_clone_source_privileges() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.enable_experimental_clone_table()?;
    let session = fixture.default_session();
    let mut admin = session.get_current_user()?;
    admin.grants.grant_role("account_admin".to_string());
    session.set_authed_user(admin, None).await?;
    session.set_current_role_checked("account_admin").await?;
    for sql in [
        "CREATE DATABASE `clone.source`",
        "CREATE DATABASE clone_target",
        "CREATE TABLE `clone.source`.secret(c INT)",
        "INSERT INTO `clone.source`.secret VALUES (42)",
    ] {
        fixture.execute_command(sql).await?;
    }
    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_default_catalog()?;
    let tenant = ctx.get_tenant();
    let source = catalog.get_table(&tenant, "clone.source", "secret").await?;
    let source_db = catalog
        .get_database(&tenant, "clone.source")
        .await?
        .get_db_info()
        .database_id
        .db_id;
    let target_db = catalog
        .get_database(&tenant, "clone_target")
        .await?
        .get_db_info()
        .database_id
        .db_id;
    let snapshot = FuseTable::try_from_table(source.as_ref())?
        .read_table_snapshot()
        .await?
        .unwrap();
    let session = fixture.default_session();
    // Fixture root starts with PUBLIC as its current role. Create source objects under
    // ACCOUNT_ADMIN above so restricted PUBLIC users cannot inherit their ownership.
    session.set_current_role_checked("public").await?;
    session.set_secondary_roles_checked(Some(vec![])).await?;
    let mut creator = UserInfo::new("clone_creator", "%", AuthInfo::None);
    creator.grants.grant_privileges(
        &GrantObject::DatabaseById("default".into(), target_db),
        UserPrivilegeType::Create.into(),
    );

    for (name, point, grant) in [
        (
            "by_id",
            String::new(),
            GrantObject::TableById("default".into(), source_db, source.get_id()),
        ),
        (
            "by_name",
            format!(" AT (SNAPSHOT => '{}')", snapshot.snapshot_id.simple()),
            GrantObject::Table("default".into(), "clone.source".into(), "secret".into()),
        ),
    ] {
        let sql = format!("CREATE TABLE clone_target.{name} CLONE `clone.source`.secret{point}");
        session.set_authed_user(creator.clone(), None).await?;
        let err = fixture.execute_command(&sql).await.unwrap_err();
        assert_eq!(err.code(), ErrorCode::PERMISSION_DENIED);
        assert!(!catalog.exists_table(&tenant, "clone_target", name).await?);

        let mut reader = UserInfo::new("clone_reader", "%", AuthInfo::None);
        reader
            .grants
            .grant_privileges(&grant, UserPrivilegeType::Select.into());
        session.set_authed_user(reader, None).await?;
        let err = fixture.execute_command(&sql).await.unwrap_err();
        assert_eq!(err.code(), ErrorCode::PERMISSION_DENIED);

        let mut authorized = creator.clone();
        authorized
            .grants
            .grant_privileges(&grant, UserPrivilegeType::Select.into());
        session.set_authed_user(authorized, None).await?;
        fixture.execute_command(&sql).await?;
        assert!(catalog.exists_table(&tenant, "clone_target", name).await?);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_clone_rejects_missing_masking_policy_definition() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.enable_experimental_clone_table()?;
    let database = fixture.default_db_name();
    let source_name = fixture.default_table_name();
    let clone_name = format!("{}_missing_mask_clone", source_name);

    fixture.create_default_database().await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{source_name}(id INT, secured STRING)"
        ))
        .await?;

    let ctx = fixture.new_query_ctx().await?;
    let tenant = ctx.get_tenant();
    let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;
    let source = catalog.get_table(&tenant, &database, &source_name).await?;
    let secured_column_id = source.schema().field_with_name("secured")?.column_id();
    let missing_policy_id = u64::MAX;
    catalog
        .set_table_column_mask_policy(SetTableColumnMaskPolicyReq {
            tenant: tenant.clone(),
            table_id: source.get_id(),
            seq: MatchSeq::Exact(source.get_table_info().ident.seq),
            action: SetSecurityPolicyAction::Set(missing_policy_id, vec![secured_column_id]),
        })
        .await?;

    let err = fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{clone_name} CLONE {database}.{source_name}"
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::UnknownDatamask("").code());
    assert!(err.message().contains(&missing_policy_id.to_string()));
    assert!(
        !catalog
            .exists_table(&tenant, &database, &clone_name)
            .await?,
        "a missing policy definition must fail before publishing the clone"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_persistent_navigation_rejects_legacy_policies() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.enable_experimental_clone_table()?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    let database = fixture.default_db_name();
    let name = fixture.default_table_name();
    fixture
        .execute_command(&format!("INSERT INTO {database}.{name} VALUES (1, (2, 3))"))
        .await?;
    let table = fixture.latest_default_table().await?;
    let snapshot = FuseTable::try_from_table(table.as_ref())?
        .read_table_snapshot()
        .await?
        .unwrap();
    fixture
        .execute_command(&format!("INSERT INTO {database}.{name} VALUES (2, (4, 5))"))
        .await?;
    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_default_catalog()?;
    let tenant = ctx.get_tenant();

    // These unsupported metadata states are not constructible through current policy SQL.
    for row_policy in [true, false] {
        let source = catalog.get_table(&tenant, &database, &name).await?;
        let info = source.get_table_info();
        let head = FuseTable::try_from_table(source.as_ref())?.snapshot_loc();
        let mut meta = info.meta.clone();
        meta.row_access_policy = row_policy.then(|| "legacy_row_policy".to_string());
        meta.column_mask_policy =
            (!row_policy).then(|| [("id".to_string(), "legacy_mask_policy".to_string())].into());
        catalog
            .update_single_table_meta(
                &tenant,
                UpdateTableMetaReq {
                    table_id: source.get_id(),
                    seq: MatchSeq::Exact(info.ident.seq),
                    new_table_meta: meta.clone(),
                    base_snapshot_location: head.clone(),
                    lvt_check: None,
                },
                info,
            )
            .await?;

        for sql in [
            format!(
                "CREATE TABLE {database}.legacy_clone CLONE {database}.{name} AT (SNAPSHOT => '{}')",
                snapshot.snapshot_id.simple()
            ),
            format!(
                "ALTER TABLE {database}.{name} FLASHBACK TO (SNAPSHOT => '{}')",
                snapshot.snapshot_id.simple()
            ),
        ] {
            let err = fixture.execute_command(&sql).await.unwrap_err();
            assert_eq!(err.code(), ErrorCode::INVALID_ARGUMENT);
            assert!(err.message().contains("legacy security policies"));
        }
        assert!(
            !catalog
                .exists_table(&tenant, &database, "legacy_clone")
                .await?
        );
        let current = catalog.get_table(&tenant, &database, &name).await?;
        assert_eq!(current.get_table_info().meta, meta);
        assert_eq!(
            FuseTable::try_from_table(current.as_ref())?.snapshot_loc(),
            head
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_clone_special_snapshot_publications_respect_lvt() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.enable_experimental_clone_table()?;
    let database = fixture.default_db_name();
    let source_name = format!("{}_ddl_fence", fixture.default_table_name());
    let modify_clone = format!("{source_name}_modify");
    let segment_clone = format!("{source_name}_segment");

    fixture.create_default_database().await?;
    for sql in [
        format!(
            "CREATE TABLE {database}.{source_name}(d DECIMAL(10, 2)) \
             row_per_block=1 block_per_segment=1"
        ),
        format!("INSERT INTO {database}.{source_name} VALUES (1.00), (2.00)"),
        format!("CREATE TABLE {database}.{modify_clone} CLONE {database}.{source_name}"),
        format!("CREATE TABLE {database}.{segment_clone} CLONE {database}.{source_name}"),
    ] {
        fixture.execute_command(&sql).await?;
    }

    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;
    let tenant = ctx.get_tenant();
    for (table_name, ddl) in [
        (
            modify_clone.as_str(),
            format!("ALTER TABLE {database}.{modify_clone} MODIFY COLUMN d DECIMAL(15, 2)"),
        ),
        (
            segment_clone.as_str(),
            format!(
                "ALTER TABLE {database}.{segment_clone} \
                 SET OPTIONS(segment_format = 'column_oriented')"
            ),
        ),
    ] {
        let table = catalog.get_table(&tenant, &database, table_name).await?;
        let before = FuseTable::try_from_table(table.as_ref())?.snapshot_loc();
        catalog
            .set_table_lvt(
                &LeastVisibleTimeIdent::new(&tenant, table.get_id()),
                &LeastVisibleTime::new(Utc::now() + Duration::days(1)),
            )
            .await?;

        let err = fixture.execute_command(&ddl).await.unwrap_err();
        assert_eq!(err.code(), ErrorCode::TABLE_SNAPSHOT_EXPIRED);
        let current = catalog.get_table(&tenant, &database, table_name).await?;
        assert_eq!(
            FuseTable::try_from_table(current.as_ref())?.snapshot_loc(),
            before
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_non_clone_segment_format_publishes_immediately() -> anyhow::Result<()> {
    // The LVT-guarded publication path is buffered until COMMIT inside an explicit transaction.
    // A table with no clone group needs no fence, so it must keep the immediate option upsert.
    let fixture = TestFixture::setup().await?;
    fixture.enable_experimental_clone_table()?;
    let database = fixture.default_db_name();
    let name = format!("{}_plain_segment_format", fixture.default_table_name());

    fixture.create_default_database().await?;
    for sql in [
        format!("CREATE TABLE {database}.{name}(id INT) row_per_block=1 block_per_segment=1"),
        format!("INSERT INTO {database}.{name} VALUES (1), (2)"),
    ] {
        fixture.execute_command(&sql).await?;
    }

    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;
    let tenant = ctx.get_tenant();
    let table_id = catalog.get_table(&tenant, &database, &name).await?.get_id();

    fixture.execute_command("BEGIN").await?;
    fixture
        .execute_command(&format!(
            "ALTER TABLE {database}.{name} SET OPTIONS(segment_format = 'column_oriented')"
        ))
        .await?;
    let buffered = fixture
        .default_session()
        .txn_mgr()
        .lock()
        .req()
        .update_table_metas
        .iter()
        .any(|(req, _)| req.table_id == table_id);
    assert!(
        !buffered,
        "a table without a clone group must publish its segment format immediately, \
         not buffer it until COMMIT"
    );
    fixture.execute_command("COMMIT").await?;

    let current = catalog.get_table(&tenant, &database, &name).await?;
    assert_eq!(
        current
            .get_table_info()
            .meta
            .options
            .get(OPT_KEY_SEGMENT_FORMAT)
            .map(String::as_str),
        Some("column_oriented")
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_clone_purge_internal_reference_ownership() -> anyhow::Result<()> {
    // PURGE is an alias of the unified enterprise VACUUM path.
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;
    fixture.enable_experimental_clone_table()?;
    fixture
        .default_session()
        .get_settings()
        .set_data_retention_time_in_days(0)?;
    fixture.default_session().get_settings().set_setting(
        "data_retention_num_snapshots_to_keep".to_string(),
        "1".to_string(),
    )?;

    let database = fixture.default_db_name();
    let source_name = fixture.default_table_name();
    let left_name = format!("{}_left", source_name);
    let left_child_name = format!("{}_left_child", source_name);
    let right_name = format!("{}_right", source_name);

    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{source_name} VALUES (1, (2, 3))"
        ))
        .await?;

    let source = fixture.latest_default_table().await?;
    let source_fuse = FuseTable::try_from_table(source.as_ref())?;
    let source_owned_segment =
        source_fuse.read_table_snapshot().await?.unwrap().segments[0].clone();

    for sql in [
        format!("CREATE TABLE {database}.{left_name} CLONE {database}.{source_name}"),
        format!("CREATE TABLE {database}.{right_name} CLONE {database}.{source_name}"),
    ] {
        fixture.execute_command(&sql).await?;
    }

    // Move only the source past its original segment. Descendant roots must still protect it.
    for sql in [
        format!("TRUNCATE TABLE {database}.{source_name}"),
        format!("INSERT INTO {database}.{source_name} VALUES (4, (4, 4))"),
    ] {
        fixture.execute_command(&sql).await?;
    }

    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;
    let tenant = ctx.get_tenant();
    let source = catalog.get_table(&tenant, &database, &source_name).await?;
    let source_fuse = FuseTable::try_from_table(source.as_ref())?;
    let protected = source_fuse
        .get_snapshot_referenced_segments(ctx.clone(), |_| {})
        .await?
        .unwrap();
    assert!(
        protected.contains(&source_owned_segment),
        "a source-owned segment retained by a descendant must remain protected"
    );

    // Advance each descendant's independent LVT so reference-only cleanup can stop protecting
    // source-owned history below every live member's published boundary.
    for (table, id) in [(&left_name, 2), (&right_name, 3)] {
        fixture
            .execute_command(&format!("TRUNCATE TABLE {database}.{table}"))
            .await?;
        fixture
            .execute_command(&format!(
                "INSERT INTO {database}.{table} VALUES ({id}, ({id}, {id}))"
            ))
            .await?;
        fixture
            .execute_command(&format!("VACUUM TABLE {database}.{table}"))
            .await?;
    }

    let protected = source_fuse
        .get_snapshot_referenced_segments(ctx.clone(), |_| {})
        .await?
        .unwrap();
    assert!(
        !protected.contains(&source_owned_segment),
        "snapshots below every descendant's published LVT must not remain protected"
    );

    // Capture a segment owned by the middle clone, then make its child the only retained root.
    let left = catalog.get_table(&tenant, &database, &left_name).await?;
    let left_fuse = FuseTable::try_from_table(left.as_ref())?;
    let left_owned_segment = left_fuse.read_table_snapshot().await?.unwrap().segments[0].clone();
    for sql in [
        format!("CREATE TABLE {database}.{left_child_name} CLONE {database}.{left_name}"),
        format!("TRUNCATE TABLE {database}.{left_name}"),
        format!("INSERT INTO {database}.{left_name} VALUES (5, (5, 5))"),
        format!("OPTIMIZE TABLE {database}.{left_name} PURGE"),
    ] {
        fixture.execute_command(&sql).await?;
    }
    assert!(
        left_fuse
            .get_operator_ref()
            .exists(&left_owned_segment.0)
            .await?,
        "a middle-clone segment retained only by its child must survive purge"
    );

    // The original source-owned segment is no longer reachable by any retained root.
    fixture
        .execute_command(&format!("OPTIMIZE TABLE {database}.{source_name} PURGE"))
        .await?;
    assert!(
        !source_fuse
            .get_operator_ref()
            .exists(&source_owned_segment.0)
            .await?
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_clone_anchor_and_replace_lineage() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.enable_experimental_clone_table()?;
    let database = fixture.default_db_name();
    let source_name = fixture.default_table_name();
    let head_clone = format!("{}_clone_head", source_name);
    let historical_clone = format!("{}_clone_historical", source_name);
    let chained_clone = format!("{}_clone_chain", source_name);

    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{source_name} VALUES (1, (2, 3))"
        ))
        .await?;

    let source = fixture.latest_default_table().await?;
    let source_fuse = FuseTable::try_from_table(source.as_ref())?;
    let first_snapshot_location = source_fuse.snapshot_loc().unwrap();
    let first_snapshot_id = source_fuse
        .read_table_snapshot()
        .await?
        .unwrap()
        .snapshot_id
        .simple()
        .to_string();

    for sql in [
        format!("ALTER TABLE {database}.{source_name} ADD COLUMN evolved STRING"),
        format!("ALTER TABLE {database}.{source_name} CLUSTER BY (id)"),
        format!("INSERT INTO {database}.{source_name} (id, t) VALUES (2, (4, 6))"),
    ] {
        fixture.execute_command(&sql).await?;
    }
    let source = fixture.latest_default_table().await?;
    let source_fuse = FuseTable::try_from_table(source.as_ref())?;
    let source_head_location = source_fuse.snapshot_loc().unwrap();
    let source_head = source_fuse.read_table_snapshot().await?.unwrap();

    for sql in [
        format!("CREATE TABLE {database}.{head_clone} CLONE {database}.{source_name}"),
        format!(
            "CREATE TABLE {database}.{historical_clone} CLONE {database}.{source_name} \
             AT (SNAPSHOT => '{first_snapshot_id}')"
        ),
        format!("CREATE TABLE {database}.{chained_clone} CLONE {database}.{head_clone}"),
    ] {
        fixture.execute_command(&sql).await?;
    }

    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;
    let tenant = ctx.get_tenant();
    let source = catalog.get_table(&tenant, &database, &source_name).await?;
    let head = catalog.get_table(&tenant, &database, &head_clone).await?;
    let historical = catalog
        .get_table(&tenant, &database, &historical_clone)
        .await?;
    let chained = catalog
        .get_table(&tenant, &database, &chained_clone)
        .await?;

    // Each clone owns a fresh root snapshot while sharing immutable source segments.
    let head_fuse = FuseTable::try_from_table(head.as_ref())?;
    let head_anchor_location = head_fuse.snapshot_loc().unwrap();
    let head_anchor = head_fuse.read_table_snapshot().await?.unwrap();
    assert!(
        !head
            .get_table_info()
            .meta
            .options
            .contains_key(OPT_KEY_LEGACY_SNAPSHOT_LOC)
    );
    assert!(
        !head
            .get_table_info()
            .meta
            .options
            .contains_key(OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG)
    );
    assert_ne!(head_anchor_location, source_head_location);
    assert_eq!(head_anchor.prev_snapshot_id, None);
    assert_eq!(head_anchor.segments, source_head.segments);

    let selected_source_snapshot = FuseTable::try_from_table(source.as_ref())?
        .read_table_snapshot_with_location(Some(first_snapshot_location.clone()))
        .await?
        .unwrap();
    let historical_fuse = FuseTable::try_from_table(historical.as_ref())?;
    let historical_anchor_location = historical_fuse.snapshot_loc().unwrap();
    let historical_anchor = historical_fuse.read_table_snapshot().await?.unwrap();
    assert_ne!(historical_anchor_location, first_snapshot_location);
    assert_eq!(historical_anchor.prev_snapshot_id, None);
    assert_eq!(
        historical_anchor.segments,
        selected_source_snapshot.segments
    );

    // Replacement publishes a new table but retains direct-source lineage for dropped ancestors.
    let source_id = source.get_id();
    let old_head_id = head.get_id();
    let chained_id = chained.get_id();
    fixture
        .execute_command(&format!(
            "CREATE OR REPLACE TABLE {database}.{head_clone} CLONE {database}.{source_name}"
        ))
        .await?;
    let replaced_head = catalog.get_table(&tenant, &database, &head_clone).await?;
    assert_ne!(replaced_head.get_id(), old_head_id);

    let group_id = source_fuse.clone_group_id()?;
    let lineage = catalog.list_clone_group_bindings(group_id).await?;
    let direct_source = |table_id| {
        lineage
            .iter()
            .find_map(|(id, source_id)| (*id == table_id).then_some(*source_id))
    };
    assert_eq!(direct_source(old_head_id), Some(source_id));
    assert_eq!(direct_source(chained_id), Some(old_head_id));
    assert_eq!(direct_source(replaced_head.get_id()), Some(source_id));

    // Self-replacement must bind the new table to the old source before publishing it.
    fixture
        .execute_command(&format!(
            "CREATE OR REPLACE TABLE {database}.{source_name} CLONE {database}.{source_name}"
        ))
        .await?;
    let replaced_source = catalog.get_table(&tenant, &database, &source_name).await?;
    assert_ne!(replaced_source.get_id(), source_id);
    let lineage = catalog.list_clone_group_bindings(group_id).await?;
    assert_eq!(
        lineage.iter().find_map(|(id, direct_source)| {
            (*id == replaced_source.get_id()).then_some(*direct_source)
        }),
        Some(source_id)
    );

    Ok(())
}
