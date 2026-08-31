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

use databend_common_meta_app::schema::SetSecurityPolicyAction;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReq;
use databend_meta_client::types::MatchSeq;
use databend_query::sessions::TableContextTableAccess;
use databend_query::storages::fuse::FuseTable;
use databend_query::test_kits::TestFixture;
use databend_storages_common_table_meta::table::OPT_KEY_LEGACY_SNAPSHOT_LOC;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG;

#[tokio::test(flavor = "multi_thread")]
async fn test_empty_cross_database_clone_internal_metadata() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let source_database = fixture.default_db_name();
    let source_name = fixture.default_table_name();
    let target_database = "clone_target_db";
    let clone_name = "cross_database_clone";

    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    fixture
        .execute_command(&format!("CREATE DATABASE {target_database}"))
        .await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {target_database}.{clone_name} CLONE {source_database}.{source_name}"
        ))
        .await?;

    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;
    let source = catalog
        .get_table(&ctx.get_tenant(), &source_database, &source_name)
        .await?;
    let cloned = catalog
        .get_table(&ctx.get_tenant(), target_database, clone_name)
        .await?;
    let source_fuse = FuseTable::try_from_table(source.as_ref())?;
    let cloned_fuse = FuseTable::try_from_table(cloned.as_ref())?;
    assert!(source_fuse.snapshot_loc().is_none());
    assert!(cloned_fuse.snapshot_loc().is_none());
    assert_eq!(cloned_fuse.clone_group_id()?, source_fuse.clone_group_id()?);
    assert_ne!(source.get_id(), cloned.get_id());
    assert_ne!(
        FuseTable::parse_storage_prefix_from_table_info(source.get_table_info())?,
        FuseTable::parse_storage_prefix_from_table_info(cloned.get_table_info())?
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_clone_rejects_missing_masking_policy_definition() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
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
    assert_eq!(
        err.code(),
        databend_common_exception::ErrorCode::UnknownDatamask("").code()
    );
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
async fn test_clone_purge_internal_reference_ownership() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
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

    fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{left_name} CLONE {database}.{source_name}"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{right_name} CLONE {database}.{source_name}"
        ))
        .await?;

    // Move only the source past its original segment. Descendant roots must still protect it.
    fixture
        .execute_command(&format!("TRUNCATE TABLE {database}.{source_name}"))
        .await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{source_name} VALUES (4, (4, 4))"
        ))
        .await?;

    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;
    let source = catalog
        .get_table(&ctx.get_tenant(), &database, &source_name)
        .await?;
    let source_fuse = FuseTable::try_from_table(source.as_ref())?;
    let protected = source_fuse
        .get_snapshot_referenced_segments(ctx.clone(), |_| {})
        .await?
        .unwrap();
    assert!(
        protected.contains(&source_owned_segment),
        "a source-owned segment retained by a descendant must remain protected"
    );

    // Once every descendant moves beyond the segment's retention root, it becomes collectable.
    for (table, id) in [(&left_name, 2), (&right_name, 3)] {
        fixture
            .execute_command(&format!("TRUNCATE TABLE {database}.{table}"))
            .await?;
        fixture
            .execute_command(&format!(
                "INSERT INTO {database}.{table} VALUES ({id}, ({id}, {id}))"
            ))
            .await?;
    }

    let protected = source_fuse
        .get_snapshot_referenced_segments(ctx.clone(), |_| {})
        .await?
        .unwrap();
    assert!(
        !protected.contains(&source_owned_segment),
        "snapshots older than every member's retention root must not remain protected"
    );

    // Capture a segment owned by the middle clone, then make its child the only retained root.
    let left = catalog
        .get_table(&ctx.get_tenant(), &database, &left_name)
        .await?;
    let left_fuse = FuseTable::try_from_table(left.as_ref())?;
    let left_owned_segment = left_fuse.read_table_snapshot().await?.unwrap().segments[0].clone();
    fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{left_child_name} CLONE {database}.{left_name}"
        ))
        .await?;
    fixture
        .execute_command(&format!("TRUNCATE TABLE {database}.{left_name}"))
        .await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{left_name} VALUES (5, (5, 5))"
        ))
        .await?;
    fixture
        .execute_command(&format!("OPTIMIZE TABLE {database}.{left_name} PURGE"))
        .await?;
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

    fixture
        .execute_command(&format!(
            "ALTER TABLE {database}.{source_name} ADD COLUMN evolved STRING"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "ALTER TABLE {database}.{source_name} CLUSTER BY (id)"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{source_name} (id, t) VALUES (2, (4, 6))"
        ))
        .await?;
    let source = fixture.latest_default_table().await?;
    let source_fuse = FuseTable::try_from_table(source.as_ref())?;
    let source_head_location = source_fuse.snapshot_loc().unwrap();
    let source_head = source_fuse.read_table_snapshot().await?.unwrap();

    fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{head_clone} CLONE {database}.{source_name}"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{historical_clone} CLONE {database}.{source_name} \
             AT (SNAPSHOT => '{first_snapshot_id}')"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{chained_clone} CLONE {database}.{head_clone}"
        ))
        .await?;

    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;
    let source = catalog
        .get_table(&ctx.get_tenant(), &database, &source_name)
        .await?;
    let head = catalog
        .get_table(&ctx.get_tenant(), &database, &head_clone)
        .await?;
    let historical = catalog
        .get_table(&ctx.get_tenant(), &database, &historical_clone)
        .await?;
    let chained = catalog
        .get_table(&ctx.get_tenant(), &database, &chained_clone)
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
    assert_eq!(
        historical_anchor.cluster_key_meta,
        selected_source_snapshot.cluster_key_meta
    );
    assert_eq!(
        historical_anchor.schema.fields(),
        selected_source_snapshot.schema.fields()
    );
    assert_eq!(
        historical_anchor.schema.metadata,
        selected_source_snapshot.schema.metadata
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
    let replaced_head = catalog
        .get_table(&ctx.get_tenant(), &database, &head_clone)
        .await?;
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
    let replaced_source = catalog
        .get_table(&ctx.get_tenant(), &database, &source_name)
        .await?;
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
