// Copyright 2026 Datafuse Labs.
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
use databend_common_exception::Result;
use databend_common_sql::Planner;
use databend_common_storages_fuse::FuseTable;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::TableContextSettings;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::TestFixture;
use futures_util::TryStreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_ttl_flashback_schema_validation() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    for sql in [
        "CREATE TABLE default.ttl_flashback (id INT)",
        "INSERT INTO default.ttl_flashback VALUES (1)",
    ] {
        fixture.execute_command(sql).await?;
    }
    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog("default").await?;
    let tenant = fixture.default_tenant();
    let original = catalog
        .get_table(&tenant, "default", "ttl_flashback")
        .await?;
    let snapshot = databend_common_storages_fuse::FuseTable::try_from_table(original.as_ref())?
        .read_table_snapshot()
        .await?
        .unwrap();
    let snapshot_id = snapshot.snapshot_id.simple().to_string();
    for sql in [
        "ALTER TABLE default.ttl_flashback ADD COLUMN ts TIMESTAMP",
        "ALTER TABLE default.ttl_flashback SET TTL ts",
    ] {
        fixture.execute_command(sql).await?;
    }
    let before = catalog
        .get_table(&tenant, "default", "ttl_flashback")
        .await?;
    // Reading history remains allowed even when the current TTL is incompatible.
    fixture
        .execute_query(&format!(
            "SELECT id FROM default.ttl_flashback AT (SNAPSHOT => '{snapshot_id}')"
        ))
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let flashback =
        format!("ALTER TABLE default.ttl_flashback FLASHBACK TO (SNAPSHOT => '{snapshot_id}')");
    let err = match fixture.execute_query(&flashback).await {
        Ok(_) => panic!("flashback should reject a TTL referring to a missing column"),
        Err(err) => err,
    };
    assert_eq!(err.code(), ErrorCode::ILLEGAL_REFERENCE);
    assert!(err.message().contains("REMOVE TTL"));
    let after = catalog
        .get_table(&tenant, "default", "ttl_flashback")
        .await?;
    assert_eq!(before.get_table_info().meta, after.get_table_info().meta);
    assert_eq!(before.get_table_info().ident, after.get_table_info().ident);

    fixture
        .execute_command("ALTER TABLE default.ttl_flashback REMOVE TTL")
        .await?;
    fixture.execute_command(&flashback).await?;
    let restored = catalog
        .get_table(&tenant, "default", "ttl_flashback")
        .await?;
    assert!(restored.schema().field_with_name("ts").is_err());
    assert!(restored.get_table_info().meta.ttl.is_none());

    // A current policy that is valid for the historical schema is retained.
    fixture
        .execute_command("ALTER TABLE default.ttl_flashback SET TTL to_timestamp(id)")
        .await?;
    fixture.execute_command(&flashback).await?;
    let restored = catalog
        .get_table(&tenant, "default", "ttl_flashback")
        .await?;
    assert_eq!(
        restored.get_table_info().meta.ttl.as_deref(),
        Some("to_timestamp(id)")
    );

    // Existing column names alone are insufficient: the historical type must
    // also be compatible with the TTL expression.
    for sql in [
        "CREATE TABLE default.ttl_flashback_type (ts STRING)",
        "INSERT INTO default.ttl_flashback_type VALUES ('2020-01-01')",
    ] {
        fixture.execute_command(sql).await?;
    }
    let original = catalog
        .get_table(&tenant, "default", "ttl_flashback_type")
        .await?;
    let snapshot = databend_common_storages_fuse::FuseTable::try_from_table(original.as_ref())?
        .read_table_snapshot()
        .await?
        .unwrap();
    let snapshot_id = snapshot.snapshot_id.simple().to_string();
    for sql in [
        "ALTER TABLE default.ttl_flashback_type MODIFY COLUMN ts TIMESTAMP",
        "ALTER TABLE default.ttl_flashback_type SET TTL ts",
    ] {
        fixture.execute_command(sql).await?;
    }
    let err = match fixture
        .execute_query(&format!(
            "ALTER TABLE default.ttl_flashback_type FLASHBACK TO (SNAPSHOT => '{snapshot_id}')"
        ))
        .await
    {
        Ok(_) => panic!("flashback should reject an incompatible TTL result type"),
        Err(err) => err,
    };
    assert_eq!(err.code(), ErrorCode::ILLEGAL_REFERENCE);
    assert!(err.message().contains("TTL"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_ttl_if_exists_does_not_target_later_table() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    for sql in [
        "ALTER TABLE IF EXISTS default.ttl_absent SET TTL ts",
        "ALTER TABLE IF EXISTS ttl_absent_catalog.db.t REMOVE TTL",
    ] {
        fixture.execute_command(sql).await?;
    }
    let err = match fixture
        .execute_query("ALTER TABLE default.ttl_absent REMOVE TTL")
        .await
    {
        Ok(_) => panic!("missing target without IF EXISTS should fail"),
        Err(err) => err,
    };
    assert_eq!(err.code(), ErrorCode::UNKNOWN_TABLE);

    // A plan bound to a missing table must not affect a table created later.
    for (action, create) in [
        (
            "SET TTL ts",
            "CREATE TABLE default.ttl_later (ts TIMESTAMP)",
        ),
        (
            "REMOVE TTL",
            "CREATE TABLE default.ttl_later (ts TIMESTAMP) TTL ts",
        ),
    ] {
        let ctx = fixture.new_query_ctx().await?;
        let (plan, _) = Planner::new(ctx.clone())
            .plan_sql(&format!("ALTER TABLE IF EXISTS default.ttl_later {action}"))
            .await?;
        fixture.execute_command(create).await?;
        let catalog = ctx.get_catalog("default").await?;
        let tenant = fixture.default_tenant();
        let before = catalog.get_table(&tenant, "default", "ttl_later").await?;
        InterpreterFactory::get(ctx.clone(), &plan)
            .await?
            .execute2()
            .await?;
        let after = catalog.get_table(&tenant, "default", "ttl_later").await?;
        assert_eq!(before.get_table_info().meta, after.get_table_info().meta);
        assert_eq!(before.get_table_info().ident, after.get_table_info().ident);
        fixture
            .execute_command("DROP TABLE default.ttl_later")
            .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_alter_ttl_does_not_create_snapshot() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture
        .execute_command("CREATE TABLE default.ttl_meta_only (ts TIMESTAMP)")
        .await?;
    fixture
        .execute_command("INSERT INTO default.ttl_meta_only VALUES ('2020-01-01')")
        .await?;

    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog("default").await?;
    let tenant = fixture.default_tenant();
    let table = catalog
        .get_table(&tenant, "default", "ttl_meta_only")
        .await?;
    let snapshot = FuseTable::try_from_table(table.as_ref())?.snapshot_loc();
    let mut seq = table.get_table_info().ident.seq;

    for action in ["SET TTL ts", "REMOVE TTL"] {
        fixture
            .execute_command(&format!("ALTER TABLE default.ttl_meta_only {action}"))
            .await?;
        let table = catalog
            .get_table(&tenant, "default", "ttl_meta_only")
            .await?;
        assert!(table.get_table_info().ident.seq > seq);
        assert_eq!(
            FuseTable::try_from_table(table.as_ref())?.snapshot_loc(),
            snapshot
        );
        seq = table.get_table_info().ident.seq;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_ttl_stored_expression_preserves_json_keys() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    for (sql, expected) in [
        (
            "CREATE TABLE default.ttl_json_keys (payload VARIANT) TTL try_to_timestamp(PAYLOAD:ExpiresAt)",
            "try_to_timestamp(payload:ExpiresAt)",
        ),
        (
            "ALTER TABLE default.ttl_json_keys SET TTL try_to_timestamp(PAYLOAD:Meta:ExpiresAt)",
            "try_to_timestamp(payload:Meta:ExpiresAt)",
        ),
        (
            "ALTER TABLE default.ttl_json_keys RENAME COLUMN payload TO document",
            "try_to_timestamp(document:Meta:ExpiresAt)",
        ),
    ] {
        fixture.execute_command(sql).await?;
        let ctx = fixture.new_query_ctx().await?;
        let table = ctx.get_table("default", "default", "ttl_json_keys").await?;
        assert_eq!(table.get_table_info().meta.ttl.as_deref(), Some(expected));
    }

    // A different session must not fold names already resolved in stored TTL.
    fixture.execute_command("CREATE TABLE default.ttl_case (\"EventTime\" TIMESTAMP, payload VARIANT) TTL greatest(\"EventTime\", try_to_timestamp(payload:ExpiresAt))")
        .await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings()
        .set_setting("quoted_ident_case_sensitive".to_string(), "0".to_string())?;
    let (plan, _) = Planner::new(ctx.clone())
        .plan_sql("ALTER TABLE default.ttl_case RENAME COLUMN payload TO document")
        .await?;
    InterpreterFactory::get(ctx.clone(), &plan)
        .await?
        .execute2()
        .await?;
    let catalog = ctx.get_catalog("default").await?;
    let table = catalog
        .get_table(&fixture.default_tenant(), "default", "ttl_case")
        .await?;
    let ttl = table.get_table_info().meta.ttl.as_ref().unwrap();
    assert!(ttl.contains("\"EventTime\""), "{ttl}");
    assert!(ttl.contains("document:ExpiresAt"), "{ttl}");
    databend_common_sql::validate_stored_ttl_expr(ctx, table.schema(), ttl)?;

    // Stored keys preserve JSON path keys through normalization and display.
    for (table_name, clause) in [
        ("json_cluster", "CLUSTER BY"),
        ("json_partition", "PARTITION BY"),
    ] {
        fixture
            .execute_command(&format!(
                "CREATE TABLE default.{table_name} (payload VARIANT) \
                 {clause} (to_timestamp(PAYLOAD:ExpiresAt))"
            ))
            .await?;
        let blocks = fixture
            .execute_query(&format!("SHOW CREATE TABLE default.{table_name}"))
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let output =
            databend_common_expression::block_debug::pretty_format_blocks(&blocks)?.to_string();
        assert!(output.contains("payload:ExpiresAt"), "{output}");
        assert!(!output.contains("payload:expiresat"), "{output}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_ttl_storage_uses_default_name_resolution() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    // All combinations must produce TTL text that binds under default rules.
    for (i, (unquoted, quoted, column, input, expected)) in [
        ("0", "0", "EventTime", "EventTime", "eventtime"),
        ("0", "1", "\"EventTime\"", "\"EventTime\"", "\"EventTime\""),
        ("1", "0", "EventTime", "EventTime", "\"EventTime\""),
        ("1", "1", "EventTime", "EventTime", "\"EventTime\""),
    ]
    .iter()
    .enumerate()
    {
        let name = format!("ttl_default_names_{i}");
        let ctx = fixture.new_query_ctx().await?;
        ctx.get_settings()
            .set_setting("unquoted_ident_case_sensitive".into(), (*unquoted).into())?;
        ctx.get_settings()
            .set_setting("quoted_ident_case_sensitive".into(), (*quoted).into())?;
        for sql in [
            format!("CREATE TABLE default.{name} ({column} TIMESTAMP) TTL {input}"),
            format!("ALTER TABLE default.{name} SET TTL {input} + INTERVAL 1 DAY"),
        ] {
            let (plan, _) = Planner::new(ctx.clone()).plan_sql(&sql).await?;
            InterpreterFactory::get(ctx.clone(), &plan)
                .await?
                .execute2()
                .await?;
        }
        let catalog = ctx.get_catalog("default").await?;
        let table = catalog
            .get_table(&fixture.default_tenant(), "default", &name)
            .await?;
        let ttl = table.get_table_info().meta.ttl.as_deref().unwrap();
        assert_eq!(ttl, format!("{expected} + INTERVAL 1 DAY"));
        let default_ctx = fixture.new_query_ctx().await?;
        databend_common_sql::validate_stored_ttl_expr(default_ctx, table.schema(), ttl)?;

        // Rename uses an already-resolved new name and must quote it even when
        // quoted identifiers are case-insensitive in the renaming session.
        let rename_ctx = fixture.new_query_ctx().await?;
        rename_ctx
            .get_settings()
            .set_setting("unquoted_ident_case_sensitive".into(), "1".into())?;
        rename_ctx
            .get_settings()
            .set_setting("quoted_ident_case_sensitive".into(), "0".into())?;
        let old_name = table.schema().fields()[0].name().clone();
        let (plan, _) = Planner::new(rename_ctx.clone())
            .plan_sql(&format!(
                "ALTER TABLE default.{name} RENAME COLUMN {old_name} TO NewTime"
            ))
            .await?;
        InterpreterFactory::get(rename_ctx, &plan)
            .await?
            .execute2()
            .await?;
        let table = catalog
            .get_table(&fixture.default_tenant(), "default", &name)
            .await?;
        let ttl = table.get_table_info().meta.ttl.as_deref().unwrap();
        assert_eq!(ttl, "\"NewTime\" + INTERVAL 1 DAY");
        databend_common_sql::validate_stored_ttl_expr(
            fixture.new_query_ctx().await?,
            table.schema(),
            ttl,
        )?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_ttl_schema_compatibility() -> Result<()> {
    let fixture = TestFixture::setup().await?;

    // The definition depends on `ts` while it is nullable. Making the column
    // non-null folds the expression to a constant, but does not make it invalid.
    fixture
        .execute_command(
            "CREATE TABLE default.ttl_nullable (ts TIMESTAMP, id INT) \
             TTL if(ts IS NULL, to_timestamp('2020-01-01'), to_timestamp('2020-01-02'))",
        )
        .await?;
    fixture
        .execute_command("INSERT INTO default.ttl_nullable VALUES ('2020-01-01', 1)")
        .await?;
    fixture
        .execute_command("ALTER TABLE default.ttl_nullable MODIFY COLUMN ts TIMESTAMP NOT NULL")
        .await?;
    let ctx = fixture.new_query_ctx().await?;
    let table = ctx.get_table("default", "default", "ttl_nullable").await?;
    let ttl = table.get_table_info().meta.ttl.clone().unwrap();
    databend_common_sql::validate_stored_ttl_expr(ctx, table.schema(), &ttl)?;

    // CREATE/SET bind directly against a schema before a concrete table exists.
    // Revalidation must use the same semantics instead of expanding a virtual
    // computed column and reaching a different conclusion.
    let ctx = fixture.new_query_ctx().await?;
    let (plan, _) = Planner::new(ctx.clone())
        .plan_sql(
            "CREATE TABLE default.ttl_virtual (ts TIMESTAMP, \
             v TIMESTAMP AS (ts) VIRTUAL) TTL v",
        )
        .await?;
    let databend_common_sql::plans::Plan::CreateTable(plan) = plan else {
        panic!("expected CREATE TABLE plan")
    };
    databend_common_sql::validate_stored_ttl_expr(
        ctx,
        plan.schema.clone(),
        plan.ttl.as_deref().expect("CREATE TTL"),
    )?;
    Ok(())
}
