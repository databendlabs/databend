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

use databend_common_catalog::table::Table;
use databend_common_storage::read_parquet_schema_async_rs;
use databend_common_storages_fuse::FUSE_TBL_VIRTUAL_BLOCK_PREFIX;
use databend_common_storages_fuse::FUSE_TBL_VIRTUAL_BLOCK_PREFIX_V1;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::MetaWriter;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::statistics::merge_statistics;
use databend_enterprise_query::storages::fuse::operations::virtual_columns::commit_refresh_virtual_column;
use databend_enterprise_query::storages::fuse::operations::virtual_columns::do_vacuum_virtual_column;
use databend_enterprise_query::storages::fuse::operations::virtual_columns::prepare_refresh_virtual_column;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::pipelines::PipelineBuildResult;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelineCompleteExecutor;
use databend_query::test_kits::*;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_refresh_virtual_column() -> anyhow::Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    fixture
        .default_session()
        .get_settings()
        .set_data_retention_time_in_days(1)?;
    fixture
        .default_session()
        .get_settings()
        .set_enable_experimental_virtual_column(0)?;
    fixture.create_default_database().await?;
    fixture.create_variant_table().await?;

    let number_of_block = 2;
    append_variant_sample_data(number_of_block, &fixture).await?;

    fixture
        .default_session()
        .get_settings()
        .set_enable_experimental_virtual_column(1)?;

    let table_ctx = fixture.new_query_ctx().await?;
    table_ctx
        .get_settings()
        .set_data_retention_time_in_days(1)?;

    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;

    let results =
        prepare_refresh_virtual_column(table_ctx.clone(), fuse_table, None, false, None).await?;

    assert!(!results.is_empty());

    let mut build_res = PipelineBuildResult::create();
    let _ = commit_refresh_virtual_column(
        table_ctx.clone(),
        fuse_table,
        &mut build_res.main_pipeline,
        results,
    )
    .await?;

    let settings = table_ctx.get_settings();
    build_res.set_max_threads(settings.get_max_threads()? as usize);
    let settings = ExecutorSettings::try_create(table_ctx.clone())?;

    if build_res.main_pipeline.is_complete_pipeline()? {
        let mut pipelines = build_res.sources_pipelines;
        pipelines.push(build_res.main_pipeline);

        let complete_executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;
        table_ctx.set_executor(complete_executor.get_inner())?;
        complete_executor.execute()?;
    }

    let table = fixture.latest_default_table().await?;
    let table_schema = table.schema();
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let dal = fuse_table.get_operator_ref();
    let snapshot_opt = fuse_table.read_table_snapshot().await?;
    let snapshot = snapshot_opt.unwrap();

    let segment_reader =
        MetaReaders::segment_info_reader(fuse_table.get_operator(), table_schema.clone());

    for (location, ver) in &snapshot.segments {
        let segment_info = segment_reader
            .read(&LoadParams {
                location: location.to_string(),
                len_hint: None,
                ver: *ver,
                put_cache: false,
            })
            .await?;

        let block_metas = segment_info.block_metas()?;
        for block_meta in block_metas {
            assert!(block_meta.virtual_block_meta.is_some());
            let virtual_block_meta = block_meta.virtual_block_meta.clone().unwrap();

            let virtual_loc = virtual_block_meta.virtual_location.0;
            assert!(dal.exists(&virtual_loc).await?);

            let schema = read_parquet_schema_async_rs(dal, &virtual_loc, None)
                .await
                .ok();
            assert!(schema.is_some());
            let schema = schema.unwrap();
            assert_eq!(schema.fields.len(), 2);
            assert_eq!(schema.fields[0].name(), "v['a']");
            assert_eq!(schema.fields[1].name(), "v['b']");
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_vacuum_virtual_column_removes_orphan() -> anyhow::Result<()> {
    // Build virtual columns, then inject an orphan _vb_v2 file.
    // Vacuum should remove the orphan while keeping referenced files.
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    fixture
        .default_session()
        .get_settings()
        .set_data_retention_time_in_days(0)?;
    fixture
        .default_session()
        .get_settings()
        .set_enable_experimental_virtual_column(0)?;
    fixture.create_default_database().await?;
    fixture.create_variant_table().await?;

    let number_of_block = 1;
    append_variant_sample_data(number_of_block, &fixture).await?;

    fixture
        .default_session()
        .get_settings()
        .set_enable_experimental_virtual_column(1)?;

    let table_ctx = fixture.new_query_ctx().await?;
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;

    let results =
        prepare_refresh_virtual_column(table_ctx.clone(), fuse_table, None, false, None).await?;
    assert!(!results.is_empty());

    let mut build_res = PipelineBuildResult::create();
    let _ = commit_refresh_virtual_column(
        table_ctx.clone(),
        fuse_table,
        &mut build_res.main_pipeline,
        results,
    )
    .await?;

    let settings = table_ctx.get_settings();
    build_res.set_max_threads(settings.get_max_threads()? as usize);
    let settings = ExecutorSettings::try_create(table_ctx.clone())?;

    if build_res.main_pipeline.is_complete_pipeline()? {
        let mut pipelines = build_res.sources_pipelines;
        pipelines.push(build_res.main_pipeline);

        let complete_executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;
        table_ctx.set_executor(complete_executor.get_inner())?;
        complete_executor.execute()?;
    }

    let table = fixture.latest_default_table().await?;
    let table_schema = table.schema();
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let dal = fuse_table.get_operator_ref();
    let snapshot = fuse_table.read_table_snapshot().await?.unwrap();

    let segment_reader =
        MetaReaders::segment_info_reader(fuse_table.get_operator(), table_schema.clone());

    let mut referenced_locations = Vec::new();
    for (location, ver) in &snapshot.segments {
        let segment_info = segment_reader
            .read(&LoadParams {
                location: location.to_string(),
                len_hint: None,
                ver: *ver,
                put_cache: false,
            })
            .await?;

        for block_meta in segment_info.block_metas()?.into_iter() {
            if let Some(virtual_block_meta) = &block_meta.virtual_block_meta {
                let virtual_location = virtual_block_meta.virtual_location.0.clone();
                if !virtual_location.is_empty() {
                    referenced_locations.push(virtual_location);
                }
            }
        }
    }
    assert!(!referenced_locations.is_empty());

    for location in &referenced_locations {
        assert!(dal.exists(location).await?);
    }

    let table_prefix = fuse_table
        .meta_location_generator()
        .prefix()
        .trim_start_matches('/');
    let orphan_location = format!(
        "{}/{}/orphan-virtual-column",
        table_prefix, FUSE_TBL_VIRTUAL_BLOCK_PREFIX
    );
    dal.write(&orphan_location, "orphan").await?;
    assert!(dal.exists(&orphan_location).await?);

    let removed = do_vacuum_virtual_column(table_ctx.clone(), fuse_table).await?;
    assert!(removed >= 1);
    assert!(!dal.exists(&orphan_location).await?);

    for location in referenced_locations {
        assert!(dal.exists(&location).await?);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_vacuum_virtual_column_removes_legacy_prefix() -> anyhow::Result<()> {
    // Create a legacy _vb directory and file; vacuum should remove the whole prefix.
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    fixture
        .default_session()
        .get_settings()
        .set_data_retention_time_in_days(0)?;
    fixture.create_default_database().await?;
    fixture.create_variant_table().await?;

    let number_of_block = 1;
    append_variant_sample_data(number_of_block, &fixture).await?;

    let table_ctx = fixture.new_query_ctx().await?;
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let dal = fuse_table.get_operator_ref();

    let table_prefix = fuse_table
        .meta_location_generator()
        .prefix()
        .trim_start_matches('/');
    let v1_prefix = format!("{}/{}/", table_prefix, FUSE_TBL_VIRTUAL_BLOCK_PREFIX_V1);
    dal.create_dir(&v1_prefix).await?;

    let legacy_location = format!("{}/legacy-virtual-column", v1_prefix.trim_end_matches('/'));
    dal.write(&legacy_location, "legacy").await?;
    assert!(dal.exists(&legacy_location).await?);

    let _ = do_vacuum_virtual_column(table_ctx.clone(), fuse_table).await?;
    assert!(!dal.exists(&legacy_location).await?);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_vacuum_virtual_column_replaced_block_keeps_schema() -> anyhow::Result<()> {
    // Scenario: a table has both valid _vb_v2 blocks and one block pointing to a legacy
    // _vb location. This can only happen with historical data written before the v2 format
    // was introduced — we simulate it by rewriting block meta in a segment.
    //
    // Vacuum generates ReplacedBlock entries (to clear legacy block metas) and an
    // AppendVirtualSchema(Replace) entry (carrying the rebuilt schema from _vb_v2 blocks),
    // then commits them together.
    //
    // Verifications after vacuum:
    //   1. Table-level virtual_schema is preserved with the same field count
    //   2. Legacy block metas are cleared (virtual_block_meta = None)
    //   3. Valid _vb_v2 block metas remain intact
    //   4. No legacy _vb references remain in any block

    // ── Step 1: Setup table with 2 blocks of variant data and refresh virtual columns ──
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    fixture
        .default_session()
        .get_settings()
        .set_data_retention_time_in_days(0)?;
    fixture
        .default_session()
        .get_settings()
        .set_enable_experimental_virtual_column(0)?;
    fixture.create_default_database().await?;
    fixture.create_variant_table().await?;

    let number_of_block = 2;
    append_variant_sample_data(number_of_block, &fixture).await?;

    fixture
        .default_session()
        .get_settings()
        .set_enable_experimental_virtual_column(1)?;

    let table_ctx = fixture.new_query_ctx().await?;
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;

    let results =
        prepare_refresh_virtual_column(table_ctx.clone(), fuse_table, None, false, None).await?;
    assert!(!results.is_empty());

    let mut build_res = PipelineBuildResult::create();
    let _ = commit_refresh_virtual_column(
        table_ctx.clone(),
        fuse_table,
        &mut build_res.main_pipeline,
        results,
    )
    .await?;

    let settings = table_ctx.get_settings();
    build_res.set_max_threads(settings.get_max_threads()? as usize);
    let settings = ExecutorSettings::try_create(table_ctx.clone())?;

    if build_res.main_pipeline.is_complete_pipeline()? {
        let mut pipelines = build_res.sources_pipelines;
        pipelines.push(build_res.main_pipeline);

        let complete_executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;
        table_ctx.set_executor(complete_executor.get_inner())?;
        complete_executor.execute()?;
    }

    // Save the original virtual_schema for later comparison.
    let table = fixture.latest_default_table().await?;
    let original_virtual_schema = table.get_table_info().meta.virtual_schema.clone();
    assert!(original_virtual_schema.is_some());

    // ── Step 2: Simulate a legacy _vb block by rewriting one block's virtual location ──
    // Read the first segment, change block[0]'s virtual_location prefix from _vb_v2 to _vb,
    // write a new segment, and commit a new snapshot that includes this tampered segment.
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let operator = fuse_table.get_operator();
    let table_schema = table.schema();
    let segment_reader =
        MetaReaders::segment_info_reader(fuse_table.get_operator(), table_schema.clone());
    let snapshot = fuse_table.read_table_snapshot().await?.unwrap();
    let (segment_location, segment_ver) = snapshot.segments[0].clone();

    let segment_info = segment_reader
        .read(&LoadParams {
            location: segment_location.clone(),
            len_hint: None,
            ver: segment_ver,
            put_cache: false,
        })
        .await?;

    let mut segment_info = SegmentInfo::try_from(segment_info.as_ref())?;
    let mut blocks = segment_info.blocks.clone();
    let mut block_meta = Arc::unwrap_or_clone(blocks[0].clone());
    let Some(virtual_block_meta) = block_meta.virtual_block_meta.as_mut() else {
        panic!("missing virtual block meta for legacy conversion");
    };
    let legacy_location = virtual_block_meta.virtual_location.0.replace(
        FUSE_TBL_VIRTUAL_BLOCK_PREFIX,
        FUSE_TBL_VIRTUAL_BLOCK_PREFIX_V1,
    );
    virtual_block_meta.virtual_location.0 = legacy_location;
    blocks[0] = Arc::new(block_meta);

    segment_info.blocks = blocks;
    segment_info.format_version = SegmentInfo::VERSION;

    let location_gen = fuse_table.meta_location_generator();
    let new_segment_location =
        location_gen.gen_segment_info_location(TestFixture::default_table_meta_timestamps(), false);
    segment_info
        .write_meta(&operator, &new_segment_location)
        .await?;

    // Commit a new snapshot with the tampered segment appended.
    let mut new_snapshot = TableSnapshot::try_from_previous(
        snapshot.clone(),
        Some(fuse_table.get_table_info().ident.seq),
        TestFixture::default_table_meta_timestamps(),
    )?;
    let mut new_segments = snapshot.segments.clone();
    new_segments.push((new_segment_location.clone(), SegmentInfo::VERSION));
    new_snapshot.segments = new_segments;
    new_snapshot.summary = merge_statistics(snapshot.summary.clone(), &segment_info.summary, None);

    fuse_table
        .commit_to_meta_server(
            table_ctx.as_ref(),
            fuse_table.get_table_info(),
            location_gen,
            new_snapshot,
            None,
            &None,
            &operator,
        )
        .await?;

    // ── Step 3: Run vacuum and verify results ──
    let latest_table = fixture.latest_default_table().await?;
    let latest_fuse_table = FuseTable::try_from_table(latest_table.as_ref())?;
    let vacuum_ctx = fixture.new_query_ctx().await?;
    vacuum_ctx
        .get_settings()
        .set_data_retention_time_in_days(1)?;
    let removed = do_vacuum_virtual_column(vacuum_ctx.clone(), latest_fuse_table).await?;
    assert!(removed >= 1);

    // Verify: table-level virtual_schema is preserved with the same field count.
    let table = fixture.latest_default_table().await?;
    let new_virtual_schema = table.get_table_info().meta.virtual_schema.clone();
    assert!(new_virtual_schema.is_some());
    assert_eq!(
        original_virtual_schema.as_ref().unwrap().fields.len(),
        new_virtual_schema.as_ref().unwrap().fields.len()
    );

    // Verify: no legacy references remain; legacy blocks have None, v2 blocks have Some.
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let snapshot = fuse_table.read_table_snapshot().await?.unwrap();
    let segment_reader =
        MetaReaders::segment_info_reader(fuse_table.get_operator(), table.schema());
    let mut legacy_found = false;
    let mut none_count = 0;
    let mut some_count = 0;
    for (location, ver) in &snapshot.segments {
        let segment_info = segment_reader
            .read(&LoadParams {
                location: location.to_string(),
                len_hint: None,
                ver: *ver,
                put_cache: false,
            })
            .await?;
        for block_meta in segment_info.block_metas()?.into_iter() {
            if let Some(virtual_block_meta) = &block_meta.virtual_block_meta {
                some_count += 1;
                if TableMetaLocationGenerator::is_legacy_virtual_block_location(
                    &virtual_block_meta.virtual_location.0,
                ) {
                    legacy_found = true;
                }
            } else {
                none_count += 1;
            }
        }
    }

    assert!(!legacy_found);
    assert!(none_count >= 1);
    assert!(some_count >= 1);

    Ok(())
}
