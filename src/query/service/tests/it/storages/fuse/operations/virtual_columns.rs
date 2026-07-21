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

use std::sync::Arc;

use databend_common_storage::read_parquet_schema_async_rs;
use databend_common_storages_fuse::FUSE_TBL_VIRTUAL_BLOCK_PREFIX;
use databend_common_storages_fuse::FUSE_TBL_VIRTUAL_BLOCK_PREFIX_V1;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::MetaWriter;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::operations::VirtualColumnVacuumResult;
use databend_common_storages_fuse::operations::commit_refresh_virtual_column;
use databend_common_storages_fuse::operations::do_vacuum_virtual_column;
use databend_common_storages_fuse::operations::prepare_refresh_virtual_column;
use databend_common_storages_fuse::statistics::merge_statistics;
use databend_query::pipelines::PipelineBuildResult;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelineCompleteExecutor;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextSettings;
use databend_query::test_kits::*;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_cache::Table;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;

/// Create a variant table, insert data, and refresh virtual columns.
async fn setup_table_with_virtual_columns(num_blocks: usize) -> anyhow::Result<TestFixture> {
    let fixture = TestFixture::setup().await?;
    fixture
        .default_session()
        .get_settings()
        .set_enable_experimental_virtual_column(1)?;
    fixture.create_default_database().await?;
    fixture.create_variant_table().await?;
    append_variant_sample_data(num_blocks, &fixture).await?;

    let ctx = fixture.new_query_ctx().await?;
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;

    let results =
        prepare_refresh_virtual_column(ctx.clone(), fuse_table, None, false, None).await?;
    if !results.is_empty() {
        let mut build_res = PipelineBuildResult::create();
        commit_refresh_virtual_column(
            ctx.clone(),
            fuse_table,
            &mut build_res.main_pipeline,
            results,
        )
        .await?;
        let settings = ctx.get_settings();
        build_res.set_max_threads(settings.get_max_threads()? as usize);
        let settings = ExecutorSettings::try_create(ctx.clone())?;
        if build_res.main_pipeline.is_complete_pipeline()? {
            let mut pipelines = build_res.sources_pipelines;
            pipelines.push(build_res.main_pipeline);
            let executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;
            ctx.set_executor(executor.get_inner())?;
            executor.execute().await?;
        }
    }
    Ok(fixture)
}

async fn prepare_vacuum_virtual_column_for_test(
    ctx: Arc<QueryContext>,
    fuse_table: &FuseTable,
) -> anyhow::Result<(VirtualColumnVacuumResult, bool)> {
    let mut build_res = PipelineBuildResult::create();
    let vacuum_result =
        do_vacuum_virtual_column(ctx.clone(), fuse_table, &mut build_res.main_pipeline).await?;

    Ok((vacuum_result, build_res.main_pipeline.is_empty()))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_refresh_virtual_column() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;

    fixture
        .default_session()
        .get_settings()
        .set_enable_experimental_virtual_column(0)?;
    fixture.create_default_database().await?;
    fixture.create_variant_table().await?;
    fixture
        .execute_command(&format!(
            "alter table {}.{} set options(enable_virtual_column = false)",
            fixture.default_db_name(),
            fixture.default_table_name()
        ))
        .await?;

    let number_of_block = 2;
    append_variant_sample_data(number_of_block, &fixture).await?;

    fixture
        .default_session()
        .get_settings()
        .set_enable_experimental_virtual_column(1)?;
    fixture
        .execute_command(&format!(
            "alter table {}.{} set options(enable_virtual_column = true)",
            fixture.default_db_name(),
            fixture.default_table_name()
        ))
        .await?;

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
        complete_executor.execute().await?;
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

// Inject a legacy _vb/ directory with a file. Vacuum prepare should not remove
// files before the commit/cleanup phase runs.
#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_vacuum_virtual_column_prepares_without_cleanup() -> anyhow::Result<()> {
    let fixture = setup_table_with_virtual_columns(1).await?;

    let ctx = fixture.new_query_ctx().await?;
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let dal = fuse_table.get_operator_ref();

    let prefix = fuse_table
        .meta_location_generator()
        .prefix()
        .trim_start_matches('/');
    let v1_prefix = format!("{}/{}/", prefix, FUSE_TBL_VIRTUAL_BLOCK_PREFIX_V1);
    dal.create_dir(&v1_prefix).await?;
    let legacy_file = format!("{}legacy-file", v1_prefix);
    dal.write(&legacy_file, "legacy").await?;
    assert!(dal.exists(&legacy_file).await?);

    let (vacuum_result, pipeline_is_empty) =
        prepare_vacuum_virtual_column_for_test(ctx, fuse_table).await?;
    assert!(!vacuum_result.need_commit);
    assert!(vacuum_result.need_cleanup);
    assert_eq!(vacuum_result.removed_files, 0);
    assert!(pipeline_is_empty);
    assert!(dal.exists(&legacy_file).await?);
    Ok(())
}

// Simulate a table with both _vb_v2 blocks and one legacy _vb block (by tampering
// a block's virtual_location prefix). Vacuum prepare should build a commit
// pipeline but should not change metadata until that pipeline is executed.
#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_vacuum_virtual_column_prepares_legacy_block_commit() -> anyhow::Result<()> {
    let fixture = setup_table_with_virtual_columns(2).await?;

    let table = fixture.latest_default_table().await?;
    let original_schema = table.get_table_info().meta.virtual_schema.clone();
    assert!(original_schema.is_some());

    // Tamper block[0] in segment[0]: change _vb_v2 prefix to _vb (legacy).
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let operator = fuse_table.get_operator();
    let snapshot = fuse_table.read_table_snapshot().await?.unwrap();
    let reader = MetaReaders::segment_info_reader(fuse_table.get_operator(), table.schema());
    let (seg_loc, seg_ver) = snapshot.segments[0].clone();

    let seg = reader
        .read(&LoadParams {
            location: seg_loc.clone(),
            len_hint: None,
            ver: seg_ver,
            put_cache: false,
        })
        .await?;

    let mut seg_info = SegmentInfo::try_from(seg.as_ref())?;
    let mut blocks = seg_info.blocks.clone();
    let mut bm = Arc::unwrap_or_clone(blocks[0].clone());
    let vbm = bm
        .virtual_block_meta
        .as_mut()
        .expect("block should have virtual_block_meta");
    vbm.virtual_location.0 = vbm.virtual_location.0.replace(
        FUSE_TBL_VIRTUAL_BLOCK_PREFIX,
        FUSE_TBL_VIRTUAL_BLOCK_PREFIX_V1,
    );
    blocks[0] = Arc::new(bm);
    seg_info.blocks = blocks;
    seg_info.format_version = SegmentInfo::VERSION;

    let loc_gen = fuse_table.meta_location_generator();
    let new_seg_loc =
        loc_gen.gen_segment_info_location(TestFixture::default_table_meta_timestamps(), false);
    seg_info.write_meta(&operator, &new_seg_loc).await?;

    // Commit a new snapshot with the tampered segment appended.
    let ctx = fixture.new_query_ctx().await?;
    let mut new_snap = TableSnapshot::try_from_previous(
        snapshot.clone(),
        fuse_table.cluster_key_meta(),
        Some(fuse_table.get_table_info().ident.seq),
        TestFixture::default_table_meta_timestamps(),
    )?;
    let mut segs = snapshot.segments.clone();
    segs.push((new_seg_loc, SegmentInfo::VERSION));
    new_snap.segments = segs;
    new_snap.summary = merge_statistics(snapshot.summary.clone(), &seg_info.summary, None);
    fuse_table
        .commit_to_meta_server(
            ctx.as_ref(),
            fuse_table.get_table_info(),
            loc_gen,
            new_snap,
            None,
            &None,
            &operator,
        )
        .await?;

    // Prepare vacuum without executing the commit pipeline.
    let latest_table = fixture.latest_default_table().await?;
    let latest_fuse = FuseTable::try_from_table(latest_table.as_ref())?;
    let vacuum_ctx = fixture.new_query_ctx().await?;
    let (vacuum_result, pipeline_is_empty) =
        prepare_vacuum_virtual_column_for_test(vacuum_ctx, latest_fuse).await?;
    assert!(vacuum_result.need_commit);
    assert!(vacuum_result.need_cleanup);
    assert_eq!(vacuum_result.removed_files, 1);
    assert!(!pipeline_is_empty);

    // Verify metadata is unchanged because the commit pipeline was not executed.
    let table = fixture.latest_default_table().await?;
    let new_schema = table.get_table_info().meta.virtual_schema.clone();
    assert!(new_schema.is_some());
    assert_eq!(
        original_schema.unwrap().fields.len(),
        new_schema.unwrap().fields.len()
    );

    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let snap = fuse_table.read_table_snapshot().await?.unwrap();
    let reader = MetaReaders::segment_info_reader(fuse_table.get_operator(), table.schema());
    let mut legacy_ref_count = 0;
    for (loc, ver) in &snap.segments {
        let seg = reader
            .read(&LoadParams {
                location: loc.to_string(),
                len_hint: None,
                ver: *ver,
                put_cache: false,
            })
            .await?;
        for bm in seg.block_metas()? {
            if let Some(vbm) = &bm.virtual_block_meta
                && TableMetaLocationGenerator::is_legacy_virtual_block_location(
                    &vbm.virtual_location.0,
                )
            {
                legacy_ref_count += 1;
            }
        }
    }
    assert_eq!(legacy_ref_count, 1);
    Ok(())
}
