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

use std::collections::BTreeMap;
use std::iter::once;
use std::sync::Arc;

use databend_common_ast::ast::TableIndexType as AstTableIndexType;
use databend_common_catalog::plan::InvertedIndexInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_pipeline::core::Pipeline;
use databend_common_sql::plans::RefreshTableIndexPlan;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::SegmentInfoReader;
use databend_common_storages_fuse::io::read::InvertedIndexReader;
use databend_common_storages_fuse::io::read::bloom::block_filter_reader::load_bloom_filter_by_columns;
use databend_common_storages_fuse::io::read::bloom::block_filter_reader::load_index_meta;
use databend_common_storages_fuse::io::read::load_spatial_index_files;
use databend_common_storages_fuse::io::read::load_spatial_index_meta;
use databend_common_storages_fuse::io::read::read_segment_stats;
use databend_common_storages_fuse::operations::commit_refresh_virtual_column;
use databend_common_storages_fuse::operations::do_refresh_table_index;
use databend_common_storages_fuse::operations::prepare_refresh_virtual_column;
use databend_common_storages_fuse::pruning::create_inverted_index_query;
use databend_query::interpreters::Interpreter;
use databend_query::interpreters::RefreshTableIndexInterpreter;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelineCompleteExecutor;
use databend_query::sessions::TableContext;
use databend_query::sessions::TableContextProgress;
use databend_query::sessions::TableContextSettings;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::append_string_sample_data;
use databend_query::test_kits::*;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_cache::CachePolicy;
use databend_storages_common_cache::Reader;
use databend_storages_common_index::INVERTED_INDEX_FILE_FORMAT_VERSION;
use databend_storages_common_index::InvertedIndexBundleFooter;
use databend_storages_common_index::filters::Filter;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::MetaEncoding;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::VACUUM2_OBJECT_KEY_PREFIX;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::format::MetaCompression;
use databend_storages_common_table_meta::meta::trim_object_prefix;
use databend_storages_common_table_meta::table::TableCompression;
use futures_util::TryStreamExt;
use geo_index::rtree::RTreeBuilder;
use geo_index::rtree::sort::HilbertSort;
use parquet::file::metadata::KeyValue;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_refresh_inverted_index() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;

    fixture
        .default_session()
        .get_settings()
        .set_data_retention_time_in_days(0)?;
    fixture.create_default_database().await?;
    fixture.create_string_table().await?;

    let number_of_block = 2;
    append_string_sample_data(number_of_block, &fixture).await?;

    let table = fixture.latest_default_table().await?;

    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;
    let table_id = table.get_id();
    let index_name = "idx1".to_string();
    let mut options = BTreeMap::new();
    options.insert("tokenizer".to_string(), "english".to_string());
    options.insert(
        "filters".to_string(),
        "english_stop,english_stemmer,chinese_stop".to_string(),
    );
    let tenant = ctx.get_tenant();

    let req = CreateTableIndexReq {
        create_option: CreateOption::Create,
        table_id,
        tenant,
        name: index_name.clone(),
        column_ids: vec![0, 1],
        sync_creation: false,
        options: options.clone(),
        index_type: TableIndexType::Inverted,
    };

    let res = catalog.create_table_index(req).await;
    assert!(res.is_ok());

    let refresh_index_plan = RefreshTableIndexPlan {
        index_type: AstTableIndexType::Inverted,
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
        index_name: index_name.clone(),
        segment_locs: None,
    };
    let interpreter = RefreshTableIndexInterpreter::try_create(ctx.clone(), refresh_index_plan)?;
    let _ = interpreter
        .execute(ctx.clone())
        .await?
        .try_collect::<Vec<_>>()
        .await?;

    let new_table = table.refresh(ctx.as_ref()).await?;
    let new_fuse_table = FuseTable::create_without_refresh_table_info(
        new_table.get_table_info().clone(),
        ctx.get_settings().get_s3_storage_class()?,
    )?;
    let table_schema = new_fuse_table.schema();

    // get index location from new table snapshot
    let new_snapshot = new_fuse_table.read_table_snapshot().await?;
    assert!(new_snapshot.is_some());
    let new_snapshot = new_snapshot.unwrap();

    let table_info = new_table.get_table_info();
    let table_indexes = &table_info.meta.indexes;
    let table_index = table_indexes.get(&index_name);
    assert!(table_index.is_some());
    let table_index = table_index.unwrap();
    let index_version = table_index.version.clone();

    let segment_reader =
        SegmentInfoReader::new(new_fuse_table.get_operator(), table_schema.clone());

    let mut block_metas = vec![];
    for (segment_loc, ver) in &new_snapshot.segments {
        let segment_info = segment_reader
            .read(&(segment_loc.to_string(), *ver), &CachePolicy::BYPASS)
            .await?;
        for block_meta in segment_info.block_metas()? {
            block_metas.push(block_meta);
        }
    }
    assert_eq!(block_metas.len(), 1);
    let block_meta = &block_metas[0];

    let dal = new_fuse_table.get_operator_ref();
    let query_fields = vec![("title".to_string(), None), ("content".to_string(), None)];
    let index_schema = DataSchema::new(vec![
        DataField::new("title", DataType::String),
        DataField::new("content", DataType::String),
    ]);

    let index_meta = block_meta
        .inverted_index_metas
        .as_deref()
        .and_then(|metas| metas.iter().find(|meta| meta.index_name == index_name))
        .expect("refreshed block must contain explicit inverted-index metadata");
    assert_eq!(index_meta.index_version, index_version);
    assert_eq!(index_meta.location.1, INVERTED_INDEX_FILE_FORMAT_VERSION);
    let index_loc = &index_meta.location.0;
    assert!(index_loc.contains(&format!("/_i_i_v2/{index_version}/")));
    let index_file = index_loc.rsplit('/').next().unwrap();
    let index_object_id = index_file.strip_suffix(".index").unwrap();
    assert!(index_object_id.starts_with(VACUUM2_OBJECT_KEY_PREFIX));
    let index_uuid = trim_object_prefix(index_object_id);
    assert_eq!(index_uuid.len(), 32);
    assert!(index_uuid.chars().all(|ch| ch.is_ascii_hexdigit()));
    let block_file = block_meta.location.0.rsplit('/').next().unwrap();
    let block_object_id = trim_object_prefix(block_file).split('_').next().unwrap();
    assert_ne!(index_uuid, block_object_id);
    let bundle = dal.read(index_loc).await?.to_bytes();
    assert_eq!(u64::try_from(bundle.len()).unwrap(), index_meta.size);
    let footer = InvertedIndexBundleFooter::open(bundle.as_ref())?;
    assert!(!footer.managed_json.is_empty());
    assert!(!footer.meta_json.is_empty());

    let has_score = true;

    let queries = vec![
        ("rust".to_string(), vec![0, 1]),
        ("java".to_string(), vec![2]),
        ("data".to_string(), vec![4, 1, 5]),
    ];

    for (query_text, ids) in queries.into_iter() {
        let inverted_index_info = InvertedIndexInfo {
            index_name: index_name.clone(),
            index_version: index_version.clone(),
            index_options: options.clone(),
            index_schema: index_schema.clone(),
            query_fields: query_fields.clone(),
            query_text,
            has_score,
            inverted_index_option: None,
        };

        let prepared = create_inverted_index_query(&inverted_index_info, None)?;

        let index_reader = InvertedIndexReader::create(
            dal.clone(),
            CachePolicy::DEFAULT,
            has_score,
            prepared.tokenizer_manager,
            prepared.warmup,
        );

        let matched_rows = index_reader
            .do_filter(
                prepared.query.box_clone(),
                index_loc,
                index_meta.location.1,
                index_meta.size,
                block_meta.row_count,
            )
            .await?;
        assert!(matched_rows.is_some());
        let (matched_rows, _) = matched_rows.unwrap();
        assert_eq!(matched_rows.len(), ids.len());
        for (matched_row, id) in matched_rows.iter().zip(ids.iter()) {
            assert_eq!(matched_row, id);
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partial_compact_refreshes_untouched_blocks() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let settings = fixture.default_session().get_settings();
    settings.set_setting("enable_compact_after_write".into(), "0".into())?;
    fixture.create_default_database().await?;
    let database = fixture.default_db_name();
    let table_name = fixture.default_table_name();
    fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{table_name} (content STRING) \
             ROW_PER_BLOCK = 1000 BLOCK_PER_SEGMENT = 1000"
        ))
        .await?;
    for word in ["alpha", "beta", "gamma"] {
        fixture
            .execute_command(&format!(
                "INSERT INTO {database}.{table_name} VALUES ('{word}')"
            ))
            .await?;
    }
    // Existing blocks lack the ASYNC index, so refresh must cover both untouched
    // blocks and Compact outputs; the block writer cannot mask missing hints.
    fixture
        .execute_command(&format!(
            "CREATE ASYNC INVERTED INDEX idx ON {database}.{table_name}(content)"
        ))
        .await?;
    settings.set_setting("enable_compact_after_write".into(), "1".into())?;
    settings.set_auto_compaction_imperfect_blocks_threshold(1)?;
    let ctx = fixture.new_query_ctx().await?;
    let table = ctx.get_table("default", &database, &table_name).await?;
    let before = FuseTable::try_from_table(table.as_ref())?
        .read_table_snapshot()
        .await?
        .unwrap();
    assert_eq!(before.segments.len(), 3);
    // Model accumulated write hints, as when async hooks coalesce writes.
    ctx.written_segment_locations()
        .extend(before.segments.iter().cloned());
    // A threshold of 1 gives a two-block hint. Compact includes one boundary
    // segment, rewriting three of the four segments and leaving one untouched.
    TestFixture::execute_query_with_ctx(
        ctx.clone(),
        &format!("INSERT INTO {database}.{table_name} VALUES ('delta')"),
    )
    .await?
    .try_collect::<Vec<_>>()
    .await?;

    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let snapshot = fuse_table.read_table_snapshot().await?.unwrap();
    assert_eq!(snapshot.summary.row_count, 4);
    assert_eq!(snapshot.summary.block_count, 2);
    // Compact is complete at statement return; index coverage is now asynchronous.
    let blocks = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let (_, blocks) = refresh_batch_blocks(&fixture).await?;
            if blocks.iter().all(|block| {
                block
                    .inverted_index_metas
                    .as_deref()
                    .is_some_and(|metas| metas.iter().any(|meta| meta.index_name == "idx"))
            }) {
                break anyhow::Ok(blocks);
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await??;
    let mut block_rows = blocks
        .iter()
        .map(|block| block.row_count)
        .collect::<Vec<_>>();
    block_rows.sort_unstable();
    assert_eq!(block_rows, vec![1, 3]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_batch_shared_reads_and_hint_scope() -> anyhow::Result<()> {
    for mode in [
        RefreshBatchMode::VirtualMetadata,
        RefreshBatchMode::StaleAndValid,
    ] {
        let fixture = refresh_batch_fixture(mode).await?;
        let (before, before_blocks) = refresh_batch_blocks(&fixture).await?;
        let before_stats = refresh_batch_non_index_metadata(&fixture).await?;
        let before_filters = refresh_batch_filters(&fixture).await?;
        assert_eq!(before_blocks.len(), 3);
        assert_eq!(
            before_blocks[0].virtual_block_meta.is_some(),
            mode == RefreshBatchMode::VirtualMetadata
        );
        let ctx = fixture.new_query_ctx().await?;
        let table = fixture.latest_default_table().await?;
        let fuse = FuseTable::try_from_table(table.as_ref())?;
        let names = table
            .get_table_info()
            .meta
            .indexes
            .values()
            .filter(|index| !index.sync_creation)
            .map(|index| index.name.clone())
            .collect::<Vec<_>>();
        let mut pipeline = Pipeline::create();
        assert_eq!(
            fuse.do_refresh_indexes(ctx.clone(), &names, Some(vec![]), &mut pipeline)
                .await?,
            0
        );
        assert!(pipeline.is_empty());
        assert_eq!(ctx.get_scan_progress().get_values().rows, 0);
        assert_eq!(
            refresh_batch_blocks(&fixture).await?.0.snapshot_id,
            before.snapshot_id
        );

        let mut previous_snapshot = before.snapshot_id;
        for segments in [&before.segments[..1], &before.segments[1..]] {
            let ctx = fixture.new_query_ctx().await?;
            let table = fixture.latest_default_table().await?;
            let fuse = FuseTable::try_from_table(table.as_ref())?;
            let mut pipeline = Pipeline::create();
            // Duplicate and stale hints must neither duplicate work nor broaden its scope.
            let mut hints = segments.to_vec();
            hints.extend_from_slice(segments);
            hints.push(("not-in-current-snapshot".into(), 4));
            assert_eq!(
                fuse.do_refresh_indexes(ctx.clone(), &names, Some(hints), &mut pipeline)
                    .await?,
                segments.len() as u64
            );
            // Retain the sum of per-index budgets after fan-in, not just the lane count.
            let indexes_per_block = if mode == RefreshBatchMode::VirtualMetadata {
                8
            } else {
                4
            };
            assert_eq!(
                pipeline.get_max_threads(),
                indexes_per_block * segments.len()
            );
            PipelineCompleteExecutor::try_create(
                pipeline,
                ExecutorSettings::try_create(ctx.clone())?,
            )?
            .execute()
            .await?;
            assert_eq!(
                ctx.get_scan_progress().get_values().rows,
                segments.len() * 2
            );
            // Each row also has a 64-KiB unrelated column, which must not be read.
            assert!(ctx.get_scan_progress().get_values().bytes < 16 * 1024);
            let (published, blocks) = refresh_batch_blocks(&fixture).await?;
            assert_eq!(published.prev_snapshot_id.unwrap().0, previous_snapshot);
            previous_snapshot = published.snapshot_id;
            if segments.len() == 1 {
                for old in &before_blocks[1..] {
                    assert_eq!(
                        blocks.iter().find(|block| block.location == old.location),
                        Some(old)
                    );
                }
            }
        }
        let (after, after_blocks) = refresh_batch_blocks(&fixture).await?;
        assert_eq!(after.summary.row_count, before.summary.row_count);
        assert_eq!(
            refresh_batch_non_index_metadata(&fixture).await?,
            before_stats
        );
        let table = fixture.latest_default_table().await?;
        let fuse = FuseTable::try_from_table(table.as_ref())?;
        for block in &after_blocks {
            assert_eq!(block.inverted_index_metas.as_ref().unwrap().len(), 2);
            assert_eq!(block.vector_index_metas.as_ref().unwrap().len(), 2);
            // Changing the distance adds statistics without dropping the old distance.
            assert_eq!(
                block.vector_stats.as_ref().unwrap().len(),
                if mode == RefreshBatchMode::VirtualMetadata {
                    2
                } else {
                    3
                }
            );
            // Statistics of geometries outside the current index are retained too.
            assert_eq!(block.spatial_stats.as_ref().unwrap().len(), 3);
            let bloom_location = block.bloom_filter_index_location.as_ref().unwrap();
            let old = before_blocks
                .iter()
                .find(|old| old.location == block.location)
                .unwrap();
            // Published Ngram replacements are immutable; old snapshots retain their file.
            assert_ne!(
                Some(bloom_location),
                old.bloom_filter_index_location.as_ref()
            );
            assert!(
                bloom_location
                    .0
                    .starts_with(&fuse.meta_location_generator().bloom_refresh_prefix())
            );
            let bloom = load_index_meta(
                fuse.get_operator(),
                &bloom_location.0,
                block.bloom_filter_index_size,
                None,
                CachePolicy::BYPASS,
            )
            .await?;
            let gram = if mode == RefreshBatchMode::VirtualMetadata {
                3
            } else {
                4
            };
            for name in [
                "Ngram(0)_3_1048576".to_string(),
                format!("Ngram(1)_{gram}_1048576"),
            ] {
                assert!(bloom.columns.iter().any(|(column, _)| column == &name));
            }
            let spatial = load_spatial_index_meta(
                fuse.get_operator(),
                &block.spatial_index_location.as_ref().unwrap().0,
                CachePolicy::BYPASS,
            )
            .await?;
            for name in ["geo0", "geo1"] {
                assert_eq!(
                    spatial.metadata[name],
                    table.get_table_info().meta.indexes[name].version
                );
            }
            if mode != RefreshBatchMode::VirtualMetadata {
                for (key, value) in old.vector_stats.as_ref().unwrap() {
                    assert_eq!(block.vector_stats.as_ref().unwrap().get(key), Some(value));
                }
                assert_eq!(
                    block.inverted_index_meta("inv0"),
                    old.inverted_index_meta("inv0")
                );
                assert_eq!(
                    block.vector_index_meta("vec0"),
                    old.vector_index_meta("vec0")
                );
                for (name, new, old) in [
                    (
                        "inv1",
                        block.inverted_index_meta("inv1"),
                        old.inverted_index_meta("inv1"),
                    ),
                    (
                        "vec1",
                        block.vector_index_meta("vec1"),
                        old.vector_index_meta("vec1"),
                    ),
                ] {
                    assert_ne!(new, old);
                    assert_eq!(
                        new.unwrap().index_version,
                        table.get_table_info().meta.indexes[name].version
                    );
                }
            }
        }
        // Ordinary Bloom (including unprojected columns) and prebuilt Ngram
        // payloads must survive unchanged; obsolete Ngram definitions are removed.
        let after_filters = refresh_batch_filters(&fixture).await?;
        for (block, filters) in before_filters {
            assert!(filters.keys().any(|name| name.starts_with("Bloom(")));
            for (name, bytes) in filters {
                if mode == RefreshBatchMode::StaleAndValid && name == "Ngram(1)_3_1048576" {
                    assert!(!after_filters[&block].contains_key(&name));
                } else {
                    assert_eq!(after_filters[&block][&name], bytes);
                }
            }
        }
        let mut pipeline = Pipeline::create();
        let ctx = fixture.new_query_ctx().await?;
        assert_eq!(
            fuse.do_refresh_indexes(
                ctx.clone(),
                &names,
                Some(after.segments.clone()),
                &mut pipeline
            )
            .await?,
            0
        );
        assert!(pipeline.is_empty());
        assert_eq!(ctx.get_scan_progress().get_values().rows, 0);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_batch_conflict_does_not_publish_snapshot() -> anyhow::Result<()> {
    let fixture = refresh_batch_fixture(RefreshBatchMode::StaleAndValid).await?;
    let (before, _) = refresh_batch_blocks(&fixture).await?;
    let ctx = fixture.new_query_ctx().await?;
    let table = fixture.latest_default_table().await?;
    let fuse = FuseTable::try_from_table(table.as_ref())?;
    let names = table
        .get_table_info()
        .meta
        .indexes
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    let mut pipeline = Pipeline::create();
    fuse.do_refresh_indexes(
        ctx.clone(),
        &names,
        Some(before.segments.clone()),
        &mut pipeline,
    )
    .await?;
    // Metadata-only DDL invalidates the table version but does not publish a new snapshot.
    let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;
    catalog
        .create_table_index(CreateTableIndexReq {
            create_option: CreateOption::Create,
            tenant: ctx.get_tenant(),
            table_id: table.get_id(),
            name: "concurrent".into(),
            column_ids: vec![8],
            sync_creation: true,
            options: BTreeMap::new(),
            index_type: TableIndexType::Inverted,
        })
        .await?;
    assert!(
        PipelineCompleteExecutor::try_create(pipeline, ExecutorSettings::try_create(ctx)?)?
            .execute()
            .await
            .is_err()
    );
    let (failed, _) = refresh_batch_blocks(&fixture).await?;
    assert_eq!(failed.snapshot_id, before.snapshot_id);
    // Immutable Ngram artifacts cannot alter files referenced by the failed snapshot.
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_background_refresh_batch_bounds_work_and_keeps_old_bloom() -> anyhow::Result<()> {
    let fixture = refresh_batch_fixture(RefreshBatchMode::StaleAndValid).await?;
    let (before, blocks) = refresh_batch_blocks(&fixture).await?;
    let table = fixture.latest_default_table().await?;
    let fuse = FuseTable::try_from_table(table.as_ref())?;
    let operator = fuse.get_operator();
    let mut old_files = Vec::new();
    for block in &blocks {
        let path = &block.bloom_filter_index_location.as_ref().unwrap().0;
        old_files.push((path.clone(), operator.read(path).await?.to_vec()));
    }
    let names = table
        .get_table_info()
        .meta
        .indexes
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    let ctx = fixture.new_query_ctx().await?;
    let mut pipeline = Pipeline::create();
    assert_eq!(
        fuse.do_refresh_indexes_batch(
            ctx.clone(),
            &names,
            before.segments.clone(),
            &mut pipeline,
            (1, u64::MAX)
        )
        .await?,
        1
    );
    PipelineCompleteExecutor::try_create(pipeline, ExecutorSettings::try_create(ctx)?)?
        .execute()
        .await?;
    let (_, after) = refresh_batch_blocks(&fixture).await?;
    assert_eq!(
        after
            .iter()
            .zip(&blocks)
            .filter(|(new, old)| new.bloom_filter_index_location != old.bloom_filter_index_location)
            .count(),
        1
    );
    for (path, bytes) in old_files {
        assert_eq!(operator.read(&path).await?.to_vec(), bytes);
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RefreshBatchMode {
    VirtualMetadata,
    StaleAndValid,
}

async fn refresh_batch_fixture(mode: RefreshBatchMode) -> anyhow::Result<TestFixture> {
    let fixture = TestFixture::setup().await?;
    let settings = fixture.default_session().get_settings();
    settings.set_max_threads(2)?;
    settings.set_enable_experimental_virtual_column(1)?;
    settings.set_setting("enable_compact_after_write".into(), "0".into())?;
    fixture.create_default_database().await?;
    let name = format!(
        "{}.{}",
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    fixture
        .execute_command(&format!(
            "CREATE TABLE {name} (a STRING, b STRING, v1 VECTOR(4), v2 VECTOR(4), \
         g1 GEOMETRY, g2 GEOMETRY, unused STRING, payload VARIANT, wide_unused STRING, g3 GEOMETRY) \
         enable_virtual_column = true approx_distinct_columns = 'a,unused' \
         analyze_frequency_columns = 'unused' analyze_top_n_size = 3"
        ))
        .await?;
    for word in ["alpha", "beta", "gamma"] {
        fixture
            .execute_command(&format!(
                "INSERT INTO {name} VALUES ('{word}', 'delta', [1, 2, 3, 4], [4, 3, 2, 1], \
             st_geomfromtext('POINT(0 0)'), st_geomfromtext('POINT(1 1)'), 'unused', \
             parse_json('{{\"word\":\"{word}\"}}'), repeat('x', 65536), st_geomfromtext('POINT(2 2)')), \
             ('{word} other', 'epsilon', [1, 2, 3, 4], [4, 3, 2, 1], \
             st_geomfromtext('POINT(0 0)'), st_geomfromtext('POINT(1 1)'), 'other', \
             parse_json('{{}}'), repeat('y', 65536), st_geomfromtext('POINT(2 2)'))"
            ))
            .await?;
    }
    let ctx = fixture.new_query_ctx().await?;
    let table = fixture.latest_default_table().await?;
    if mode == RefreshBatchMode::VirtualMetadata {
        // Exercise segment-local virtual schemas separately from Top-N: the
        // existing virtual-column refresh does not retain block Top-N yet.
        let fuse = FuseTable::try_from_table(table.as_ref())?;
        let virtuals = prepare_refresh_virtual_column(ctx.clone(), fuse, None, true, None).await?;
        assert_eq!(virtuals.len(), 3);
        let mut pipeline = Pipeline::create();
        commit_refresh_virtual_column(ctx.clone(), fuse, &mut pipeline, virtuals).await?;
        PipelineCompleteExecutor::try_create(pipeline, ExecutorSettings::try_create(ctx.clone())?)?
            .execute()
            .await?;
    }
    for (kind, prefix, columns) in [
        ("INVERTED", "inv", ["a", "b"]),
        ("NGRAM", "ngram", ["a", "b"]),
        ("VECTOR", "vec", ["v1", "v2"]),
        ("SPATIAL", "geo", ["g1", "g2"]),
    ] {
        for (position, column) in columns.into_iter().enumerate() {
            let options = if kind == "VECTOR" {
                "distance='l2'"
            } else {
                ""
            };
            fixture
                .execute_command(&format!(
                    "CREATE ASYNC {kind} INDEX {prefix}{position} ON {name}({column}) {options}"
                ))
                .await?;
        }
    }
    if mode != RefreshBatchMode::VirtualMetadata {
        // Each type has one valid sibling and one stale definition to rebuild.
        for (kind, prefix) in [
            ("INVERTED", "inv"),
            ("NGRAM", "ngram"),
            ("VECTOR", "vec"),
            ("SPATIAL", "geo"),
        ] {
            for position in 0..2 {
                fixture
                    .execute_command(&format!(
                        "REFRESH {kind} INDEX {prefix}{position} ON {name}"
                    ))
                    .await?;
            }
        }
        for (kind, index, definition) in [
            ("INVERTED", "inv1", "(unused)"),
            ("NGRAM", "ngram1", "(b) gram_size=4"),
            ("VECTOR", "vec1", "(v2) distance='cosine'"),
            ("SPATIAL", "geo1", "(g3)"),
        ] {
            fixture
                .execute_command(&format!(
                    "CREATE OR REPLACE ASYNC {kind} INDEX {index} ON {name}{definition}"
                ))
                .await?;
        }
    }
    Ok(fixture)
}

// Each fixture segment contains one block. Compare segment-local virtual schemas
// and serialized HLL/Top-N by unchanged block location, not rewritten segment id.
async fn refresh_batch_non_index_metadata(
    fixture: &TestFixture,
) -> anyhow::Result<BTreeMap<String, serde_json::Value>> {
    let table = fixture.latest_default_table().await?;
    let fuse = FuseTable::try_from_table(table.as_ref())?;
    let snapshot = fuse.read_table_snapshot().await?.unwrap();
    let reader = SegmentInfoReader::new(fuse.get_operator(), table.schema());
    let mut result = BTreeMap::new();
    for location in &snapshot.segments {
        let segment = reader.read(location, &CachePolicy::BYPASS).await?;
        let blocks = segment.block_metas()?;
        assert_eq!(blocks.len(), 1);
        assert_eq!(
            blocks[0].virtual_block_meta.is_some(),
            segment.summary.virtual_segment_schema.is_some()
        );
        let stats = read_segment_stats(
            fuse.get_operator(),
            segment.summary.additional_stats_loc().unwrap(),
            CachePolicy::BYPASS,
        )
        .await?;
        assert!(!stats.block_hlls[0].is_empty());
        if blocks[0].virtual_block_meta.is_none() {
            assert!(stats.block_top_ns[0].contains_key(&6));
        }
        result.insert(
            blocks[0].location.0.clone(),
            serde_json::to_value((
                &segment.summary.virtual_segment_schema,
                &segment.summary.virtual_col_stats,
                stats.as_ref(),
            ))?,
        );
    }
    Ok(result)
}

// Read serialized filter payloads, not the whole file: refresh may append new
// Ngram columns in place, but must not rebuild ordinary Bloom or valid siblings.
async fn refresh_batch_filters(
    fixture: &TestFixture,
) -> anyhow::Result<BTreeMap<String, BTreeMap<String, Vec<u8>>>> {
    let table = fixture.latest_default_table().await?;
    let fuse = FuseTable::try_from_table(table.as_ref())?;
    let ctx = fixture.new_query_ctx().await?;
    let context: Arc<dyn TableContext> = ctx;
    let mut result = BTreeMap::new();
    for block in refresh_batch_blocks(fixture).await?.1 {
        let location = block.bloom_filter_index_location.as_ref().unwrap();
        let meta = load_index_meta(
            fuse.get_operator(),
            &location.0,
            block.bloom_filter_index_size,
            None,
            CachePolicy::BYPASS,
        )
        .await?;
        let names = meta
            .columns
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        let bloom = load_bloom_filter_by_columns(
            fuse.get_operator(),
            &ReadSettings::from_ctx(&context)?,
            &names,
            &location.0,
            block.bloom_filter_index_size,
            CachePolicy::BYPASS,
        )
        .await?;
        let filters = bloom
            .filter_schema
            .fields()
            .iter()
            .zip(bloom.filters.iter())
            .map(|(field, filter)| Ok((field.name().clone(), filter.to_bytes()?)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        result.insert(block.location.0.clone(), filters);
    }
    Ok(result)
}

async fn refresh_batch_blocks(
    fixture: &TestFixture,
) -> anyhow::Result<(Arc<TableSnapshot>, Vec<Arc<BlockMeta>>)> {
    let table = fixture.latest_default_table().await?;
    let fuse = FuseTable::try_from_table(table.as_ref())?;
    let snapshot = fuse.read_table_snapshot().await?.unwrap();
    let reader = SegmentInfoReader::new(fuse.get_operator(), table.schema());
    let mut blocks = Vec::new();
    for location in &snapshot.segments {
        blocks.extend(
            reader
                .read(location, &CachePolicy::BYPASS)
                .await?
                .block_metas()?,
        );
    }
    Ok((snapshot, blocks))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_batch_legacy_spatial_statistics_and_indexes() -> anyhow::Result<()> {
    // Cover building a missing ordinary tree and retaining a legacy origin tree.
    for (origin, indexed) in [(false, false), (true, true)] {
        let (min, max) = if origin { (0.0, 0.0) } else { (1.0, 3.0) };
        let (fixture, name) = legacy_spatial_fixture(
            "g GEOMETRY",
            &format!("(to_geometry('POINT({min} {min})')), (to_geometry('POINT({max} {max})'))"),
        )
        .await?;
        let table = fixture.latest_default_table().await?;
        let fuse = FuseTable::try_from_table(table.as_ref())?;
        let (_, blocks) = refresh_batch_blocks(&fixture).await?;
        let mut block = (*blocks[0]).clone();
        if indexed {
            let mut builder = RTreeBuilder::<f64>::new(2);
            builder.add(min, min, min, min);
            builder.add(max, max, max, max);
            let tree = builder.finish::<HilbertSort>();
            // The legacy format stores a separate SRID column. The current
            // reader must project the tree without mistaking SRID for data.
            let schema = TableSchema::new(vec![
                TableField::new("0", TableDataType::Binary),
                TableField::new("0_srid", TableDataType::Number(NumberDataType::Int32)),
            ]);
            let data = DataBlock::new(
                vec![
                    BlockEntry::new_const_column_arg::<BinaryType>(tree.into_inner(), 1),
                    BlockEntry::new_const_column_arg::<Int32Type>(0, 1),
                ],
                1,
            );
            let serialized = blocks_to_parquet(
                &schema,
                vec![data],
                TableCompression::Zstd,
                false,
                Some(vec![KeyValue {
                    key: "geo".into(),
                    value: Some(table.get_table_info().meta.indexes["geo"].version.clone()),
                }]),
            )?;
            let location = fuse
                .meta_location_generator()
                .block_spatial_index_location();
            block.spatial_index_size = Some(serialized.len() as u64);
            fuse.get_operator()
                .write(&location.0, serialized.payload)
                .await?;
            block.spatial_index_location = Some(location);
        }
        let old_location = block.spatial_index_location.clone();
        replace_refresh_batch_legacy_spatial_block(&fixture, block).await?;
        let (before, blocks) = refresh_batch_blocks(&fixture).await?;
        let stat = &blocks[0].spatial_stats.as_ref().unwrap()[&0];
        assert!(!stat.is_valid);
        assert_eq!(stat.min_x.into_inner(), min);
        assert_eq!(stat.max_x.into_inner(), max);
        if origin {
            assert!(!stat.has_null);
            assert!(!stat.has_empty_rect);
        }
        let table = fixture.latest_default_table().await?;
        let fuse = FuseTable::try_from_table(table.as_ref())?;
        let ctx = fixture.new_query_ctx().await?;
        let mut pipeline = Pipeline::create();
        let names = vec!["geo".into()];
        assert_eq!(
            fuse.do_refresh_indexes(ctx.clone(), &names, None, &mut pipeline)
                .await?,
            u64::from(!indexed)
        );
        if indexed {
            assert!(pipeline.is_empty());
        } else {
            PipelineCompleteExecutor::try_create(
                pipeline,
                ExecutorSettings::try_create(ctx.clone())?,
            )?
            .execute()
            .await?;
            assert_eq!(ctx.get_scan_progress().get_values().rows, 2);
        }
        let (after, blocks) = refresh_batch_blocks(&fixture).await?;
        if indexed {
            assert_eq!(after.snapshot_id, before.snapshot_id);
            assert_eq!(blocks[0].spatial_index_location, old_location);
        } else {
            assert_eq!(after.prev_snapshot_id.unwrap().0, before.snapshot_id);
        }
        let location = blocks[0].spatial_index_location.as_ref().unwrap();
        let context: Arc<dyn TableContext> = ctx.clone();
        let columns = load_spatial_index_files(
            fuse.get_operator(),
            &ReadSettings::from_ctx(&context)?,
            &["0".into()],
            &location.0,
            CachePolicy::BYPASS,
        )
        .await?;
        assert!(!columns[0].index(0).unwrap().as_binary().unwrap().is_empty());
        if indexed {
            let columns = load_spatial_index_files(
                fuse.get_operator(),
                &ReadSettings::from_ctx(&context)?,
                &["0_srid".into(), "0".into()],
                &location.0,
                CachePolicy::BYPASS,
            )
            .await?;
            assert_eq!(columns[0].index(0).unwrap().to_string(), "0");
            assert!(!columns[1].index(0).unwrap().as_binary().unwrap().is_empty());
        }
        let contains = format!("ST_INTERSECTS(g,to_geometry('POINT({min} {min})'))");
        assert_eq!(
            refresh_batch_query_count(&fixture, &contains).await?,
            if origin { "2" } else { "1" }
        );
        let ctx = fixture.new_query_ctx().await?;
        let result = TestFixture::execute_query_with_ctx(
            ctx.clone(),
            &format!("SELECT count() FROM {name} WHERE ST_INTERSECTS(g,to_geometry('POINT(2 2)'))"),
        )
        .await?
        .try_collect::<Vec<_>>()
        .await?;
        assert_eq!(
            result[0].get_by_offset(0).index(0).unwrap().to_string(),
            "0"
        );
        // Legacy statistics decode to the original false default, keeping
        // conservative queries without affecting refresh completion checks.
        assert_eq!(
            ctx.get_scan_progress().get_values().rows,
            if indexed { 2 } else { 0 }
        );
        let table = fixture.latest_default_table().await?;
        let mut pipeline = Pipeline::create();
        assert_eq!(
            FuseTable::try_from_table(table.as_ref())?
                .do_refresh_indexes(fixture.new_query_ctx().await?, &names, None, &mut pipeline,)
                .await?,
            0
        );
        assert!(pipeline.is_empty());
        assert_eq!(
            refresh_batch_blocks(&fixture).await?.0.snapshot_id,
            after.snapshot_id
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_rejects_stale_internal_definitions() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {}.{} (a STRING, v VECTOR(4), g GEOMETRY)",
            fixture.default_db_name(),
            fixture.default_table_name()
        ))
        .await?;
    let name = format!(
        "{}.{}",
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    for (kind, index, definition) in [
        ("INVERTED", "inv0", "(a)"),
        ("NGRAM", "ngram0", "(a)"),
        ("VECTOR", "vec0", "(v) distance='l2'"),
        ("SPATIAL", "geo0", "(g)"),
    ] {
        fixture
            .execute_command(&format!(
                "CREATE ASYNC {kind} INDEX {index} ON {name}{definition}"
            ))
            .await?;
    }
    // SQL type mismatches are covered by sqllogic. These malformed definitions
    // can only be supplied by internal callers; reject them before any scan.
    let ctx = fixture.new_query_ctx().await?;
    let table = fixture.latest_default_table().await?;
    let fuse = FuseTable::try_from_table(table.as_ref())?;

    // Both single-index entries share schema validation after module extraction.
    // A full-table schema must not be accepted for a single-column definition.
    for name in ["inv0", "ngram0", "vec0", "geo0"] {
        let index = &table.get_table_info().meta.indexes[name];
        let mut pipeline = Pipeline::create();
        let result = if index.index_type == TableIndexType::Inverted {
            fuse.do_refresh_inverted_index(
                ctx.clone(),
                name.into(),
                index.version.clone(),
                &index.options,
                table.schema(),
                None,
                &mut pipeline,
            )
            .await
        } else {
            do_refresh_table_index(
                fuse,
                ctx.clone(),
                name.into(),
                index.index_type.clone(),
                table.schema(),
                None,
                &mut pipeline,
            )
            .await
        };
        let error = result.unwrap_err();
        assert_eq!(error.code(), 1602);
        assert!(
            error
                .to_string()
                .contains("definition changed before refresh")
        );
        assert!(pipeline.is_empty());
    }

    // The generic entry must keep rejecting Inverted; its dedicated entry owns
    // the version/options checks as well as the shared schema validation.
    let mut pipeline = Pipeline::create();
    let error = do_refresh_table_index(
        fuse,
        ctx.clone(),
        "inv0".into(),
        TableIndexType::Inverted,
        Arc::new(table.schema().project(&[0])),
        None,
        &mut pipeline,
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), ErrorCode::REFRESH_INDEX_ERROR);
    assert!(error.to_string().contains("not support"));
    assert!(pipeline.is_empty());

    // Inverted-specific version/options checks must remain in its entry point.
    let index = &table.get_table_info().meta.indexes["inv0"];
    for (version, options) in [
        (format!("{}-stale", index.version), index.options.clone()),
        (
            index.version.clone(),
            BTreeMap::from([("tokenizer".into(), "whitespace".into())]),
        ),
    ] {
        let mut pipeline = Pipeline::create();
        let error = fuse
            .do_refresh_inverted_index(
                ctx.clone(),
                "inv0".into(),
                version,
                &options,
                Arc::new(table.schema().project(&[0])),
                None,
                &mut pipeline,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), 1602);
        assert!(
            error
                .to_string()
                .contains("definition changed before refresh")
        );
        assert!(pipeline.is_empty());
    }
    assert_eq!(ctx.get_scan_progress().get_values().rows, 0);
    assert!(fuse.read_table_snapshot().await?.is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_preserves_legacy_spatial_query_semantics() -> anyhow::Result<()> {
    for (empty, indexed) in [
        ("POINT EMPTY", false),
        ("POLYGON EMPTY", false),
        ("POLYGON EMPTY", true),
    ] {
        let (fixture, name) = legacy_spatial_fixture(
            "g GEOMETRY, a STRING",
            &format!("(to_geometry('POINT(1 1)'),'alpha'),(to_geometry('{empty}'),'beta'),(NULL,'gamma'),(to_geometry('POINT(3 3)'),'delta')"),
        ).await?;
        fixture
            .execute_command(&format!("CREATE ASYNC NGRAM INDEX ng ON {name}(a)"))
            .await?;
        if indexed {
            fixture
                .execute_command(&format!("REFRESH SPATIAL INDEX geo ON {name}"))
                .await?;
        }
        let (_, blocks) = refresh_batch_blocks(&fixture).await?;
        let old_location = blocks[0].spatial_index_location.clone();
        replace_refresh_batch_legacy_spatial_block(&fixture, (*blocks[0]).clone()).await?;
        let mut predicates = vec![
            ("g IS NULL", "1"),
            ("ST_ASWKT(g) LIKE '%EMPTY%'", "1"),
            ("ST_INTERSECTS(g,to_geometry('POINT(1 1)'))", "1"),
            ("ST_INTERSECTS(g,to_geometry('POINT(2 2)'))", "0"),
            ("NOT ST_INTERSECTS(g,to_geometry('POINT(100 100)'))", "3"),
        ];
        if empty == "POLYGON EMPTY" {
            // In current scalar semantics a Polygon EMPTY has zero distance.
            predicates.extend([
                ("ST_DWITHIN(g,to_geometry('POINT(100 100)'),0)", "1"),
                ("ST_DWITHIN(g,to_geometry('POLYGON EMPTY'),0)", "3"),
                ("ST_DWITHIN(to_geometry('POLYGON EMPTY'),g,0)", "3"),
                ("NOT ST_DWITHIN(g,to_geometry('POINT(100 100)'),0)", "2"),
            ]);
        }
        // Check legacy semantics across unrelated refresh and Spatial planning.
        // As before, complete the POINT EMPTY refresh, but only plan the missing
        // POLYGON EMPTY tree: valid-statistics distance pruning has a separate
        // EMPTY-geometry issue, also reproducible without any Spatial index.
        for stage in 0..3 {
            if stage == 1 {
                fixture
                    .execute_command(&format!("REFRESH NGRAM INDEX ng ON {name}"))
                    .await?;
            } else if stage == 2 {
                let (before, _) = refresh_batch_blocks(&fixture).await?;
                let table = fixture.latest_default_table().await?;
                let ctx = fixture.new_query_ctx().await?;
                let mut pipeline = Pipeline::create();
                assert_eq!(
                    FuseTable::try_from_table(table.as_ref())?
                        .do_refresh_indexes(ctx.clone(), &["geo".into()], None, &mut pipeline)
                        .await?,
                    u64::from(!indexed)
                );
                assert_eq!(pipeline.is_empty(), indexed);
                assert_eq!(
                    refresh_batch_blocks(&fixture).await?.0.snapshot_id,
                    before.snapshot_id
                );
                if !indexed && empty == "POINT EMPTY" {
                    PipelineCompleteExecutor::try_create(
                        pipeline,
                        ExecutorSettings::try_create(ctx)?,
                    )?
                    .execute()
                    .await?;
                }
            }
            if stage < 2 || indexed || empty == "POLYGON EMPTY" {
                let (_, blocks) = refresh_batch_blocks(&fixture).await?;
                // Unrelated refreshes must not enable pruning with legacy stats.
                assert!(!blocks[0].spatial_stats.as_ref().unwrap()[&0].is_valid);
                assert_eq!(blocks[0].spatial_index_location, old_location);
            }
            for (predicate, expected) in &predicates {
                assert_eq!(
                    refresh_batch_query_count(&fixture, predicate).await?,
                    *expected,
                    "{empty}, indexed={indexed}, stage={stage}, predicate={predicate}"
                );
            }
            let error = async {
                fixture.execute_query(&format!(
                    "SELECT count() FROM {name} WHERE ST_INTERSECTS(g,to_geometry('SRID=4326;POINT(100 100)'))"
                )).await?.try_collect::<Vec<_>>().await
            }.await.unwrap_err();
            assert!(error.to_string().contains("Incompatible SRID"));
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_batch_failed_vector_upload_does_not_publish_snapshot() -> anyhow::Result<()> {
    let fixture = refresh_batch_fixture(RefreshBatchMode::StaleAndValid).await?;
    let (before, old_blocks) = refresh_batch_blocks(&fixture).await?;
    let before_stats = refresh_batch_non_index_metadata(&fixture).await?;
    let table = fixture.latest_default_table().await?;
    let fuse = FuseTable::try_from_table(table.as_ref())?;
    let operator = fuse.get_operator();
    let version = &table.get_table_info().meta.indexes["vec1"].version;
    let blocker = format!(
        "{}{}",
        fuse.meta_location_generator()
            .vector_index_v2_location_prefix(),
        version
    );
    // A real FS error only on vec1 bundle uploads; reads and other branches work.
    operator
        .write(&blocker, b"not-a-directory".to_vec())
        .await?;
    let names = vec!["inv1".into(), "vec1".into(), "ngram1".into(), "geo1".into()];
    let ctx = fixture.new_query_ctx().await?;
    let mut pipeline = Pipeline::create();
    assert_eq!(
        fuse.do_refresh_indexes(ctx.clone(), &names, None, &mut pipeline)
            .await?,
        3
    );
    let result =
        PipelineCompleteExecutor::try_create(pipeline, ExecutorSettings::try_create(ctx)?)?
            .execute()
            .await;
    let error = result.unwrap_err();
    assert!(error.to_string().contains(version));
    let (after, blocks) = refresh_batch_blocks(&fixture).await?;
    assert_eq!(after.snapshot_id, before.snapshot_id);
    assert_eq!(blocks, old_blocks);
    assert_eq!(
        refresh_batch_non_index_metadata(&fixture).await?,
        before_stats
    );
    operator.delete(&blocker).await?;
    let table = fixture.latest_default_table().await?;
    let ctx = fixture.new_query_ctx().await?;
    let mut pipeline = Pipeline::create();
    assert_eq!(
        FuseTable::try_from_table(table.as_ref())?
            .do_refresh_indexes(ctx.clone(), &names, None, &mut pipeline)
            .await?,
        3
    );
    PipelineCompleteExecutor::try_create(pipeline, ExecutorSettings::try_create(ctx)?)?
        .execute()
        .await?;
    let (after, blocks) = refresh_batch_blocks(&fixture).await?;
    assert_eq!(after.prev_snapshot_id.unwrap().0, before.snapshot_id);
    for block in blocks {
        assert!(block.vector_index_meta("vec1").is_some());
        assert!(block.inverted_index_meta("inv1").is_some());
    }
    Ok(())
}

// Share only setup; each regression keeps its geometry data and assertions local.
async fn legacy_spatial_fixture(
    columns: &str,
    values: &str,
) -> anyhow::Result<(TestFixture, String)> {
    let fixture = TestFixture::setup().await?;
    fixture
        .default_session()
        .get_settings()
        .set_setting("enable_compact_after_write".into(), "0".into())?;
    fixture.create_default_database().await?;
    let name = format!(
        "{}.{}",
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    fixture
        .execute_command(&format!("CREATE TABLE {name} ({columns})"))
        .await?;
    fixture
        .execute_command(&format!("INSERT INTO {name} VALUES {values}"))
        .await?;
    fixture
        .execute_command(&format!("CREATE ASYNC SPATIAL INDEX geo ON {name}(g)"))
        .await?;
    Ok((fixture, name))
}

async fn refresh_batch_query_count(
    fixture: &TestFixture,
    predicate: &str,
) -> anyhow::Result<String> {
    let query = format!(
        "SELECT count() FROM {}.{} WHERE {predicate}",
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    let blocks = fixture
        .execute_query(&query)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    Ok(blocks[0].get_by_offset(0).index(0).unwrap().to_string())
}

async fn replace_refresh_batch_legacy_spatial_block(
    fixture: &TestFixture,
    block: BlockMeta,
) -> anyhow::Result<()> {
    let ctx = fixture.new_query_ctx().await?;
    let table = fixture.latest_default_table().await?;
    let fuse = FuseTable::try_from_table(table.as_ref())?;
    let operator = fuse.get_operator();
    let snapshot = fuse.read_table_snapshot().await?.unwrap();
    assert_eq!(snapshot.segments.len(), 1);
    let reader = SegmentInfoReader::new(operator.clone(), table.schema());
    let compact = reader
        .read(&snapshot.segments[0], &CachePolicy::BYPASS)
        .await?;
    let mut segment = SegmentInfo::try_from(compact.as_ref())?;
    assert_eq!(segment.blocks.len(), 1);
    segment.summary.index_size = block.bloom_filter_index_size
        + block.inverted_index_size.unwrap_or_default()
        + block.vector_index_size.unwrap_or_default()
        + block.spatial_index_size.unwrap_or_default();
    segment.summary.bloom_index_size = Some(block.bloom_filter_index_size);
    segment.summary.ngram_index_size = block.ngram_filter_index_size;
    segment.summary.inverted_index_size = block.inverted_index_size;
    segment.summary.vector_index_size = block.vector_index_size;
    segment.summary.spatial_index_size = block.spatial_index_size;
    segment.summary.spatial_stats = block.spatial_stats.clone();
    segment.blocks[0] = Arc::new(block);
    let generator = fuse.meta_location_generator();
    let location =
        generator.gen_segment_info_location(TestFixture::default_table_meta_timestamps(), false);
    // Store genuinely missing fields, not a decoded current struct with its
    // defaults already applied. Both summary and blocks use the old shape.
    let mut blocks = serde_json::to_value(&segment.blocks)?;
    let mut summary = serde_json::to_value(&segment.summary)?;
    for value in blocks
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .chain(once(&mut summary))
    {
        for stat in value["spatial_stats"].as_object_mut().unwrap().values_mut() {
            let fields = stat.as_object_mut().unwrap();
            fields.remove("is_valid");
            fields.remove("has_empty_rect");
        }
    }
    let blocks = serde_json::to_vec(&blocks)?;
    let summary = serde_json::to_vec(&summary)?;
    let mut bytes = SegmentInfo::VERSION.to_le_bytes().to_vec();
    bytes.push(MetaEncoding::Json as u8);
    bytes.push(MetaCompression::None as u8);
    bytes.extend_from_slice(&(blocks.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&(summary.len() as u64).to_le_bytes());
    bytes.extend(blocks);
    bytes.extend(summary);
    operator.write(&location, bytes).await?;
    let mut new_snapshot = TableSnapshot::try_from_previous(
        snapshot.clone(),
        fuse.cluster_key_info(),
        Some(table.get_table_info().ident.seq),
        TestFixture::default_table_meta_timestamps(),
    )?;
    new_snapshot.segments = vec![(location, SegmentInfo::VERSION)];
    new_snapshot.summary = segment.summary;
    fuse.commit_to_meta_server(
        ctx.as_ref(),
        table.get_table_info(),
        generator,
        new_snapshot,
        None,
        &None,
        &operator,
    )
    .await?;
    Ok(())
}
