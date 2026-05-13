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

use databend_common_catalog::plan::ParquetReadOptions;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::ParquetFileFormatParams;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::storage::StorageFsConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StageFileStatus;
use databend_common_storage::StageFilesInfo;
use databend_common_storages_parquet::ParquetTable;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use futures::TryStreamExt;
use tempfile::TempDir;
use tokio::fs;

use super::data::Scenario;
use super::data::make_test_file_rg;
use super::utils::create_parquet_test_fixture;

const PARQUET_META_CACHE: &str = "memory_cache_parquet_meta_data";

fn reset_parquet_meta_cache() -> databend_common_exception::Result<()> {
    let cache_manager = CacheManager::instance();
    cache_manager.set_cache_capacity(PARQUET_META_CACHE, 128)?;
    if let Some(cache) = cache_manager.get_parquet_meta_data_cache() {
        cache.clear();
    }
    Ok(())
}

fn parquet_meta_cache_len() -> usize {
    CacheManager::instance()
        .get_parquet_meta_data_cache()
        .map(|cache| cache.len())
        .unwrap_or_default()
}

async fn copy_parquet_to_stage_root(
    file_names: &[&str],
) -> anyhow::Result<(TempDir, Vec<(String, u64)>)> {
    let (file, _) = make_test_file_rg(Scenario::Int32).await;
    let stage_root = tempfile::tempdir()?;
    let mut files = Vec::with_capacity(file_names.len());
    for file_name in file_names {
        let stage_path = stage_root.path().join(file_name);
        fs::copy(file.path(), &stage_path).await?;
        let size = fs::metadata(&stage_path).await?.len();
        files.push((file_name.to_string(), size));
    }
    Ok((stage_root, files))
}

fn fs_stage_info(stage_root: &TempDir) -> StageInfo {
    let mut stage_info = StageInfo::new_external_stage(
        StorageParams::Fs(StorageFsConfig {
            root: stage_root.path().display().to_string(),
        }),
        true,
    );
    stage_info.file_format_params = FileFormatParams::Parquet(ParquetFileFormatParams::default());
    stage_info
}

fn stage_file(path: String, size: u64, md5: Option<String>) -> StageFileInfo {
    StageFileInfo {
        path,
        size,
        md5,
        last_modified: None,
        etag: None,
        status: StageFileStatus::NeedCopy,
        creator: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_parquet_metadata_cache_boundaries() -> anyhow::Result<()> {
    let fixture = create_parquet_test_fixture().await;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_setting(
        "enable_stage_parquet_table_statistics".to_string(),
        "0".to_string(),
    )?;

    reset_parquet_meta_cache()?;
    let (stage_root, files) =
        copy_parquet_to_stage_root(&["first.parquet", "second.parquet"]).await?;
    let (first_file_name, first_file_size) = files[0].clone();
    let (second_file_name, second_file_size) = files[1].clone();
    let stage_info = fs_stage_info(&stage_root);
    let files_info = StageFilesInfo {
        path: first_file_name.clone(),
        files: None,
        pattern: None,
    };

    // ParquetTable schema inference should not populate the metadata cache if
    // the stage file only has the weak path/mtime/size fallback identity.
    let weak_table = ParquetTable::create(
        ctx.as_ref(),
        stage_info.clone(),
        files_info.clone(),
        ParquetReadOptions::default(),
        Some(vec![
            stage_file(first_file_name.clone(), first_file_size, None),
            stage_file(second_file_name.clone(), second_file_size, None),
        ]),
        ctx.get_settings(),
        QueryKind::Query,
        &ParquetFileFormatParams::default(),
    )
    .await?;
    assert_eq!(0, parquet_meta_cache_len());

    // The single-file ParquetTable path can answer from the footer already
    // read during schema inference, without enabling extra planning-time
    // footer scans or populating cache when only weak identity is available.
    let weak_single_table = ParquetTable::create(
        ctx.as_ref(),
        stage_info.clone(),
        files_info.clone(),
        ParquetReadOptions::default(),
        Some(vec![stage_file(
            first_file_name.clone(),
            first_file_size,
            None,
        )]),
        ctx.get_settings(),
        QueryKind::Query,
        &ParquetFileFormatParams::default(),
    )
    .await?;
    reset_parquet_meta_cache()?;
    let weak_single_statistics = weak_single_table
        .table_statistics(ctx.clone(), false, None)
        .await?
        .unwrap();
    assert_eq!(Some(20), weak_single_statistics.num_rows);
    assert_eq!(0, parquet_meta_cache_len());

    // With a strong object identity, ParquetTable can cache the already-read
    // first footer so ParquetSource can reuse it later.
    let strong_file = stage_file(
        first_file_name.clone(),
        first_file_size,
        Some("first-md5".to_string()),
    );
    let _ = ParquetTable::create(
        ctx.as_ref(),
        stage_info.clone(),
        files_info.clone(),
        ParquetReadOptions::default(),
        Some(vec![strong_file]),
        ctx.get_settings(),
        QueryKind::Query,
        &ParquetFileFormatParams::default(),
    )
    .await?;
    assert_eq!(1, parquet_meta_cache_len());

    reset_parquet_meta_cache()?;
    let disabled_statistics = weak_table
        .table_statistics(ctx.clone(), false, None)
        .await?;
    assert!(disabled_statistics.is_none());
    assert_eq!(0, parquet_meta_cache_len());

    ctx.get_settings().set_setting(
        "enable_stage_parquet_table_statistics".to_string(),
        "1".to_string(),
    )?;

    // ParquetTable table_statistics reads full footers when gated, but still
    // avoids cache population if only weak fallback identity is available.
    reset_parquet_meta_cache()?;
    let weak_statistics = weak_table
        .table_statistics(ctx.clone(), false, None)
        .await?
        .unwrap();
    assert_eq!(Some(40), weak_statistics.num_rows);
    assert_eq!(0, parquet_meta_cache_len());

    // With strong object identities, ParquetTable table_statistics populates
    // the metadata cache while collecting exact row counts.
    reset_parquet_meta_cache()?;
    let strong_table = ParquetTable::create(
        ctx.as_ref(),
        stage_info,
        files_info,
        ParquetReadOptions::default(),
        Some(vec![
            stage_file(
                first_file_name.clone(),
                first_file_size,
                Some("first-md5".to_string()),
            ),
            stage_file(
                second_file_name,
                second_file_size,
                Some("second-md5".to_string()),
            ),
        ]),
        ctx.get_settings(),
        QueryKind::Query,
        &ParquetFileFormatParams::default(),
    )
    .await?;
    reset_parquet_meta_cache()?;
    let strong_statistics = strong_table
        .table_statistics(ctx.clone(), false, None)
        .await?
        .unwrap();
    assert_eq!(Some(40), strong_statistics.num_rows);
    assert_eq!(2, parquet_meta_cache_len());

    // ParquetSource small-file path reads the whole file and should not touch
    // the parquet metadata cache.
    reset_parquet_meta_cache()?;
    let (file, _) = make_test_file_rg(Scenario::Int32).await;
    let file_path = file.path().to_string_lossy();
    let stream = fixture
        .execute_query(&format!(
            "select /*+ set_var(parquet_fast_read_bytes=104857600) */ sum(i) from 'fs://{file_path}'"
        ))
        .await?;
    stream.try_collect::<Vec<_>>().await?;
    assert_eq!(0, parquet_meta_cache_len());

    // For a normal ParquetSource File part, source execution reads the footer
    // through read_metadata_async_cached and should populate the cache.
    reset_parquet_meta_cache()?;
    let stream = fixture
        .execute_query(&format!(
            "select /*+ set_var(parquet_fast_read_bytes=0) */ sum(i) from 'fs://{file_path}'"
        ))
        .await?;
    stream.try_collect::<Vec<_>>().await?;
    assert_eq!(1, parquet_meta_cache_len());

    Ok(())
}
