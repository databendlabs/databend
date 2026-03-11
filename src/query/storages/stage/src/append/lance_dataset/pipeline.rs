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

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_pipeline::core::Pipeline;
use databend_storages_common_stage::CopyIntoLocationInfo;
use futures::TryStreamExt;
use opendal::Operator;
use opendal::services::Memory;

use super::committer_processor::LanceDatasetCommitter;
use super::limit_file_size_processor::LimitFileSizeProcessor;
use super::writer_processor::FragmentWriterParams;
use super::writer_processor::LanceDatasetWriter;
use super::writer_processor::SharedFragmentState;

pub(crate) fn append_data_to_lance_dataset(
    pipeline: &mut Pipeline,
    info: CopyIntoLocationInfo,
    schema: TableSchemaRef,
    op: Operator,
    query_id: String,
    mem_limit: usize,
    max_threads: usize,
) -> Result<()> {
    let target_dataset_path = build_target_dataset_path(&info, &query_id);
    GlobalIORuntime::instance().block_on(prepare_target_dataset_path(
        &info,
        op.clone(),
        target_dataset_path.as_str(),
    ))?;
    let staging_accessor = Operator::new(Memory::default())?.finish();
    let staging_dataset_path = "tmp".to_string();

    let max_threads = max_threads.max(1);
    let fragment_state = Arc::new(SharedFragmentState::new());
    let params = Arc::new(FragmentWriterParams::try_create(
        schema.clone(),
        op.clone(),
        target_dataset_path.clone(),
        staging_accessor.clone(),
        staging_dataset_path.clone(),
    )?);

    let _ = LimitFileSizeProcessor::build(pipeline, mem_limit, max_threads, &info.options)?;
    pipeline.add_transform(|input, output| {
        LanceDatasetWriter::try_create(input, output, params.clone(), fragment_state.clone())
    })?;

    pipeline.try_resize(1)?;
    pipeline.add_transform(|input, output| {
        LanceDatasetCommitter::try_create(
            input,
            output,
            info.clone(),
            op.clone(),
            target_dataset_path.clone(),
            schema.clone(),
            fragment_state.clone(),
        )
    })?;
    Ok(())
}

fn build_target_dataset_path(info: &CopyIntoLocationInfo, query_id: &str) -> String {
    if info.options.use_raw_path {
        return info.path.to_string();
    }

    let path = info.path.as_str();
    let (path, sep) = if path == "/" {
        ("", "")
    } else if path.ends_with('/') {
        (path, "")
    } else {
        (path, "/")
    };
    format!("{path}{sep}{query_id}")
}

async fn ensure_dataset_absent(data_accessor: &Operator, path: &str) -> Result<()> {
    if data_accessor.stat(path).await.is_ok() {
        return Err(ErrorCode::StageFileAlreadyExists(format!(
            "lance dataset already exists at '{path}'"
        )));
    }

    let prefix = if path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    };
    let mut lister = data_accessor
        .lister_with(prefix.as_str())
        .recursive(true)
        .await?;
    if lister.try_next().await?.is_some() {
        return Err(ErrorCode::StageFileAlreadyExists(format!(
            "lance dataset already exists at '{path}'"
        )));
    }

    Ok(())
}

async fn cleanup_dataset_if_exists(data_accessor: &Operator, path: &str) -> Result<()> {
    if path.trim_matches('/').is_empty() {
        return Err(ErrorCode::BadArguments(
            "LANCE overwrite does not support root dataset path".to_string(),
        ));
    }

    if let Ok(mut lister) = data_accessor.lister_with(path).recursive(true).await {
        while let Some(entry) = lister.try_next().await? {
            if entry.metadata().is_file() {
                data_accessor.delete(entry.path()).await?;
            }
        }
    }

    Ok(())
}

async fn prepare_target_dataset_path(
    info: &CopyIntoLocationInfo,
    data_accessor: Operator,
    dataset_path: &str,
) -> Result<()> {
    if info.options.overwrite {
        cleanup_dataset_if_exists(&data_accessor, dataset_path).await
    } else {
        ensure_dataset_absent(&data_accessor, dataset_path).await
    }
}

#[cfg(test)]
mod tests {
    use databend_common_ast::ast::CopyIntoLocationOptions;
    use databend_common_meta_app::principal::FileFormatParams;
    use databend_common_meta_app::principal::ParquetFileFormatParams;
    use databend_common_meta_app::principal::StageInfo;
    use databend_storages_common_stage::CopyIntoLocationInfo;

    use super::build_target_dataset_path;

    fn make_info(path: &str, use_raw_path: bool) -> CopyIntoLocationInfo {
        let mut stage = StageInfo::new_internal_stage("test");
        stage.file_format_params = FileFormatParams::Parquet(ParquetFileFormatParams::default());

        CopyIntoLocationInfo {
            stage: Box::new(stage),
            path: path.to_string(),
            options: CopyIntoLocationOptions {
                use_raw_path,
                ..Default::default()
            },
            is_ordered: false,
            partition_by: None,
        }
    }

    #[test]
    fn test_build_target_dataset_path_root_stage_path() {
        let info = make_info("/", false);
        let path = build_target_dataset_path(&info, "qid");
        assert_eq!(path, "qid");
    }

    #[test]
    fn test_build_target_dataset_path_non_root() {
        let info = make_info("foo", false);
        let path = build_target_dataset_path(&info, "qid");
        assert_eq!(path, "foo/qid");

        let info = make_info("foo/", false);
        let path = build_target_dataset_path(&info, "qid");
        assert_eq!(path, "foo/qid");
    }

    #[test]
    fn test_build_target_dataset_path_use_raw_path() {
        let info = make_info("/", true);
        let path = build_target_dataset_path(&info, "qid");
        assert_eq!(path, "/");
    }
}
