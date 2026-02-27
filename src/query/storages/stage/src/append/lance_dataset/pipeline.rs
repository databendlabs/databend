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
use std::sync::atomic::AtomicUsize;

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
use super::writer_processor::LanceDatasetWriter;
use super::writer_processor::SharedFragmentState;

pub(crate) fn append_data_to_lance_dataset(
    pipeline: &mut Pipeline,
    info: CopyIntoLocationInfo,
    schema: TableSchemaRef,
    op: Operator,
    query_id: String,
    _group_id: &AtomicUsize,
    max_threads: usize,
) -> Result<()> {
    let target_dataset_path = if info.options.use_raw_path {
        info.path.to_string()
    } else if info.path.ends_with('/') {
        format!("{}{query_id}", info.path)
    } else {
        format!("{}/{query_id}", info.path)
    };
    GlobalIORuntime::instance().block_on(prepare_target_dataset_path(
        &info,
        op.clone(),
        target_dataset_path.as_str(),
    ))?;
    let staging_accessor = Operator::new(Memory::default())?.finish();
    let staging_dataset_path = "tmp".to_string();

    let processor_num = max_threads.max(1);
    let fragment_state = Arc::new(SharedFragmentState::new());
    pipeline.try_resize(processor_num)?;
    pipeline.add_transform(|input, output| {
        LanceDatasetWriter::try_create(
            input,
            output,
            info.clone(),
            schema.clone(),
            op.clone(),
            target_dataset_path.clone(),
            staging_accessor.clone(),
            staging_dataset_path.clone(),
            fragment_state.clone(),
        )
    })?;

    pipeline.try_resize(1)?;
    pipeline.add_transform(|input, output| {
        LanceDatasetCommitter::try_create(
            input,
            output,
            op.clone(),
            target_dataset_path.clone(),
            schema.clone(),
            fragment_state.clone(),
        )
    })?;
    Ok(())
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

    if data_accessor.stat(path).await.is_ok() {
        data_accessor.delete(path).await?;
    }

    let prefix = if path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    };
    if let Ok(mut lister) = data_accessor
        .lister_with(prefix.as_str())
        .recursive(true)
        .await
    {
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
