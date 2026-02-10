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

use anyhow::Result;
use databend_common_exception::ErrorCode;
use databend_common_management::*;
use databend_common_meta_app::principal::StageFile;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::StageParams;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::storage::StorageS3Config;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_types::SeqV;
use fastrace::func_name;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_add_stage() -> anyhow::Result<()> {
    let (kv_api, stage_api) = new_stage_api().await?;

    let stage_info = create_test_stage_info();
    stage_api
        .add_stage(stage_info.clone(), &CreateOption::Create)
        .await?;
    let value = kv_api.get_kv("__fd_stages/admin/mystage").await?;

    match value {
        Some(SeqV {
            seq: 1,
            meta: _,
            data: value,
        }) => {
            assert_eq!(
                value,
                serialize_struct(&stage_info, ErrorCode::IllegalUserStageFormat, || "")?
            );
        }
        catch => panic!("GetKVActionReply{:?}", catch),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_already_exists_add_stage() -> anyhow::Result<()> {
    let (_, stage_api) = new_stage_api().await?;

    let stage_info = create_test_stage_info();
    stage_api
        .add_stage(stage_info.clone(), &CreateOption::Create)
        .await?;

    match stage_api
        .add_stage(stage_info.clone(), &CreateOption::Create)
        .await
    {
        Ok(_) => panic!("Already exists add stage must be return Err."),
        Err(cause) => assert_eq!(cause.code(), 2502),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_get_stages() -> anyhow::Result<()> {
    let (_, stage_api) = new_stage_api().await?;

    let stages = stage_api.get_stages().await?;
    assert_eq!(stages, vec![]);

    let stage_info = create_test_stage_info();
    stage_api
        .add_stage(stage_info.clone(), &CreateOption::Create)
        .await?;

    let stages = stage_api.get_stages().await?;
    assert_eq!(stages[0], stage_info);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_drop_stage() -> anyhow::Result<()> {
    let (_, stage_api) = new_stage_api().await?;

    let stage_info = create_test_stage_info();
    stage_api
        .add_stage(stage_info.clone(), &CreateOption::Create)
        .await?;

    let stages = stage_api.get_stages().await?;
    assert_eq!(stages, vec![stage_info.clone()]);

    stage_api.drop_stage(&stage_info.stage_name).await?;

    let stages = stage_api.get_stages().await?;
    assert_eq!(stages, vec![]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_unknown_stage_drop_stage() -> anyhow::Result<()> {
    let (_, stage_api) = new_stage_api().await?;

    match stage_api.drop_stage("UNKNOWN_ID").await {
        Ok(_) => panic!("Unknown stage drop stage must be return Err."),
        Err(cause) => assert_eq!(cause.code(), 2501),
    }

    Ok(())
}

fn create_test_stage_info() -> StageInfo {
    StageInfo {
        stage_name: "mystage".to_string(),
        stage_params: StageParams {
            storage: StorageParams::S3(StorageS3Config {
                bucket: "mystage_bucket".to_string(),
                ..Default::default()
            }),
        },
        ..Default::default()
    }
}

async fn new_stage_api() -> Result<(Arc<MetaStore>, StageMgr)> {
    let test_api = MetaStore::new_local_testing::<DatabendRuntime>().await;
    let test_api = Arc::new(test_api);

    let mgr = StageMgr::create(
        test_api.clone(),
        &Tenant::new_or_err("admin", func_name!()).unwrap(),
    );
    Ok((test_api, mgr))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_add_stage_file() -> anyhow::Result<()> {
    let (kv_api, stage_api) = new_stage_api().await?;

    let stage_info = create_test_stage_info();
    stage_api
        .add_stage(stage_info.clone(), &CreateOption::Create)
        .await?;
    let mystage = stage_api.get_stage("mystage").await?.1;
    assert_eq!(mystage.number_of_files, 0);

    let stage_file = StageFile {
        path: "books.csv".to_string(),
        size: 100,
        ..Default::default()
    };
    stage_api.add_file("mystage", stage_file.clone()).await?;
    let value = kv_api
        .get_kv("__fd_stage_files/admin/mystage/books%2ecsv")
        .await?;

    match value {
        Some(SeqV {
            seq: 2,
            meta: _,
            data: value,
        }) => {
            assert_eq!(
                value,
                serialize_struct(&stage_file, ErrorCode::IllegalStageFileFormat, || "")?
            );
        }
        catch => panic!("GetKVActionReply{:?}", catch),
    }

    let new_mystage = stage_api.get_stage("mystage").await?.1;
    assert_eq!(mystage.number_of_files + 1, new_mystage.number_of_files);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_remove_files() -> anyhow::Result<()> {
    let (_kv_api, stage_api) = new_stage_api().await?;
    let stage_info = create_test_stage_info();
    stage_api
        .add_stage(stage_info.clone(), &CreateOption::Create)
        .await?;
    let mystage = stage_api.get_stage("mystage").await?.1;
    assert_eq!(mystage.number_of_files, 0);

    stage_api
        .add_file("mystage", StageFile {
            path: "books.csv".to_string(),
            size: 100,
            ..Default::default()
        })
        .await?;
    stage_api
        .add_file("mystage", StageFile {
            path: "test/books.csv".to_string(),
            size: 100,
            ..Default::default()
        })
        .await?;

    stage_api
        .remove_files("mystage", vec!["books.csv".to_string()])
        .await?;
    let files = stage_api.list_files("mystage").await?;
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].path, "test/books.csv".to_string());

    let new_mystage = stage_api.get_stage("mystage").await?.1;
    assert_eq!(new_mystage.number_of_files, 1);
    Ok(())
}
