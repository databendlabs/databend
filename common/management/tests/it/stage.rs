// Copyright 2021 Datafuse Labs.
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

use common_base::tokio;
use common_exception::Result;
use common_management::*;
use common_meta_api::KVApi;
use common_meta_embedded::MetaEmbedded;
use common_meta_types::Credentials;
use common_meta_types::FileFormat;
use common_meta_types::SeqV;
use common_meta_types::StageParams;
use common_meta_types::UserStageInfo;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_add_stage() -> Result<()> {
    let (kv_api, stage_api) = new_stage_api().await?;

    let stage_info = create_test_stage_info();
    stage_api.add_stage(stage_info.clone()).await?;
    let value = kv_api.get_kv("__fd_stages/admin/mystage").await?;

    match value {
        Some(SeqV {
            seq: 1,
            meta: _,
            data: value,
        }) => {
            assert_eq!(value, serde_json::to_vec(&stage_info)?);
        }
        catch => panic!("GetKVActionReply{:?}", catch),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_already_exists_add_stage() -> Result<()> {
    let (_, stage_api) = new_stage_api().await?;

    let stage_info = create_test_stage_info();
    stage_api.add_stage(stage_info.clone()).await?;

    match stage_api.add_stage(stage_info.clone()).await {
        Ok(_) => panic!("Already exists add stage must be return Err."),
        Err(cause) => assert_eq!(cause.code(), 2502),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_get_stages() -> Result<()> {
    let (_, stage_api) = new_stage_api().await?;

    let stages = stage_api.get_stages().await?;
    assert_eq!(stages, vec![]);

    let stage_info = create_test_stage_info();
    stage_api.add_stage(stage_info.clone()).await?;

    let stages = stage_api.get_stages().await?;
    assert_eq!(stages[0], stage_info);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_drop_stage() -> Result<()> {
    let (_, stage_api) = new_stage_api().await?;

    let stage_info = create_test_stage_info();
    stage_api.add_stage(stage_info.clone()).await?;

    let stages = stage_api.get_stages().await?;
    assert_eq!(stages, vec![stage_info.clone()]);

    stage_api.drop_stage(&stage_info.stage_name, None).await?;

    let stages = stage_api.get_stages().await?;
    assert_eq!(stages, vec![]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_unknown_stage_drop_stage() -> Result<()> {
    let (_, stage_api) = new_stage_api().await?;

    match stage_api.drop_stage("UNKNOWN_ID", None).await {
        Ok(_) => panic!("Unknown stage drop stage must be return Err."),
        Err(cause) => assert_eq!(cause.code(), 2501),
    }

    Ok(())
}

fn create_test_stage_info() -> UserStageInfo {
    UserStageInfo {
        stage_name: "mystage".to_string(),
        stage_params: StageParams::new("test", Credentials {
            access_key_id: String::from("test"),
            secret_access_key: String::from("test"),
        }),
        file_format: FileFormat::default(),
        comments: "".to_string(),
    }
}

async fn new_stage_api() -> Result<(Arc<MetaEmbedded>, StageMgr)> {
    let test_api = Arc::new(MetaEmbedded::new_temp().await?);
    let mgr = StageMgr::create(test_api.clone(), "admin")?;
    Ok((test_api, mgr))
}
