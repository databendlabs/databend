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
use common_meta_types::SeqV;
use common_meta_types::UserDefinedFunction;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_add_udf() -> Result<()> {
    let (kv_api, udf_api) = new_udf_api().await?;

    let udf = create_test_udf();
    udf_api.add_udf(udf.clone()).await?;
    let value = kv_api.get_kv("__fd_udfs/admin/isnotempty").await?;

    match value {
        Some(SeqV {
            seq: 1,
            meta: _,
            data: value,
        }) => {
            assert_eq!(value, serde_json::to_vec(&udf)?);
        }
        catch => panic!("GetKVActionReply{:?}", catch),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_already_exists_add_udf() -> Result<()> {
    let (_, udf_api) = new_udf_api().await?;

    let udf = create_test_udf();
    udf_api.add_udf(udf.clone()).await?;

    match udf_api.add_udf(udf.clone()).await {
        Ok(_) => panic!("Already exists add udf must be return Err."),
        Err(cause) => assert_eq!(cause.code(), 2603),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_get_udfs() -> Result<()> {
    let (_, udf_api) = new_udf_api().await?;

    let udfs = udf_api.get_udfs().await?;
    assert_eq!(udfs, vec![]);

    let udf = create_test_udf();
    udf_api.add_udf(udf.clone()).await?;

    let udfs = udf_api.get_udfs().await?;
    assert_eq!(udfs[0], udf);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_drop_udf() -> Result<()> {
    let (_, udf_api) = new_udf_api().await?;

    let udf = create_test_udf();
    udf_api.add_udf(udf.clone()).await?;

    let udfs = udf_api.get_udfs().await?;
    assert_eq!(udfs, vec![udf.clone()]);

    udf_api.drop_udf(&udf.name, None).await?;

    let udfs = udf_api.get_udfs().await?;
    assert_eq!(udfs, vec![]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_unknown_udf_drop_udf() -> Result<()> {
    let (_, udf_api) = new_udf_api().await?;

    match udf_api.drop_udf("UNKNOWN_NAME", None).await {
        Ok(_) => panic!("Unknown UDF drop must be return Err."),
        Err(cause) => assert_eq!(cause.code(), 2602),
    }

    Ok(())
}

fn create_test_udf() -> UserDefinedFunction {
    UserDefinedFunction::new(
        "isnotempty",
        vec!["p".to_string()],
        "not(isnull(p))",
        "This is a description",
    )
}

async fn new_udf_api() -> Result<(Arc<MetaEmbedded>, UdfMgr)> {
    let test_api = Arc::new(MetaEmbedded::new_temp().await?);
    let mgr = UdfMgr::create(test_api.clone(), "admin")?;
    Ok((test_api, mgr))
}
