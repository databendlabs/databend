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
use std::sync::Arc;

use anyhow::Result;
use databend_common_exception::ErrorCode;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_management::udf::UdfMgr;
use databend_common_management::*;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_types::MatchSeq;
use databend_meta_types::SeqV;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_add_udf() -> anyhow::Result<()> {
    let (kv_api, udf_api) = new_udf_api().await?;

    // lambda udf
    let udf = create_test_lambda_udf();
    udf_api
        .add_udf(udf.clone(), &CreateOption::Create)
        .await??;

    let value = kv_api
        .get_kv(format!("__fd_udfs/admin/{}", udf.name).as_str())
        .await?;

    match value {
        Some(SeqV {
            seq: 1,
            meta: _,
            data: value,
        }) => {
            assert_eq!(
                value,
                serialize_struct(&udf, ErrorCode::IllegalUDFFormat, || "")?
            );
        }
        catch => panic!("GetKVActionReply{:?}", catch),
    }
    // udf server
    let udf = create_test_udf_server();

    udf_api
        .add_udf(udf.clone(), &CreateOption::Create)
        .await??;

    let value = kv_api
        .get_kv(format!("__fd_udfs/admin/{}", udf.name).as_str())
        .await?;

    match value {
        Some(SeqV {
            seq: 2,
            meta: _,
            data: value,
        }) => {
            assert_eq!(
                value,
                serialize_struct(&udf, ErrorCode::IllegalUDFFormat, || "")?
            );
        }
        catch => panic!("GetKVActionReply{:?}", catch),
    }

    // udf script
    let udf = create_test_udf_script();

    udf_api
        .add_udf(udf.clone(), &CreateOption::Create)
        .await??;

    let value = kv_api
        .get_kv(format!("__fd_udfs/admin/{}", udf.name).as_str())
        .await?;

    match value {
        Some(SeqV {
            seq: 3,
            meta: _,
            data: value,
        }) => {
            assert_eq!(
                value,
                serialize_struct(&udf, ErrorCode::IllegalUDFFormat, || "")?
            );
        }
        catch => panic!("GetKVActionReply{:?}", catch),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_already_exists_add_udf() -> anyhow::Result<()> {
    let (_, udf_api) = new_udf_api().await?;

    // lambda udf
    let udf = create_test_lambda_udf();
    udf_api
        .add_udf(udf.clone(), &CreateOption::Create)
        .await??;

    let got = udf_api.add_udf(udf.clone(), &CreateOption::Create).await?;

    let err = got.unwrap_err();

    assert_eq!(err.to_string(), r#"UDF already exists: 'isnotempty'; "#);

    // udf server
    let udf = create_test_udf_server();
    udf_api
        .add_udf(udf.clone(), &CreateOption::Create)
        .await??;

    let got = udf_api.add_udf(udf.clone(), &CreateOption::Create).await?;

    let err = got.unwrap_err();
    assert_eq!(err.to_string(), r#"UDF already exists: 'strlen'; "#);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_get_udfs() -> anyhow::Result<()> {
    let (_, udf_api) = new_udf_api().await?;

    let udfs = udf_api.list_udf().await?;
    assert_eq!(udfs, vec![]);

    let lambda_udf = create_test_lambda_udf();
    let udf_server = create_test_udf_server();

    udf_api
        .add_udf(lambda_udf.clone(), &CreateOption::Create)
        .await??;

    udf_api
        .add_udf(udf_server.clone(), &CreateOption::Create)
        .await??;

    let udfs = udf_api.list_udf().await?;
    assert_eq!(udfs, vec![lambda_udf, udf_server]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_drop_udf() -> anyhow::Result<()> {
    let (_, udf_api) = new_udf_api().await?;

    let lambda_udf = create_test_lambda_udf();
    let udf_server = create_test_udf_server();

    udf_api
        .add_udf(lambda_udf.clone(), &CreateOption::Create)
        .await??;

    udf_api
        .add_udf(udf_server.clone(), &CreateOption::Create)
        .await??;

    let udfs = udf_api.list_udf().await?;
    assert_eq!(udfs, vec![lambda_udf.clone(), udf_server.clone()]);

    udf_api.drop_udf(&lambda_udf.name, MatchSeq::GE(1)).await?;
    udf_api.drop_udf(&udf_server.name, MatchSeq::GE(1)).await?;

    let udfs = udf_api.list_udf().await?;
    assert_eq!(udfs, vec![]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_unknown_udf_drop_udf() -> anyhow::Result<()> {
    let (_, udf_api) = new_udf_api().await?;

    let res = udf_api.drop_udf("UNKNOWN_NAME", MatchSeq::GE(1)).await;
    assert_eq!(Ok(None), res);

    Ok(())
}

fn create_test_lambda_udf() -> UserDefinedFunction {
    UserDefinedFunction::create_lambda_udf(
        "isnotempty",
        vec!["p".to_string()],
        "not(is_null(p))",
        "This is a description",
    )
}

fn create_test_udf_server() -> UserDefinedFunction {
    UserDefinedFunction::create_udf_server(
        "strlen",
        "http://localhost:8888",
        "strlen_py",
        &BTreeMap::default(),
        "python",
        vec![],
        vec![DataType::String],
        DataType::Number(NumberDataType::Int64),
        "This is a description",
        None,
    )
}

fn create_test_udf_script() -> UserDefinedFunction {
    UserDefinedFunction::create_udf_script(
        "strlen2",
        "testcode",
        "strlen_py",
        "javascript",
        vec![DataType::String],
        DataType::Number(NumberDataType::Int64),
        "3.12.0",
        "This is a description",
        None,
    )
}

async fn new_udf_api() -> Result<(Arc<MetaStore>, UdfMgr)> {
    let test_api = MetaStore::new_local_testing::<DatabendRuntime>().await;
    let test_api = Arc::new(test_api);

    let mgr = UdfMgr::create(test_api.clone(), &Tenant::new_literal("admin"));
    Ok((test_api, mgr))
}
