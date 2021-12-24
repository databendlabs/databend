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

use common_base::tokio;
use common_exception::Result;
use common_meta_types::UserDefinedFunction;
use databend_query::configs::Config;
use databend_query::users::UserApiProvider;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_user_udf() -> Result<()> {
    let mut config = Config::default();
    config.query.tenant_id = "tenant1".to_string();

    let description = "this is a description";
    let isnotnull = "isnotnull";
    let isnotempty = "isnotempty";
    let user_mgr = UserApiProvider::create_global(config).await?;

    // add isnotnull.
    {
        let udf = UserDefinedFunction::new(isnotnull, "not(isnull(@1))", description);
        user_mgr.add_udf(udf).await?;
    }

    // add isnotempty.
    {
        let udf = UserDefinedFunction::new(isnotempty, "not(isempty(@1))", description);
        user_mgr.add_udf(udf).await?;
    }

    // get all.
    {
        let udfs = user_mgr.get_udfs().await?;
        assert_eq!(2, udfs.len());
        assert_eq!(isnotempty, udfs[0].name);
        assert_eq!(isnotnull, udfs[1].name);
    }

    // get.
    {
        let udf = user_mgr.get_udf(isnotnull).await?;
        assert_eq!(isnotnull, udf.name);
    }

    // drop.
    {
        user_mgr.drop_udf(isnotempty, false).await?;
        let udfs = user_mgr.get_udfs().await?;
        assert_eq!(1, udfs.len());
        assert_eq!(isnotnull, udfs[0].name);
    }

    // repeat drop same one not with if exist.
    {
        let res = user_mgr.drop_udf(isnotempty, false).await;
        assert!(res.is_err());
    }

    // repeat drop same one with if exist.
    {
        let res = user_mgr.drop_udf(isnotempty, true).await;
        assert!(res.is_ok());
    }

    Ok(())
}
