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
use databend_query::users::UserApiProvider;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_user_udf() -> Result<()> {
    let conf = crate::tests::ConfigBuilder::create().config();

    let tenant = "test";
    let description = "this is a description";
    let isempty = "isempty";
    let isnotempty = "isnotempty";
    let if_not_exists = false;
    let user_mgr = UserApiProvider::create_global(conf).await?;

    // add isempty.
    {
        let udf =
            UserDefinedFunction::new(isempty, vec!["p".to_string()], "isnull(p)", description);
        user_mgr.add_udf(tenant, udf, if_not_exists).await?;
    }

    // add isnotempty.
    {
        let udf = UserDefinedFunction::new(
            isnotempty,
            vec!["p".to_string()],
            "not(isempty(p))",
            description,
        );
        user_mgr.add_udf(tenant, udf, if_not_exists).await?;
    }

    // get all.
    {
        let udfs = user_mgr.get_udfs(tenant).await?;
        assert_eq!(2, udfs.len());
        assert_eq!(isempty, udfs[0].name);
        assert_eq!(isnotempty, udfs[1].name);
    }

    // get.
    {
        let udf = user_mgr.get_udf(tenant, isempty).await?;
        assert_eq!(isempty, udf.name);
    }

    // drop.
    {
        user_mgr.drop_udf(tenant, isnotempty, false).await?;
        let udfs = user_mgr.get_udfs(tenant).await?;
        assert_eq!(1, udfs.len());
        assert_eq!(isempty, udfs[0].name);
    }

    // repeat drop same one not with if exist.
    {
        let res = user_mgr.drop_udf(tenant, isnotempty, false).await;
        assert!(res.is_err());
    }

    // repeat drop same one with if exist.
    {
        let res = user_mgr.drop_udf(tenant, isnotempty, true).await;
        assert!(res.is_ok());
    }

    Ok(())
}
