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
use common_meta_types::WarehouseInfo;
use databend_query::users::UserApiProvider;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_user_warehouse() -> Result<()> {
    let conf = crate::tests::ConfigBuilder::create().config();

    let tenant = "test";
    let warehouse_name = "*^)&*^*)%  🐸🐸🐸 ???";
    let size = "Large";
    let if_not_exists = false;
    let user_mgr = UserApiProvider::create_global(conf).await?;

    // create warehouse one.
    {
        let warehouse = WarehouseInfo::new(tenant, warehouse_name, size);
        user_mgr
            .create_warehouse(tenant, warehouse.clone(), if_not_exists)
            .await?;
        assert_eq!(
            user_mgr.get_warehouse(tenant, warehouse_name).await?,
            warehouse
        );
    }

    // update warehouse
    {
        user_mgr
            .update_warehouse_size(tenant, warehouse_name, "XXXLarge")
            .await?;
        assert_eq!(
            user_mgr
                .get_warehouse(tenant, warehouse_name)
                .await?
                .meta
                .size,
            "XXXLarge"
        );
    }

    // drop warehouse
    {
        user_mgr
            .drop_warehouse(tenant, warehouse_name, if_not_exists)
            .await?;
        assert!(user_mgr.get_warehouses(tenant).await?.is_empty());
    }

    Ok(())
}
