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

use common_datavalues::chrono::Utc;
use common_exception::ErrorCode;
use common_meta_app::share::AddShareAccountReq;
use common_meta_app::share::CreateShareReq;
use common_meta_app::share::DropShareReq;
use common_meta_app::share::RemoveShareAccountReq;
use common_meta_app::share::ShareAccountNameIdent;
use common_meta_app::share::ShareNameIdent;
use common_tracing::tracing;

use crate::get_share_account_meta_or_err;
use crate::get_share_id_to_name_or_err;
use crate::get_share_meta_by_id_or_err;
use crate::ApiBuilder;
use crate::AsKVApi;
use crate::ShareApi;

/// Test suite of `ShareApi`.
///
/// It is not used by this crate, but is used by other crate that impl `ShareApi`,
/// to ensure an impl works as expected,
/// such as `common/meta/embedded` and `metasrv`.
#[derive(Copy, Clone)]
pub struct ShareApiTestSuite {}

impl ShareApiTestSuite {
    /// Test ShareApi on a single node
    pub async fn test_single_node_share<B, MT>(b: B) -> anyhow::Result<()>
    where
        B: ApiBuilder<MT>,
        MT: ShareApi + AsKVApi,
    {
        let suite = ShareApiTestSuite {};

        suite.share_create_get_drop(&b.build().await).await?;
        suite.share_add_remove_account(&b.build().await).await?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn share_create_get_drop<MT: ShareApi + AsKVApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let share1 = "share1";
        let share_name = ShareNameIdent {
            tenant: tenant.to_string(),
            share_name: share1.to_string(),
        };
        let share_id: u64;

        tracing::info!("--- create share1");
        let create_on = Utc::now();
        {
            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await;
            tracing::info!("create share res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.share_id, "first database id is 1");
            share_id = res.share_id;

            let (share_name_seq, share_name_ret) =
                get_share_id_to_name_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(share_name_seq > 0);
            assert_eq!(share_name, share_name_ret)
        }

        tracing::info!("--- create share1 again with if_not_exists=false");
        {
            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await;
            tracing::info!("create share res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::ShareAlreadyExists("").code(),
                ErrorCode::from(err).code()
            );
        }

        tracing::info!("--- create share1 again with if_not_exists=true");
        {
            let req = CreateShareReq {
                if_not_exists: true,
                share_name: share_name.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await;
            tracing::info!("create share res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.share_id, "first share id is 1");
        }

        tracing::info!("--- drop share1 with if_exists=true");
        {
            let req = DropShareReq {
                if_exists: true,
                share_name: share_name.clone(),
            };

            let res = mt.drop_share(req).await;
            assert!(res.is_ok());

            let ret = get_share_id_to_name_or_err(mt.as_kv_api(), share_id, "").await;
            let err = ret.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownShareId("").code(),
                ErrorCode::from(err).code()
            );
        }

        tracing::info!("--- drop share1 again with if_exists=false");
        {
            let req = DropShareReq {
                if_exists: false,
                share_name: share_name.clone(),
            };

            let res = mt.drop_share(req).await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownShare("").code(),
                ErrorCode::from(err).code()
            );
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn share_add_remove_account<MT: ShareApi + AsKVApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant = "tenant1";
        let share1 = "share1";
        let account = "account1";
        let account2 = "account2";
        let share_name = ShareNameIdent {
            tenant: tenant.to_string(),
            share_name: share1.to_string(),
        };
        let share_id: u64;
        let share_on = Utc::now();
        let create_on = Utc::now();

        tracing::info!("--- prepare share1");
        {
            let req = CreateShareReq {
                if_not_exists: false,
                share_name: share_name.clone(),
                comment: None,
                create_on,
            };

            let res = mt.create_share(req).await;
            tracing::info!("add share account res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.share_id, "first database id is 1");
            share_id = res.share_id;
        }

        tracing::info!("--- add account account1");
        {
            let req = AddShareAccountReq {
                share_name: share_name.clone(),
                share_on,
                account: account.to_string(),
            };

            // get share meta and check account has been added
            let res = mt.add_share_account(req).await;
            tracing::info!("add share account res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(res.share_id, share_id);

            let (_share_meta_seq, share_meta) =
                get_share_meta_by_id_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(share_meta.has_account(&account.to_string()));

            // get and check share account meta
            let share_account_name = ShareAccountNameIdent {
                account: account.to_string(),
                share_id,
            };
            let (_share_account_meta_seq, share_account_meta) =
                get_share_account_meta_or_err(mt.as_kv_api(), &share_account_name, "").await?;
            assert_eq!(share_account_meta.share_id, share_id);
            assert_eq!(share_account_meta.account, account.to_string());
            assert_eq!(share_account_meta.share_on, share_on);
        }

        tracing::info!("--- add account account1 again");
        {
            let req = AddShareAccountReq {
                share_name: share_name.clone(),
                share_on,
                account: account.to_string(),
            };

            let res = mt.add_share_account(req).await;
            tracing::info!("add share account res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::ShareAccountAlreadyExists("").code(),
                ErrorCode::from(err).code()
            );
        }

        tracing::info!("--- add account account2");
        {
            let req = AddShareAccountReq {
                share_name: share_name.clone(),
                share_on,
                account: account2.to_string(),
            };

            let res = mt.add_share_account(req).await;
            tracing::info!("add share account res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(res.share_id, share_id);

            let (_share_meta_seq, share_meta) =
                get_share_meta_by_id_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(share_meta.has_account(&account2.to_string()));
        }

        tracing::info!("--- remove account account2");
        {
            let req = RemoveShareAccountReq {
                share_id,
                account: account2.to_string(),
            };

            let res = mt.remove_share_account(req).await;
            tracing::info!("remove share account res: {:?}", res);
            assert!(res.is_ok());

            // check account2 has been removed from share_meta
            let (_share_meta_seq, share_meta) =
                get_share_meta_by_id_or_err(mt.as_kv_api(), share_id, "").await?;
            assert!(!share_meta.has_account(&account2.to_string()));

            // check share account meta has been removed
            let share_account_name = ShareAccountNameIdent {
                account: account2.to_string(),
                share_id,
            };
            let res = get_share_account_meta_or_err(mt.as_kv_api(), &share_account_name, "").await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownShareAccount("").code(),
                ErrorCode::from(err).code()
            );
        }

        tracing::info!("--- drop share1 with if_exists=true");
        {
            let req = DropShareReq {
                if_exists: true,
                share_name: share_name.clone(),
            };

            let res = mt.drop_share(req).await;
            assert!(res.is_ok());

            // check share account meta has been removed
            let share_account_name = ShareAccountNameIdent {
                account: account.to_string(),
                share_id,
            };
            let res = get_share_account_meta_or_err(mt.as_kv_api(), &share_account_name, "").await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UnknownShareAccount("").code(),
                ErrorCode::from(err).code()
            );
        }

        Ok(())
    }
}
