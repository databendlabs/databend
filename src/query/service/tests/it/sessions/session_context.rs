//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use databend_common_catalog::session_type::SessionType;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::tenant::Tenant;
use databend_common_settings::Settings;
use databend_query::sessions::SessionContext;

#[tokio::test(flavor = "multi_thread")]
async fn test_session_context() -> anyhow::Result<()> {
    let settings = Settings::create(Tenant::new_literal("default"));
    let session_ctx = SessionContext::try_create(settings, SessionType::MySQL, None)?;

    // Abort status.
    {
        session_ctx.set_abort(true);
        let val = session_ctx.get_abort();
        assert!(val);
    }

    // Current database status.
    {
        session_ctx.set_current_database("bend".to_string());
        let val = session_ctx.get_current_database();
        assert_eq!("bend", val);
    }

    // Client host.
    {
        let demo = "127.0.0.1";
        session_ctx.set_client_host(Some(demo.to_string()));

        let val = session_ctx.get_client_host();
        assert_eq!(Some(demo), val.as_deref());
    }

    // Current user.
    {
        let user_info = UserInfo::new_no_auth("user1", "%");
        session_ctx.set_current_user(user_info);

        let val = session_ctx.get_current_user().unwrap();
        assert_eq!("user1".to_string(), val.name);
    }

    // io shutdown tx.
    {
        session_ctx.set_io_shutdown_tx(|| {});

        let val = session_ctx.take_io_shutdown_tx();
        assert!(val.is_some());

        let val = session_ctx.take_io_shutdown_tx();
        assert!(val.is_none());
    }

    // Warehouse.
    {
        session_ctx.set_current_warehouse(Some("wt".to_string()));

        let val = session_ctx.get_current_warehouse().unwrap();
        assert_eq!("wt".to_string(), val);
    }

    Ok(())
}

#[test]
fn test_update_query_ids_results() -> anyhow::Result<()> {
    let settings = Settings::create(Tenant::new_literal("default"));
    let session_ctx = SessionContext::try_create(settings, SessionType::MySQL, None)?;

    for qid in ["q1", "q2", "q3"] {
        session_ctx.update_query_ids_results(qid.to_string(), None);
    }
    let ids = |ctx: &SessionContext| (0..3).map(|i| ctx.get_last_query_id(i)).collect::<Vec<_>>();
    let expected_ids = vec![
        Some("q1".to_string()),
        Some("q2".to_string()),
        Some("q3".to_string()),
    ];

    // Updating an existing entry changes that entry, wherever it is, and no other.
    session_ctx.update_query_ids_results("q3".to_string(), Some("k3".to_string()));
    session_ctx.update_query_ids_results("q1".to_string(), Some("k1".to_string()));

    assert_eq!(ids(&session_ctx), expected_ids);
    assert_eq!(session_ctx.get_last_query_id(3), None);
    assert_eq!(
        session_ctx.get_query_result_cache_key("q1"),
        Some("k1".to_string())
    );
    assert_eq!(session_ctx.get_query_result_cache_key("q2"), None);
    assert_eq!(
        session_ctx.get_query_result_cache_key("q3"),
        Some("k3".to_string())
    );

    // `None` keeps the value that is already set.
    session_ctx.update_query_ids_results("q3".to_string(), None);
    assert_eq!(ids(&session_ctx), expected_ids);
    assert_eq!(
        session_ctx.get_query_result_cache_key("q3"),
        Some("k3".to_string())
    );

    // An unknown query id is appended.
    session_ctx.update_query_ids_results("q4".to_string(), Some("k4".to_string()));
    assert_eq!(session_ctx.get_last_query_id(-1), Some("q4".to_string()));
    assert_eq!(
        session_ctx.get_query_result_cache_key("q4"),
        Some("k4".to_string())
    );

    Ok(())
}
