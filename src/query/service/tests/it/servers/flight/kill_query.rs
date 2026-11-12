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

use databend_common_catalog::session_type::SessionType;
use databend_common_catalog::table_context::TableContextQueryIdentity;
use databend_common_catalog::table_context::TableContextQueryState;
use databend_common_exception::ErrorCode;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::tenant::Tenant;
use databend_common_sql::plans::KillPlan;
use databend_common_version::BUILD_INFO;
use databend_query::interpreters::KillQueryAccess;
use databend_query::servers::flight::v1::actions::FlightActions;
use databend_query::servers::flight::v1::actions::KILL_QUERY;
use databend_query::servers::flight::v1::actions::KILL_QUERY_V2;
use databend_query::servers::flight::v1::actions::KillQueryRequest;
use databend_query::servers::flight::v1::actions::LegacyKillQueryRequest;
use databend_query::servers::flight::v1::actions::flight_actions;
use databend_query::sessions::INTERNAL_SESSION_ID_PREFIX;
use databend_query::sessions::SessionManager;
use databend_query::test_kits::TestFixture;

#[test]
fn test_can_kill() {
    let mut access = KillQueryAccess {
        tenant: "tenant1".to_string(),
        user: UserIdentity::new("u1", "%"),
        is_super: false,
    };
    let other = UserIdentity::new("u2", "%");
    let other_host = UserIdentity::new("u1", "localhost");

    assert!(access.can_kill("tenant1", Some(&access.user)));
    assert!(!access.can_kill("tenant1", Some(&other)));
    assert!(!access.can_kill("tenant1", Some(&other_host)));
    assert!(!access.can_kill("tenant2", Some(&access.user)));
    assert!(!access.can_kill("tenant1", None));

    access.is_super = true;
    assert!(access.can_kill("tenant1", Some(&other)));
    assert!(access.can_kill("tenant1", None));
    assert!(!access.can_kill("tenant2", Some(&access.user)));
    assert!(!access.can_kill("tenant2", None));
    access.tenant.clear();
    assert!(!access.can_kill("tenant1", Some(&access.user)));
    assert!(!access.can_kill("", Some(&access.user)));
}

#[tokio::test]
async fn test_unknown_kill_action_fails_closed() -> anyhow::Result<()> {
    // Model an old receiver: the legacy action exists, but v2 is not registered.
    let actions = FlightActions::create().action(KILL_QUERY, |_: KillPlan| async { Ok(true) });
    let request = KillQueryRequest {
        plan: KillPlan {
            id: "target".to_string(),
            kill_connection: false,
        },
        access: KillQueryAccess {
            tenant: "test".to_string(),
            user: UserIdentity::new("u1", "%"),
            is_super: false,
        },
    };
    let payload = serde_json::to_vec(&request)?;
    let error = actions
        .do_action(KILL_QUERY_V2, &payload)
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::UNIMPLEMENTED);
    // FlightService returns errors as tonic statuses; the client recovers their code.
    let status = tonic::Status::from(error);
    assert_eq!(status.code(), tonic::Code::Unknown);
    assert_eq!(ErrorCode::from(status).code(), ErrorCode::UNIMPLEMENTED);

    // Missing requester information must not deserialize as a privileged request.
    assert!(
        serde_json::from_slice::<KillQueryRequest>(
            b"{\"plan\":{\"id\":\"x\",\"kill_connection\":false}}"
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn test_legacy_kill_wire_compatibility() -> anyhow::Result<()> {
    let plan = KillPlan {
        id: "p-target".to_string(),
        kill_connection: false,
    };
    let old_payload = serde_json::to_vec(&plan)?;
    let received: LegacyKillQueryRequest = serde_json::from_slice(&old_payload)?;
    assert!(received.access.is_none());
    assert_eq!(received.plan, plan);

    let request = LegacyKillQueryRequest {
        plan: plan.clone(),
        access: Some(KillQueryAccess {
            tenant: "tenant1".to_string(),
            user: UserIdentity::new("u1", "%"),
            is_super: true,
        }),
    };
    let payload = serde_json::to_vec(&request)?;
    // An old receiver still accepts the unchanged top-level KillPlan fields.
    assert_eq!(serde_json::from_slice::<KillPlan>(&payload)?, plan);
    let received: LegacyKillQueryRequest = serde_json::from_slice(&payload)?;
    assert_eq!(received.access.unwrap().tenant, "tenant1");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_legacy_kill_respects_tenant() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;
    let manager = SessionManager::instance();
    let actions = flight_actions();
    for (kill_connection, by_query_id, prefixed) in [
        (false, false, false),
        (true, false, false),
        (false, true, false),
        (false, true, true),
    ] {
        for (requester_tenant, is_super, target_tenant, expected) in [
            (Some("test"), true, "test", true),
            (Some("other_tenant"), true, "test", false),
            (Some(""), true, "test", false),
            (Some("test"), false, "test", false),
            (None, true, "test", true),
            (None, true, "other_tenant", false),
        ] {
            let mut target = manager.create_session(SessionType::Local).await?;
            target.set_current_tenant(Tenant::new_literal(target_tenant))?;
            let target = manager.register_session(target)?;
            target
                .set_authed_user(UserInfo::new_no_auth("u1", "%"), None)
                .await?;
            let ctx = target.create_query_context(&BUILD_INFO).await?;
            if prefixed {
                ctx.update_init_query_id(format!("p-{}", uuid::Uuid::new_v4()));
            }
            let request = LegacyKillQueryRequest {
                plan: KillPlan {
                    id: if by_query_id {
                        ctx.get_id()
                    } else {
                        target.get_id()
                    },
                    kill_connection,
                },
                access: requester_tenant.map(|tenant| KillQueryAccess {
                    tenant: tenant.to_string(),
                    user: UserIdentity::new("u2", "%"),
                    is_super,
                }),
            };
            let response = actions
                .do_action(KILL_QUERY, &serde_json::to_vec(&request)?)
                .await;
            if expected {
                assert!(serde_json::from_slice::<bool>(&response?)?);
            } else {
                assert_eq!(response.unwrap_err().code(), ErrorCode::PERMISSION_DENIED);
            }
            assert_eq!(
                ctx.get_abort_checker().try_check_aborting().is_err(),
                expected
            );
            assert_eq!(target.is_aborting(), expected && kill_connection);
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_kill_query_id_ignores_fragment_sessions() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let coordinator = fixture.new_session_with_type(SessionType::MySQL).await?;
    let coordinator_ctx = coordinator.create_query_context(&BUILD_INFO).await?;
    let fragment = fixture
        .new_session_with_type(SessionType::FlightRPC)
        .await?;
    let fragment_ctx = fragment.create_query_context(&BUILD_INFO).await?;
    let id = coordinator_ctx.get_id();
    fragment_ctx.update_init_query_id(id.clone());
    assert!(fragment.get_id().starts_with(INTERNAL_SESSION_ID_PREFIX));
    let manager = SessionManager::instance();
    assert_eq!(
        manager.get_session_by_query_id(&id).unwrap().get_id(),
        coordinator.get_id()
    );
    drop(coordinator_ctx);
    drop(coordinator);
    assert!(manager.get_session_by_query_id(&id).is_none());
    assert!(
        fragment_ctx
            .get_abort_checker()
            .try_check_aborting()
            .is_ok()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_kill_v2_authorizes_target() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let actions = flight_actions();

    for (kill_connection, by_query_id, prefixed) in [
        (false, false, false),
        (true, false, false),
        (false, true, false),
        (false, true, true),
    ] {
        for (user, tenant, is_super, expected) in [
            ("u2", "test", false, false),
            ("u1", "other_tenant", false, false),
            ("u1", "test", false, true),
            ("u2", "test", true, true),
            ("u2", "other_tenant", true, false),
            ("u1", "", true, false),
        ] {
            let target = fixture.new_session_with_type(SessionType::MySQL).await?;
            target
                .set_authed_user(UserInfo::new_no_auth("u1", "%"), None)
                .await?;
            let target_ctx = target.create_query_context(&BUILD_INFO).await?;
            if prefixed {
                target_ctx.update_init_query_id(format!("p-{}", uuid::Uuid::new_v4()));
            }
            let request = KillQueryRequest {
                plan: KillPlan {
                    id: if by_query_id {
                        target_ctx.get_id()
                    } else {
                        target.get_id()
                    },
                    kill_connection,
                },
                access: KillQueryAccess {
                    tenant: tenant.to_string(),
                    user: UserIdentity::new(user, "%"),
                    is_super,
                },
            };
            let response = actions
                .do_action(KILL_QUERY_V2, &serde_json::to_vec(&request)?)
                .await;
            if expected {
                assert!(serde_json::from_slice::<bool>(&response?)?);
            } else {
                assert_eq!(response.unwrap_err().code(), ErrorCode::PERMISSION_DENIED);
            }
            assert_eq!(
                target_ctx.get_abort_checker().try_check_aborting().is_err(),
                expected
            );
            assert_eq!(target.is_aborting(), expected && kill_connection);
        }
    }
    Ok(())
}
