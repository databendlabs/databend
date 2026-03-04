// Copyright 2022 Datafuse Labs.
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

use base64::engine::general_purpose;
use base64::prelude::*;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::schema::CreateOption;
use databend_common_users::CustomClaims;
use databend_common_users::EnsureUser;
use databend_common_users::UserApiProvider;
use databend_query::auth::AuthMgr;
use databend_query::auth::Credential;
use databend_query::test_kits::*;
use jwt_simple::prelude::*;
use p256::EncodedPoint;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

#[derive(Serialize, Deserialize)]
struct NonCustomClaims {
    user_is_admin: bool,
    user_country: String,
}

fn get_jwks_file_rs256(kid: &str) -> (RS256KeyPair, String) {
    let key_pair = RS256KeyPair::generate(2048).unwrap().with_key_id(kid);
    let rsa_components = key_pair.public_key().to_components();

    let e = general_purpose::URL_SAFE_NO_PAD.encode(rsa_components.e);
    let n = general_purpose::URL_SAFE_NO_PAD.encode(rsa_components.n);
    let j =
        serde_json::json!({"keys": [ {"kty": "RSA", "kid": kid, "e": e, "n": n, } ] }).to_string();
    (key_pair, j)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_auth_mgr_with_jwt_multi_sources() -> anyhow::Result<()> {
    let (pair1, pbkey1) = get_jwks_file_rs256("test_kid");
    let (pair2, pbkey2) = get_jwks_file_rs256("second_kid");
    let (pair3, _) = get_jwks_file_rs256("illegal_kid");

    let template1 = ResponseTemplate::new(200).set_body_raw(pbkey1, "application/json");
    let template2 = ResponseTemplate::new(200).set_body_raw(pbkey2, "application/json");
    let json_path = "/jwks.json";
    let second_path = "/plugins/jwks.json";
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(json_path))
        .respond_with(template1)
        .expect(1..)
        // Mounting the mock on the mock server - it's now effective!
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(second_path))
        .respond_with(template2)
        .expect(1..)
        // Mounting the mock on the mock server - it's now effective!
        .mount(&server)
        .await;

    let mut conf = ConfigBuilder::create().config();
    let first_url = format!("http://{}{}", server.address(), json_path);
    let second_url = format!("http://{}{}", server.address(), second_path);
    conf.query.common.jwt_key_file = first_url.clone();
    conf.query.common.jwt_key_files = vec![second_url];
    let _fixture = TestFixture::setup_with_config(&conf).await?;

    let mut session = TestFixture::create_dummy_session().await;

    let auth_mgr = AuthMgr::instance();
    {
        let user_name = "test-user2";
        let role_name = "test-role";
        let custom_claims = CustomClaims::new()
            .with_ensure_user(EnsureUser {
                roles: Some(vec![role_name.to_string()]),
                default_role: None,
            })
            .with_role("test-auth-role");
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token1 = pair1.sign(claims)?;

        let res = auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token: token1,
                    client_ip: None,
                },
                true,
            )
            .await;
        assert!(res.is_ok());

        let roles: Vec<String> = session
            .get_all_available_roles()
            .await?
            .into_iter()
            .map(|r| r.name)
            .collect();
        assert_eq!(roles.len(), 1);
        assert!(!roles.contains(&"test-auth-role".to_string()));
        let claim2 = CustomClaims::new()
            .with_ensure_user(EnsureUser {
                roles: Some(vec![role_name.to_string()]),
                default_role: None,
            })
            .with_role("test-auth-role2");
        let user2 = "candidate_by_keypair2";
        let claims = Claims::with_custom_claims(claim2, Duration::from_hours(2))
            .with_subject(user2.to_string());
        let token2 = pair2.sign(claims)?;
        let res = auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token: token2,
                    client_ip: None,
                },
                true,
            )
            .await;
        assert!(res.is_ok());

        let roles: Vec<String> = session
            .get_all_available_roles()
            .await?
            .into_iter()
            .map(|r| r.name)
            .collect();
        assert_eq!(roles.len(), 1);
        assert!(!roles.contains(&"test-auth-role2".to_string()));

        let non_custom_claim = NonCustomClaims {
            user_is_admin: false,
            user_country: "Springfield".to_string(),
        };
        let user2 = "service_account:mysql@123";
        let claims = Claims::with_custom_claims(non_custom_claim, Duration::from_hours(2))
            .with_subject(user2.to_string());
        let token2 = pair2.sign(claims)?;
        let tenant = session.get_current_tenant();
        let user2_info = UserInfo::new(user2, "%", AuthInfo::JWT);
        UserApiProvider::instance()
            .create_user(
                &tenant,
                user2_info.clone(),
                &CreateOption::CreateIfNotExists,
            )
            .await?;
        let res2 = auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token: token2,
                    client_ip: None,
                },
                true,
            )
            .await;
        assert!(res2.is_ok());
        assert_eq!(session.get_current_user().unwrap(), user2_info);

        // it would not work on claim with unknown jwt keys
        let claim3 = CustomClaims::new()
            .with_ensure_user(EnsureUser {
                roles: Some(vec![role_name.to_string()]),
                default_role: None,
            })
            .with_role("test-auth-role3");
        let user3 = "candidate_by_keypair3";
        let claims = Claims::with_custom_claims(claim3, Duration::from_hours(2))
            .with_subject(user3.to_string());
        let token3 = pair3.sign(claims)?;
        let res3 = auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token: token3,
                    client_ip: None,
                },
                true,
            )
            .await;
        assert!(res3.is_err());
        assert!(
            res3.err()
                .unwrap()
                .to_string()
                .contains("could not decode token from all available jwt key stores")
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_auth_mgr_with_jwt() -> anyhow::Result<()> {
    let kid = "test_kid";
    let key_pair = RS256KeyPair::generate(2048)?.with_key_id(kid);
    let rsa_components = key_pair.public_key().to_components();
    let e = general_purpose::URL_SAFE_NO_PAD.encode(rsa_components.e);
    let n = general_purpose::URL_SAFE_NO_PAD.encode(rsa_components.n);
    let j =
        serde_json::json!({"keys": [ {"kty": "RSA", "kid": kid, "e": e, "n": n, } ] }).to_string();

    let server = MockServer::start().await;
    let json_path = "/jwks.json";
    // Create a mock on the server.
    let template = ResponseTemplate::new(200).set_body_raw(j, "application/json");
    Mock::given(method("GET"))
        .and(path(json_path))
        .respond_with(template)
        .expect(1..)
        // Mounting the mock on the mock server - it's now effective!
        .mount(&server)
        .await;
    let jwks_url = format!("http://{}{}", server.address(), json_path);

    let mut conf = ConfigBuilder::create().config();
    conf.query.common.jwt_key_file = jwks_url.clone();

    let _fixture = TestFixture::setup_with_config(&conf).await?;

    let mut session = TestFixture::create_dummy_session().await;

    let auth_mgr = AuthMgr::instance();
    let user_name = "test";

    // without subject
    {
        let claims = Claims::create(Duration::from_hours(2));
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await;
        assert!(res.is_err());

        assert!(
            res.err()
                .unwrap()
                .to_string()
                .contains("missing field `subject` in jwt")
        );
    }

    // without custom claims
    {
        let claims = Claims::create(Duration::from_hours(2)).with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await;
        assert!(res.is_err());
        assert!(
            res.err()
                .unwrap()
                .message()
                .contains("User 'test'@'%' does not exist")
        );
    }

    // with custom claims
    {
        let custom_claims = CustomClaims::new();
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await;
        assert!(res.is_err());
        assert!(
            res.err()
                .unwrap()
                .message()
                .contains("User 'test'@'%' does not exist")
        );
    }

    // with create user
    {
        let custom_claims = CustomClaims::new().with_ensure_user(EnsureUser::default());
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await?;
        let user_info = session.get_current_user()?;
        assert_eq!(user_info.grants.roles().len(), 0);
    }

    // with create user again and grant roles
    {
        let custom_claims = CustomClaims::new().with_ensure_user(EnsureUser {
            roles: Some(vec!["role1".to_string()]),
            default_role: None,
        });
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await?;
        let user_info = session.get_current_user()?;
        assert!(user_info.grants.roles().contains("role1"));
        assert_eq!(user_info.grants.roles().len(), 1);
    }

    // with create user and grant roles
    {
        let user_name = "test-user2";
        let role_name = "test-role";
        let custom_claims = CustomClaims::new().with_ensure_user(EnsureUser {
            roles: Some(vec![role_name.to_string()]),
            default_role: None,
        });
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await?;

        let user_info = session.get_current_user()?;
        assert_eq!(user_info.name, user_name);
        assert!(user_info.grants.roles().contains("test-role"));
        assert_eq!(user_info.grants.roles().len(), 1);
    }

    // with create user and auth role
    {
        let user_name = "test-user2";
        let role_name = "test-role";
        let custom_claims = CustomClaims::new()
            .with_ensure_user(EnsureUser {
                roles: Some(vec![role_name.to_string()]),
                default_role: None,
            })
            .with_role("test-auth-role");
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await;
        assert!(res.is_ok());

        let roles: Vec<String> = session
            .get_all_available_roles()
            .await?
            .into_iter()
            .map(|r| r.name)
            .collect();
        assert_eq!(roles.len(), 1);
        assert!(!roles.contains(&"test-auth-role".to_string()));
    }

    // root auth not allowed
    {
        let claims = Claims::create(Duration::from_hours(2)).with_subject("root".to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await;
        assert!(res.is_err());
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_auth_mgr_with_jwt_es256() -> anyhow::Result<()> {
    let kid = "test_kid";
    let key_pair = ES256KeyPair::generate().with_key_id(kid);
    let encoded_point =
        EncodedPoint::from_bytes(key_pair.public_key().public_key().to_bytes_uncompressed())
            .expect("must be valid encode point");
    let x = general_purpose::URL_SAFE_NO_PAD.encode(encoded_point.x().unwrap());
    let y = general_purpose::URL_SAFE_NO_PAD.encode(encoded_point.y().unwrap());
    let j =
        serde_json::json!({"keys": [ {"kty": "EC", "kid": kid, "x": x, "y": y, } ] }).to_string();

    let server = MockServer::start().await;
    let json_path = "/jwks.json";
    // Create a mock on the server.
    let template = ResponseTemplate::new(200).set_body_raw(j, "application/json");
    Mock::given(method("GET"))
        .and(path(json_path))
        .respond_with(template)
        .expect(1..)
        // Mounting the mock on the mock server - it's now effective!
        .mount(&server)
        .await;
    let jwks_url = format!("http://{}{}", server.address(), json_path);

    let mut conf = ConfigBuilder::create().config();
    conf.query.common.jwt_key_file = jwks_url.clone();

    let _fixture = TestFixture::setup_with_config(&conf).await?;

    let mut session = TestFixture::create_dummy_session().await;

    let auth_mgr = AuthMgr::instance();
    let user_name = "test";

    // without subject
    {
        let claims = Claims::create(Duration::from_hours(2));
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await;
        assert!(res.is_err());
        assert!(
            res.err()
                .unwrap()
                .to_string()
                .contains("missing field `subject` in jwt")
        );
    }

    // without custom claims
    {
        let claims = Claims::create(Duration::from_hours(2)).with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await;
        assert!(res.is_err());
        assert!(
            res.err()
                .unwrap()
                .message()
                .contains("User 'test'@'%' does not exist")
        );
    }

    // with custom claims
    {
        let custom_claims = CustomClaims::new();
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await;
        assert!(res.is_err());
        assert!(
            res.err()
                .unwrap()
                .message()
                .contains("User 'test'@'%' does not exist")
        );
    }

    // with create user
    {
        let custom_claims = CustomClaims::new().with_ensure_user(EnsureUser::default());
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await?;
        let user_info = session.get_current_user()?;
        assert_eq!(user_info.grants.roles().len(), 0);
    }

    // with create user again and grant roles
    {
        let custom_claims = CustomClaims::new().with_ensure_user(EnsureUser {
            roles: Some(vec!["role1".to_string()]),
            default_role: None,
        });
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await?;
        let user_info = session.get_current_user()?;
        assert!(user_info.grants.roles().contains("role1"));
        assert_eq!(user_info.grants.roles().len(), 1);
    }

    // with create user and grant roles
    {
        let user_name = "test-user2";
        let role_name = "test-role";
        let custom_claims = CustomClaims::new().with_ensure_user(EnsureUser {
            roles: Some(vec![role_name.to_string()]),
            default_role: None,
        });
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await?;
        let user_info = session.get_current_user()?;
        assert_eq!(user_info.name, user_name);
        assert!(user_info.grants.roles().contains("test-role"));
        assert_eq!(user_info.grants.roles().len(), 1);
    }

    // with create user and auth role
    {
        let user_name = "test-user2";
        let role_name = "test-role";
        let custom_claims = CustomClaims::new()
            .with_ensure_user(EnsureUser {
                roles: Some(vec![role_name.to_string()]),
                default_role: None,
            })
            .with_role("test-auth-role");
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await;
        assert!(res.is_ok());

        let roles: Vec<String> = session
            .get_all_available_roles()
            .await?
            .into_iter()
            .map(|r| r.name)
            .collect();
        assert_eq!(roles.len(), 1);
        assert!(!roles.contains(&"test-auth-role".to_string()));
    }

    // root auth not allowed
    {
        let claims = Claims::create(Duration::from_hours(2)).with_subject("root".to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await;
        assert!(res.is_err());
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_jwt_auth_mgr_with_management() -> anyhow::Result<()> {
    let kid = "test_kid";
    let user_name = "test";
    let key_pair = RS256KeyPair::generate(2048)?.with_key_id(kid);
    let rsa_components = key_pair.public_key().to_components();
    let e = general_purpose::URL_SAFE_NO_PAD.encode(rsa_components.e);
    let n = general_purpose::URL_SAFE_NO_PAD.encode(rsa_components.n);
    let j =
        serde_json::json!({"keys": [ {"kty": "RSA", "kid": kid, "e": e, "n": n, } ] }).to_string();

    let server = MockServer::start().await;
    let json_path = "/jwks.json";
    // Create a mock on the server.
    let template = ResponseTemplate::new(200).set_body_raw(j, "application/json");
    Mock::given(method("GET"))
        .and(path(json_path))
        .respond_with(template)
        .expect(1..)
        // Mounting the mock on the mock server - it's now effective!
        .mount(&server)
        .await;

    let mut conf = ConfigBuilder::create().with_management_mode().config();
    conf.query.common.jwt_key_file = format!("http://{}{}", server.address(), json_path);
    let _fixture = TestFixture::setup_with_config(&conf).await?;

    let mut session = TestFixture::create_dummy_session().await;

    let auth_mgr = AuthMgr::instance();

    // with create user in other tenant
    {
        let tenant = "other";
        let custom_claims = CustomClaims::new()
            .with_tenant_id(tenant)
            .with_ensure_user(EnsureUser::default());
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await?;
        let user_info = session.get_current_user()?;
        let current_tenant = session.get_current_tenant();
        assert_eq!(current_tenant.tenant_name().to_string(), tenant.to_string());
        assert_eq!(user_info.grants.roles().len(), 0);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_auth_mgr_ensure_roles() -> anyhow::Result<()> {
    let kid = "test_kid";
    let key_pair = RS256KeyPair::generate(2048)?.with_key_id(kid);
    let rsa_components = key_pair.public_key().to_components();
    let e = general_purpose::URL_SAFE_NO_PAD.encode(rsa_components.e);
    let n = general_purpose::URL_SAFE_NO_PAD.encode(rsa_components.n);
    let j =
        serde_json::json!({"keys": [ {"kty": "RSA", "kid": kid, "e": e, "n": n, } ] }).to_string();

    let server = MockServer::start().await;
    let json_path = "/jwks.json";
    let template = ResponseTemplate::new(200).set_body_raw(j, "application/json");
    Mock::given(method("GET"))
        .and(path(json_path))
        .respond_with(template)
        .expect(1..)
        .mount(&server)
        .await;
    let jwks_url = format!("http://{}{}", server.address(), json_path);

    let mut conf = ConfigBuilder::create().config();
    conf.query.common.jwt_key_file = jwks_url.clone();

    let _fixture = TestFixture::setup_with_config(&conf).await?;

    let mut session = TestFixture::create_dummy_session().await;
    let auth_mgr = AuthMgr::instance();
    let user_name = "test-ensure-roles-user";

    // First, create a user with some existing roles
    {
        let custom_claims = CustomClaims::new().with_ensure_user(EnsureUser {
            roles: Some(vec!["existing-role".to_string()]),
            default_role: None,
        });
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await?;

        let user_info = session.get_current_user()?;
        assert_eq!(user_info.name, user_name);
        assert!(user_info.grants.roles().contains("existing-role"));
        assert_eq!(user_info.grants.roles().len(), 1);
    }

    // Now test ensure roles - add new roles to existing user
    {
        let custom_claims = CustomClaims::new().with_ensure_user(EnsureUser {
            roles: Some(vec![
                "existing-role".to_string(), // This should not be added again
                "new-role-1".to_string(),
                "new-role-2".to_string(),
            ]),
            default_role: None,
        });
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await?;

        let user_info = session.get_current_user()?;
        assert_eq!(user_info.name, user_name);
        let roles: Vec<String> = user_info.grants.roles_vec();
        assert!(roles.contains(&"existing-role".to_string()));
        assert!(roles.contains(&"new-role-1".to_string()));
        assert!(roles.contains(&"new-role-2".to_string()));
        assert_eq!(roles.len(), 3);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_auth_mgr_ensure_default_role() -> anyhow::Result<()> {
    let kid = "test_kid";
    let key_pair = RS256KeyPair::generate(2048)?.with_key_id(kid);
    let rsa_components = key_pair.public_key().to_components();
    let e = general_purpose::URL_SAFE_NO_PAD.encode(rsa_components.e);
    let n = general_purpose::URL_SAFE_NO_PAD.encode(rsa_components.n);
    let j =
        serde_json::json!({"keys": [ {"kty": "RSA", "kid": kid, "e": e, "n": n, } ] }).to_string();

    let server = MockServer::start().await;
    let json_path = "/jwks.json";
    let template = ResponseTemplate::new(200).set_body_raw(j, "application/json");
    Mock::given(method("GET"))
        .and(path(json_path))
        .respond_with(template)
        .expect(1..)
        .mount(&server)
        .await;
    let jwks_url = format!("http://{}{}", server.address(), json_path);

    let mut conf = ConfigBuilder::create().config();
    conf.query.common.jwt_key_file = jwks_url.clone();

    let _fixture = TestFixture::setup_with_config(&conf).await?;

    let mut session = TestFixture::create_dummy_session().await;
    let auth_mgr = AuthMgr::instance();
    let user_name = "test-ensure-default-role-user";

    // First, create a user with some roles but no default role
    {
        let custom_claims = CustomClaims::new().with_ensure_user(EnsureUser {
            roles: Some(vec!["role1".to_string(), "role2".to_string()]),
            default_role: None,
        });
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await?;

        let user_info = session.get_current_user()?;
        assert_eq!(user_info.name, user_name);
        let roles: Vec<String> = user_info.grants.roles_vec();
        assert!(roles.contains(&"role1".to_string()));
        assert!(roles.contains(&"role2".to_string()));
        assert!(user_info.option.default_role().is_none());
    }

    // Now test ensure default role - set JWT role as default
    {
        let custom_claims = CustomClaims::new().with_ensure_user(EnsureUser {
            roles: Some(vec!["role1".to_string(), "role2".to_string()]),
            default_role: Some("role1".to_string()),
        });
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await?;

        let user_info = session.get_current_user()?;
        assert_eq!(user_info.name, user_name);
        assert_eq!(user_info.option.default_role(), Some(&"role1".to_string()));
    }

    // Test changing default role to a different role
    {
        let custom_claims = CustomClaims::new().with_ensure_user(EnsureUser {
            roles: Some(vec!["role1".to_string(), "role2".to_string()]),
            default_role: Some("role2".to_string()),
        });
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await?;

        let user_info = session.get_current_user()?;
        assert_eq!(user_info.name, user_name);
        assert_eq!(user_info.option.default_role(), Some(&"role2".to_string()));
    }

    // Test that default role is not changed if it's the same
    {
        let custom_claims = CustomClaims::new().with_ensure_user(EnsureUser {
            roles: Some(vec!["role1".to_string(), "role2".to_string()]),
            default_role: Some("role2".to_string()),
        });
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await?;

        let user_info = session.get_current_user()?;
        assert_eq!(user_info.name, user_name);
        assert_eq!(user_info.option.default_role(), Some(&"role2".to_string()));
    }

    // Test changing default role to role3
    {
        let custom_claims = CustomClaims::new().with_ensure_user(EnsureUser {
            roles: None,
            default_role: Some("role3".to_string()),
        });
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        auth_mgr
            .auth(
                &mut session,
                &Credential::Jwt {
                    token,
                    client_ip: None,
                },
                true,
            )
            .await?;

        let user_info = session.get_current_user()?;
        assert_eq!(user_info.name, user_name);
        let roles: Vec<String> = user_info.grants.roles_vec();
        assert!(roles.contains(&"role1".to_string()));
        assert!(roles.contains(&"role2".to_string()));
        assert!(roles.contains(&"role3".to_string()));
        assert_eq!(user_info.option.default_role(), Some(&"role3".to_string()));
    }

    Ok(())
}
