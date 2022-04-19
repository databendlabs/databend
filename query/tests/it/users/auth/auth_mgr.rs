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

use base64::encode_config;
use base64::URL_SAFE_NO_PAD;
use common_base::tokio;
use common_exception::Result;
use common_meta_types::UserIdentity;
use databend_query::users::auth::jwt::CreateUser;
use databend_query::users::auth::jwt::CustomClaims;
use databend_query::users::AuthMgr;
use databend_query::users::Credential;
use databend_query::users::UserApiProvider;
use jwt_simple::prelude::*;
use wiremock::matchers::method;
use wiremock::matchers::path;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_auth_mgr_with_jwt() -> Result<()> {
    let kid = "test_kid";
    let key_pair = RS256KeyPair::generate(2048)?.with_key_id(kid);
    let rsa_components = key_pair.public_key().to_components();
    let e = encode_config(rsa_components.e, URL_SAFE_NO_PAD);
    let n = encode_config(rsa_components.n, URL_SAFE_NO_PAD);
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

    let mut conf = crate::tests::ConfigBuilder::create().config();
    conf.query.jwt_key_file = jwks_url;
    let user_mgr = UserApiProvider::create_global(conf.clone()).await?;
    let auth_mgr = AuthMgr::create(conf, user_mgr.clone()).await?;
    let tenant = "test";
    let user_name = "test";

    // without subject
    {
        let claims = Claims::create(Duration::from_hours(2));
        let token = key_pair.sign(claims)?;

        let res = auth_mgr.auth(&Credential::Jwt { token }).await;
        assert!(res.is_err());
        assert_eq!(
            "Code: 1051, displayText = missing field `subject` in jwt.",
            res.err().unwrap().to_string()
        );
    }

    // without custom claims
    {
        let claims = Claims::create(Duration::from_hours(2)).with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr.auth(&Credential::Jwt { token }).await;
        assert!(res.is_err());
        assert_eq!(
            "Code: 2201, displayText = unknown user 'test'@'%'.",
            res.err().unwrap().to_string()
        );
    }

    // with custom claims
    {
        let custom_claims = CustomClaims::new();
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr.auth(&Credential::Jwt { token }).await;
        assert!(res.is_err());
        assert_eq!(
            "Code: 2201, displayText = unknown user 'test'@'%'.",
            res.err().unwrap().to_string()
        );
    }

    // with create user in other tenant
    {
        let tenant = "other";
        let custom_claims = CustomClaims::new().with_create_user(CreateUser {
            tenant_id: Some(tenant.to_string()),
            ..Default::default()
        });
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr.auth(&Credential::Jwt { token }).await;
        assert!(res.is_err());
        assert_eq!(
            "Code: 2201, displayText = unknown user 'test'@'%'.",
            res.err().unwrap().to_string()
        );

        let user_info = user_mgr
            .get_user(tenant, UserIdentity::new(user_name, "%"))
            .await?;
        assert_eq!(user_info.grants.roles().len(), 0);
    }

    // with create user
    {
        let custom_claims = CustomClaims::new().with_create_user(CreateUser {
            ..Default::default()
        });
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let user_info = auth_mgr.auth(&Credential::Jwt { token }).await?;
        assert_eq!(user_info.grants.roles().len(), 0);
    }

    // with create user again
    {
        let custom_claims = CustomClaims::new().with_create_user(CreateUser {
            roles: vec!["role1".to_string()],
            ..Default::default()
        });
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let user_info = auth_mgr.auth(&Credential::Jwt { token }).await?;
        assert_eq!(user_info.grants.roles().len(), 0);
    }

    // with create user and grant roles
    {
        let user_name = "test-user2";
        let role_name = "test-role";
        let custom_claims = CustomClaims::new().with_create_user(CreateUser {
            roles: vec![role_name.to_string()],
            ..Default::default()
        });
        let claims = Claims::with_custom_claims(custom_claims, Duration::from_hours(2))
            .with_subject(user_name.to_string());
        let token = key_pair.sign(claims)?;

        let res = auth_mgr.auth(&Credential::Jwt { token }).await;
        assert!(res.is_ok());

        let user_info = user_mgr
            .get_user(tenant, UserIdentity::new(user_name, "%"))
            .await?;
        assert_eq!(user_info.grants.roles().len(), 1);
        assert_eq!(user_info.grants.roles()[0], role_name.to_string());
    }

    Ok(())
}
