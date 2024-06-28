// Copyright 2023 Datafuse Labs.
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

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use base64::engine::general_purpose;
use base64::prelude::*;
use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_users::JwkKeyStore;
use databend_common_users::JwtAuthenticator;
use databend_common_users::PubKey;
use jwt_simple::prelude::*;
use wiremock::matchers::method;
use wiremock::matchers::path;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;

#[derive(Serialize, Deserialize)]
struct MyAdditionalData {
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
async fn test_parse_non_custom_claim() -> Result<()> {
    let (pair1, pbkey1) = get_jwks_file_rs256("test_kid");
    let template1 = ResponseTemplate::new(200).set_body_raw(pbkey1, "application/json");
    let server = MockServer::start().await;
    let json_path = "/jwks.json";
    Mock::given(method("GET"))
        .and(path(json_path))
        .respond_with(template1)
        .expect(1..)
        // Mounting the mock on the mock server - it's now effective!
        .mount(&server)
        .await;
    let first_url = format!("http://{}{}", server.address(), json_path);
    let auth = JwtAuthenticator::create(first_url, vec![]).unwrap();
    let user_name = "test-user2";
    let my_additional_data = MyAdditionalData {
        user_is_admin: false,
        user_country: "FR".to_string(),
    };
    let claims = Claims::with_custom_claims(my_additional_data, Duration::from_hours(2))
        .with_subject(user_name.to_string());
    let token1 = pair1.sign(claims)?;

    let res = auth.parse_jwt_claims(token1.as_str()).await?;
    assert_eq!(res.custom.role, None);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_jwk_key_store_retry_on_key_not_found() -> Result<()> {
    let func_calls = Arc::new(AtomicUsize::new(0));
    let func_calls_cloned = func_calls.clone();

    let mock_load_keys = Box::new(move || -> HashMap<String, PubKey> {
        let mut keys_map = HashMap::new();
        keys_map.insert(
            "key1".to_string(),
            PubKey::RSA256(RS256KeyPair::generate(2048).unwrap().public_key()),
        );
        func_calls_cloned.fetch_add(1, Ordering::SeqCst);
        keys_map
    });
    let store = JwkKeyStore::new("".to_string()).with_load_keys_func(mock_load_keys);

    let r = store.get_key(Some("key2".to_string())).await;
    assert_eq!(
        r.unwrap_err().message(),
        "key id key2 not found in jwk store"
    );
    assert_eq!(func_calls.load(Ordering::SeqCst), 2);
    Ok(())
}
