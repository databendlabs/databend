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

use chrono::Utc;
use databend_common_auth::RefreshableToken;
use databend_common_auth::TokenFile;
use databend_common_base::base::tokio;

#[tokio::test(flavor = "multi_thread")]
async fn direct_token() {
    let token = "test-token".to_string();
    let refreshable_token = RefreshableToken::Direct(token.clone());
    let expected = "Bearer dGVzdC10b2tlbg==";
    assert_eq!(
        refreshable_token
            .to_header()
            .await
            .unwrap()
            .to_str()
            .unwrap()
            .to_string(),
        expected
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn token_file() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), "token1").unwrap();
    let mut token_file = TokenFile::new(file.path()).unwrap();
    assert_eq!(token_file.cached_token().unwrap(), "token1");
    assert!(!token_file.is_expiring());
    assert_eq!(token_file.token(), "token1");
    // Doesn't reload unless expiring
    std::fs::write(file.path(), "token2").unwrap();
    assert_eq!(token_file.token(), "token1");

    token_file.expires_at = Utc::now();
    assert!(token_file.is_expiring());
    assert_eq!(token_file.cached_token(), None);

    // Test on token reload and refresh expiration
    assert_eq!(token_file.token(), "token2");
    assert!(!token_file.is_expiring());
    assert_eq!(token_file.cached_token().unwrap(), "token2");
}
