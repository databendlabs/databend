use common_auth::TokenFile;
use common_auth::RefreshableToken;
use common_base::base::tokio;
use common_datavalues::chrono::Utc;

#[tokio::test]
async fn direct_token() {
    let token = "test-token".to_string();
    let refreshable_token = RefreshableToken::Direct(token.clone());
    let expected = "Bearer test-token";
    assert_eq!(refreshable_token.to_header().await.unwrap().to_str().unwrap().to_string(), expected);
}

#[tokio::test]
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
