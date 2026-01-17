use std::collections::HashMap;
use std::sync::Arc;

use databend_common_users::JwkKeyStore;
use databend_common_users::PubKey;
use databend_common_version::BUILD_INFO;
use jwt_simple::prelude::*;
use parking_lot::Mutex;

struct MockJwksLoader {
    keys: Mutex<HashMap<String, PubKey>>,
}

impl MockJwksLoader {
    fn new() -> Self {
        Self {
            keys: Mutex::new(HashMap::new()),
        }
    }

    fn reset_keys(&self, key_names: &[&'static str]) {
        let mut keys = self.keys.lock();
        keys.clear();
        for key_name in key_names {
            keys.insert(
                key_name.to_string(),
                PubKey::RSA256(Box::new(RS256KeyPair::generate(2048).unwrap().public_key())),
            );
        }
    }

    fn load_keys(&self) -> HashMap<String, PubKey> {
        self.keys.lock().clone()
    }
}

#[tokio::test]
async fn test_jwk_store_with_random_keys() -> anyhow::Result<()> {
    let mock_jwks_loader = Arc::new(MockJwksLoader::new());
    let mock_jwks_loader_cloned = mock_jwks_loader.clone();
    let jwk_store = JwkKeyStore::new("jwks_key".to_string(), &BUILD_INFO)
        .with_load_keys_func(Arc::new(move || mock_jwks_loader_cloned.load_keys()))
        .with_max_recent_cached_maps(2)
        .with_retry_interval(0);

    mock_jwks_loader.reset_keys(&["key1", "key2"]);
    let key = jwk_store.get_key("key1", true).await?;
    assert!(key.is_some());
    let key = jwk_store.get_key("key3", true).await?;
    assert!(key.is_none());

    mock_jwks_loader.reset_keys(&["key3", "key4"]);
    let key = jwk_store.get_key("key3", true).await?;
    assert!(key.is_some());
    let key = jwk_store.get_key("key4", true).await?;
    assert!(key.is_some());
    let key = jwk_store.get_key("key5", true).await?;
    assert!(key.is_none());
    let key = jwk_store.get_key("key1", true).await?;
    assert!(key.is_some());

    mock_jwks_loader.reset_keys(&["key5", "key6"]);
    let key = jwk_store.get_key("key5", true).await?;
    assert!(key.is_some());
    let key = jwk_store.get_key("key6", true).await?;
    assert!(key.is_some());
    let key = jwk_store.get_key("key1", true).await?;
    assert!(key.is_none());

    Ok(())
}

#[tokio::test]
async fn test_jwk_store_with_random_keys_and_long_retry_interval() -> anyhow::Result<()> {
    let mock_jwks_loader = Arc::new(MockJwksLoader::new());
    let mock_jwks_loader_cloned = mock_jwks_loader.clone();
    let jwk_store = JwkKeyStore::new("jwks_key".to_string(), &BUILD_INFO)
        .with_load_keys_func(Arc::new(move || mock_jwks_loader_cloned.load_keys()))
        .with_max_recent_cached_maps(2)
        .with_retry_interval(3600);

    mock_jwks_loader.reset_keys(&["key1", "key2"]);
    let key = jwk_store.get_key("key1", true).await?;
    assert!(key.is_some());
    let key = jwk_store.get_key("key3", true).await?;
    assert!(key.is_none());

    mock_jwks_loader.reset_keys(&["key3", "key4"]);
    let key = jwk_store.get_key("key3", true).await?;
    assert!(key.is_none());
    let key = jwk_store.get_key("key4", true).await?;
    assert!(key.is_none());

    Ok(())
}
