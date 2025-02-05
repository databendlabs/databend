use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_users::JwkKeyStore;
use databend_common_users::PubKey;
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
async fn test_jwk_store_with_random_keys() -> Result<()> {
    let mock_jwks_loader = Arc::new(MockJwksLoader::new());
    mock_jwks_loader.reset_keys(&["key1", "key2"]);

    let jwk_store = JwkKeyStore::new("jwks_key".to_string())
        .with_load_keys_func(Arc::new(move || mock_jwks_loader.load_keys()));
    let key = jwk_store.get_key(Some("key1".to_string())).await?;
    Ok(())
}
