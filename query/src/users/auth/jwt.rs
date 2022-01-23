use common_exception::Result;
use jwt_simple::algorithms::RSAPublicKey;

use crate::configs::Config;

enum PubKey {
    RSA(RSAPublicKey),
}

pub struct JwtAuthenticator {
    //Todo(youngsofun): verify settings, like issuer
    key_store: jwk::JwkKeyStore,
}

impl JwtAuthenticator {
    pub async fn try_create(cfg: Config) -> Result<Option<Self>> {
        if cfg.query.jwt_key_file == "" {
            return Ok(None);
        }
        let key_store = jwk::JwkKeyStore::new(cfg.query.jwt_key_file).await?;
        Ok(Some(JwtAuthenticator { key_store }))
    }
}

mod jwk {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use common_exception::ErrorCode;
    use common_exception::Result;
    use common_infallible::RwLock;
    use jwt_simple::algorithms::RSAPublicKey;
    use serde::Deserialize;
    use serde::Serialize;

    use crate::users::auth::jwt::PubKey;

    const JWK_REFRESH_INTERVAL: u64 = 15;

    #[derive(Debug, Serialize, Deserialize)]
    pub struct JwkKey {
        pub kid: String,
        pub kty: String,
        pub alg: Option<String>,
        #[serde(default)]
        pub n: String,
        #[serde(default)]
        pub e: String,
    }

    impl JwkKey {
        fn get_public_key(&self) -> Result<PubKey> {
            match self.kty.as_str() {
                "RSA" => {
                    let k = RSAPublicKey::from_components(self.n.as_bytes(), self.e.as_bytes())?;
                    Ok(PubKey::RSA(k))
                }
                _ => Err(ErrorCode::InvalidConfig(format!(
                    " current not support jwk with kty={}",
                    self.kty
                ))),
            }
        }
    }

    #[derive(Deserialize)]
    pub struct JwkKeys {
        pub keys: Vec<JwkKey>,
    }

    pub struct JwkKeyStore {
        url: String,
        keys: Arc<RwLock<HashMap<String, PubKey>>>,
        refresh_interval: Duration,
    }

    impl JwkKeyStore {
        pub async fn new(url: String) -> Result<Self> {
            let refresh_interval = Duration::from_secs(JWK_REFRESH_INTERVAL * 60);
            let keys = Arc::new(RwLock::new(HashMap::new()));
            let mut s = JwkKeyStore {
                url,
                keys,
                refresh_interval,
            };
            s.load_keys().await?;
            Ok(s)
        }
    }

    impl JwkKeyStore {
        pub async fn load_keys(&mut self) -> Result<()> {
            let response = reqwest::get(&self.url).await.map_err(|e| {
                ErrorCode::NetworkRequestError(format!("Could not download JWKS: {}", e))
            })?;
            let jwk_keys = response
                .json::<JwkKeys>()
                .await
                .map_err(|e| ErrorCode::InvalidConfig(format!("Failed to parse keys: {}", e)))?;
            let mut new_keys: HashMap<String, PubKey> = HashMap::new();
            for k in &jwk_keys.keys {
                new_keys.insert(k.kid.to_string(), k.get_public_key()?);
            }
            let mut keys = self.keys.write();
            *keys = new_keys;
            Ok(())
        }
    }
}
