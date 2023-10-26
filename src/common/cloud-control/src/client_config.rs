use std::fmt::Debug;
use std::time::Duration;

use crate::cloud_api::CLOUD_REQUEST_TIMEOUT_SEC;

#[derive(Debug, Clone, PartialEq)]
pub struct ClientConfig {
    metadata: Vec<(String, String)>,
    timeout: Duration,
}

impl ClientConfig {
    pub fn new() -> Self {
        ClientConfig {
            metadata: Vec::new(),
            timeout: Duration::from_secs(CLOUD_REQUEST_TIMEOUT_SEC),
        }
    }

    pub fn add_metadata<K, V>(&mut self, key: K, value: V)
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.metadata.push((key.into(), value.into()));
    }

    pub fn get_metadata(&self) -> &Vec<(String, String)> {
        &self.metadata
    }

    pub fn get_timeout(&self) -> Duration {
        self.timeout
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self::new()
    }
}
