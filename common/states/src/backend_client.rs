// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_trait::async_trait;
use common_exception::Result;
use url::Url;

use crate::backends::LocalBackend;
use crate::backends::MemoryBackend;
use crate::backends::StateBackend;
use crate::backends::StoreBackend;

pub struct BackendClient {
    backend: Box<dyn StateBackend>,
}

impl BackendClient {
    pub fn create(uri: String) -> Self {
        let uri = Url::parse(uri.as_str()).unwrap();

        let mut host = "";
        let mut port = 0u16;
        if uri.host_str().is_some() {
            host = uri.host_str().unwrap();
        }
        if uri.port().is_some() {
            port = uri.port().unwrap();
        }
        let new_address = format!("{}:{}", host, port);

        let backend: Box<dyn StateBackend> = match uri.scheme().to_lowercase().as_str() {
            // For test.
            "local" => Box::new(LocalBackend::create(new_address)),
            // Use api http kv as backend.
            "memory" => Box::new(MemoryBackend::create(new_address)),
            // Use store as backend.
            _ => Box::new(StoreBackend::create(new_address)),
        };

        BackendClient { backend }
    }

    pub async fn put(&self, key: String, value: String) -> Result<()> {
        self.backend.put(key, value)
    }

    pub async fn remove(&self, key: String) -> Result<()> {
        self.backend.remove(key)
    }

    pub async fn get(&self, key: String) -> Result<Option<String>> {
        self.backend.get(key)
    }
}
