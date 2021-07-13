// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use url::Url;

use crate::backends::Backend;
use crate::backends::HttpBackend;
use crate::backends::LocalBackend;
use crate::backends::StoreBackend;

pub struct BackendClient {
    backend: Box<dyn Backend>,
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

        let backend: Box<dyn Backend> = match uri.scheme().to_lowercase().as_str() {
            // Use local sled as backend.
            "local" => Box::new(LocalBackend::create(new_address)),
            // Use http api as backend.
            "http" => Box::new(HttpBackend::create(new_address)),
            // Use store as backend.
            _ => Box::new(StoreBackend::create(new_address)),
        };

        BackendClient { backend }
    }

    pub async fn get<T>(&self, key: String) -> Result<Option<T>>
    where T: serde::de::DeserializeOwned {
        let val = self.backend.get(key).await?;
        Ok(match val {
            None => None,
            Some(v) => Some(serde_json::from_str::<T>(v.as_str())?),
        })
    }

    pub async fn get_from_prefix<T>(&self, prefix: String) -> Result<Vec<(String, T)>>
    where T: serde::de::DeserializeOwned {
        let values = self.backend.get_from_prefix(prefix).await?;
        values
            .into_iter()
            .map(|(k, v)| Ok((k, serde_json::from_str::<T>(v.as_str())?)))
            .collect()
    }

    pub async fn put<T>(&self, key: String, value: T) -> Result<()>
    where T: serde::Serialize {
        let json = serde_json::to_string(&value).unwrap();
        self.backend.put(key, json).await
    }

    pub async fn remove(&self, key: String) -> Result<()> {
        self.backend.remove(key).await
    }
}
