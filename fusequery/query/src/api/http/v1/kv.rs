// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;
use std::sync::Arc;

use common_kvs::backends::LocalBackend;
use warp::Filter;

pub type KvStoreRef = Arc<KvStore>;
pub struct KvStore {
    db: LocalBackend,
}

/// Http kv store with sled store.
impl KvStore {
    pub fn create() -> KvStoreRef {
        Arc::new(KvStore {
            db: LocalBackend::create("".to_string()),
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct KvRequest {
    pub key: String,
    pub value: String,
}

/// A key/value store handle.
pub fn kv_handler(
    store: KvStoreRef,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    kv_list(store.clone())
        .or(kv_get(store.clone()))
        .or(kv_put(store.clone()))
        .or(kv_remove(store))
}

/// GET /v1/kv/list
fn kv_list(
    store: KvStoreRef,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "kv" / "list" / String)
        .and(warp::get())
        .and(with_store(store))
        .and_then(handlers::list)
}

fn kv_get(
    store: KvStoreRef,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "kv" / "get" / String)
        .and(warp::get())
        .and(with_store(store))
        .and_then(handlers::get)
}

fn kv_put(
    store: KvStoreRef,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "kv" / "put")
        .and(warp::post())
        .and(json_body())
        .and(with_store(store))
        .and_then(handlers::put)
}

fn kv_remove(
    store: KvStoreRef,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "kv" / "remove")
        .and(warp::post())
        .and(json_body())
        .and(with_store(store))
        .and_then(handlers::remove)
}

fn with_store(
    store: KvStoreRef,
) -> impl Filter<Extract = (KvStoreRef,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || store.clone())
}

fn json_body() -> impl Filter<Extract = (KvRequest,), Error = warp::Rejection> + Clone {
    // When accepting a body, we want a JSON body
    // (and to reject huge payloads)...
    warp::body::content_length_limit(1024 * 16).and(warp::body::json())
}

mod handlers {
    use common_kvs::backends::Backend;
    use log::info;

    use crate::api::http::v1::kv::KvRequest;
    use crate::api::http::v1::kv::KvStoreRef;

    // Get value by key.
    pub async fn get(
        key: String,
        store: KvStoreRef,
    ) -> Result<impl warp::Reply, std::convert::Infallible> {
        let v = store.db.get(key).await.unwrap();
        Ok(warp::reply::json(&v))
    }

    // List all the key/value paris.
    pub async fn list(
        prefix: String,
        store: KvStoreRef,
    ) -> Result<impl warp::Reply, std::convert::Infallible> {
        info!("kv list: {:?}", prefix);
        let values = store.db.get_from_prefix(prefix).await.unwrap();
        Ok(warp::reply::json(&values))
    }

    // Put a kv.
    pub async fn put(
        req: KvRequest,
        store: KvStoreRef,
    ) -> Result<impl warp::Reply, warp::Rejection> {
        info!("kv put: {:?}", req);
        store.db.put(req.key, req.value).await.unwrap();
        Ok(warp::http::StatusCode::OK)
    }

    // Delete by key.
    pub async fn remove(
        req: KvRequest,
        store: KvStoreRef,
    ) -> Result<impl warp::Reply, std::convert::Infallible> {
        info!("kv remove: {:?}", req);
        store.db.remove(req.key).await.unwrap();
        Ok(warp::http::StatusCode::OK)
    }
}
