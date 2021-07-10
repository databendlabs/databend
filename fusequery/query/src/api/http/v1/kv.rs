// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;
use std::sync::Arc;

use warp::Filter;

pub type KvStoreRef = Arc<KvStore>;
pub struct KvStore {}

/// A in memory key/value store.
impl KvStore {
    pub fn create() -> KvStoreRef {
        Arc::new(KvStore {})
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
        .or(kv_del(store))
}

/// GET /v1/kv/list
fn kv_list(
    store: KvStoreRef,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "kv" / "list")
        .and(warp::get())
        .and(with_store(store))
        .and_then(handlers::list)
}

fn kv_get(
    store: KvStoreRef,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "kv" / "get")
        .and(warp::post())
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

fn kv_del(
    store: KvStoreRef,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "kv" / "del")
        .and(warp::post())
        .and(json_body())
        .and(with_store(store))
        .and_then(handlers::del)
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
    use log::info;

    use crate::api::http::v1::kv::KvRequest;
    use crate::api::http::v1::kv::KvStoreRef;

    // Get value by key.
    pub async fn get(_store: KvStoreRef) -> Result<impl warp::Reply, std::convert::Infallible> {
        Ok(warp::http::StatusCode::OK)
    }

    // List all the key/value paris.
    pub async fn list(_store: KvStoreRef) -> Result<impl warp::Reply, std::convert::Infallible> {
        Ok(warp::http::StatusCode::OK)
    }

    // Put a kv.
    pub async fn put(
        req: KvRequest,
        _store: KvStoreRef,
    ) -> Result<impl warp::Reply, warp::Rejection> {
        info!("kv put: {:?}", req);
        Ok(warp::http::StatusCode::OK)
    }

    // Delete by key.
    pub async fn del(
        req: KvRequest,
        _store: KvStoreRef,
    ) -> Result<impl warp::Reply, std::convert::Infallible> {
        info!("kv del: {:?}", req);
        Ok(warp::http::StatusCode::OK)
    }
}
