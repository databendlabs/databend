// Copyright 2021 Datafuse Labs
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

use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use databend_meta_client::ClientHandle;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MetaGrpcClient;
use databend_meta_client::errors::CreationError;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_types::UpsertKV;
use mlua::Lua;
use mlua::LuaSerdeExt;
use mlua::UserData;
use mlua::UserDataMethods;
use mlua::Value;
use serde::Serialize;
use tokio::time;

use crate::admin::MetaAdminClient;

const LUA_UTIL: &str = include_str!("../lua_util.lua");

/// Call an async API method, convert the result to a Lua value, and return
/// the `(Option<Value>, Option<String>)` tuple expected by Lua methods.
///
/// On API error, returns `(nil, "<api_err>: <error>")`.
/// On Lua serialization error, returns `(nil, "Lua conversion error: <error>")`.
async fn lua_call<T, E, F, Fut>(
    lua: &Lua,
    api_err: &'static str,
    f: F,
) -> mlua::Result<(Option<Value>, Option<String>)>
where
    T: Serialize,
    E: std::fmt::Display,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    match f().await {
        Ok(result) => match lua.to_value(&result) {
            Ok(v) => Ok((Some(v), None)),
            Err(e) => Ok((None, Some(format!("Lua conversion error: {e}")))),
        },
        Err(e) => Ok((None, Some(format!("{api_err}: {e}")))),
    }
}

pub struct LuaGrpcClient {
    client: Arc<ClientHandle<DatabendRuntime>>,
}

impl LuaGrpcClient {
    pub fn new(client: Arc<ClientHandle<DatabendRuntime>>) -> Self {
        Self { client }
    }
}

impl UserData for LuaGrpcClient {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_async_method("get", |lua, this, key: String| async move {
            lua_call(&lua, "gRPC error", || this.client.get_kv(&key)).await
        });

        methods.add_async_method(
            "upsert",
            |lua, this, (key, value): (String, String)| async move {
                let upsert = UpsertKV::update(key, value.as_bytes());
                lua_call(&lua, "gRPC error", || this.client.upsert_kv(upsert)).await
            },
        );
    }
}

pub struct LuaAdminClient {
    client: MetaAdminClient,
}

impl LuaAdminClient {
    pub fn new(client: MetaAdminClient) -> Self {
        Self { client }
    }
}

impl UserData for LuaAdminClient {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_async_method("metrics", |_lua, this, ()| async move {
            match this.client.get_metrics().await {
                Ok(metrics) => Ok((Some(metrics), None::<String>)),
                Err(e) => Ok((None::<String>, Some(format!("Admin API error: {}", e)))),
            }
        });

        methods.add_async_method("status", |lua, this, ()| async move {
            lua_call(&lua, "Admin API error", || this.client.status()).await
        });

        methods.add_async_method("transfer_leader", |lua, this, to: Option<u64>| async move {
            lua_call(&lua, "Admin API error", || this.client.transfer_leader(to)).await
        });

        methods.add_async_method("trigger_snapshot", |_lua, this, ()| async move {
            match this.client.trigger_snapshot().await {
                Ok(_) => Ok((Some(true), None::<String>)),
                Err(e) => Ok((None::<bool>, Some(format!("Admin API error: {}", e)))),
            }
        });

        methods.add_async_method("list_features", |lua, this, ()| async move {
            lua_call(&lua, "Admin API error", || this.client.list_features()).await
        });

        methods.add_async_method(
            "set_feature",
            |lua, this, (feature, enable): (String, bool)| async move {
                lua_call(&lua, "Admin API error", || {
                    this.client.set_feature(&feature, enable)
                })
                .await
            },
        );
    }
}

pub struct LuaTask {
    handle: Rc<RefCell<Option<tokio::task::JoinHandle<mlua::Value>>>>,
}

impl UserData for LuaTask {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_async_method("join", |_lua, this, ()| async move {
            let handle_opt = this.handle.borrow_mut().take();
            match handle_opt {
                Some(handle) => match handle.await {
                    Ok(result) => Ok(result),
                    Err(e) => {
                        eprintln!("Join error: {}", e);
                        Ok(mlua::Value::Nil)
                    }
                },
                None => {
                    eprintln!("Handle already consumed - task was already awaited");
                    Ok(mlua::Value::Nil)
                }
            }
        });
    }
}

pub fn setup_lua_environment(lua: &Lua) -> anyhow::Result<()> {
    // Create metactl table to namespace all functions
    let metactl_table = lua
        .create_table()
        .map_err(|e| anyhow::anyhow!("Failed to create metactl table: {}", e))?;

    // Register new_grpc_client function
    let new_grpc_client = lua
        .create_function(move |_lua, address: String| {
            let client = MetaGrpcClient::try_create(
                vec![address],
                "root",
                "xxx",
                Some(Duration::from_secs(2)),
                Some(Duration::from_secs(1)),
                None,
                DEFAULT_GRPC_MESSAGE_SIZE,
            )
            .map_err(|e| mlua::Error::external(format!("Failed to create gRPC client: {}", e)))?;

            Ok(LuaGrpcClient::new(client))
        })
        .map_err(|e| anyhow::anyhow!("Failed to create new_grpc_client function: {}", e))?;

    metactl_table
        .set("new_grpc_client", new_grpc_client)
        .map_err(|e| anyhow::anyhow!("Failed to register new_grpc_client: {}", e))?;

    // Register new_admin_client function
    let new_admin_client = lua
        .create_function(|_lua, address: String| {
            let client = MetaAdminClient::new(&address);
            Ok(LuaAdminClient::new(client))
        })
        .map_err(|e| anyhow::anyhow!("Failed to create new_admin_client function: {}", e))?;

    metactl_table
        .set("new_admin_client", new_admin_client)
        .map_err(|e| anyhow::anyhow!("Failed to register new_admin_client: {}", e))?;

    // Export NULL constant to metactl namespace
    metactl_table
        .set("NULL", Value::NULL)
        .map_err(|e| anyhow::anyhow!("Failed to register NULL constant: {}", e))?;

    // Register spawn function that delegates to tokio::task::spawn_local
    let spawn_fn = lua
        .create_function(|_lua, func: mlua::Function| {
            #[allow(clippy::disallowed_methods)]
            let handle = tokio::task::spawn_local(async move {
                match func.call_async::<mlua::Value>(()).await {
                    Ok(result) => result,
                    Err(e) => {
                        eprintln!("Spawned task error: {}", e);
                        mlua::Value::Nil
                    }
                }
            });

            Ok(LuaTask {
                handle: Rc::new(RefCell::new(Some(handle))),
            })
        })
        .map_err(|e| anyhow::anyhow!("Failed to create spawn function: {}", e))?;

    metactl_table
        .set("spawn", spawn_fn)
        .map_err(|e| anyhow::anyhow!("Failed to register spawn function: {}", e))?;

    // Register async sleep function
    let sleep_fn = lua
        .create_async_function(|_lua, seconds: f64| async move {
            let duration = Duration::from_secs_f64(seconds);
            time::sleep(duration).await;
            Ok(())
        })
        .map_err(|e| anyhow::anyhow!("Failed to create sleep function: {}", e))?;

    metactl_table
        .set("sleep", sleep_fn)
        .map_err(|e| anyhow::anyhow!("Failed to register sleep function: {}", e))?;

    // Set the metactl table as a global
    lua.globals()
        .set("metactl", metactl_table)
        .map_err(|e| anyhow::anyhow!("Failed to register metactl namespace: {}", e))?;

    // Load lua_util functions (which registers to_string in metactl namespace)
    lua.load(LUA_UTIL)
        .exec()
        .map_err(|e| anyhow::anyhow!("Failed to load lua_util functions: {}", e))?;

    Ok(())
}

pub fn new_grpc_client(
    addresses: Vec<String>,
) -> Result<Arc<ClientHandle<DatabendRuntime>>, CreationError> {
    eprintln!(
        "Using gRPC API address: {}",
        serde_json::to_string(&addresses).unwrap()
    );
    MetaGrpcClient::try_create(
        addresses,
        "root",
        "xxx",
        Some(Duration::from_secs(2)),
        Some(Duration::from_secs(1)),
        None,
        DEFAULT_GRPC_MESSAGE_SIZE,
    )
}

pub fn new_admin_client(addr: &str) -> MetaAdminClient {
    MetaAdminClient::new(addr)
}

pub async fn run_lua_script(script: &str) -> anyhow::Result<()> {
    let lua = Lua::new();

    setup_lua_environment(&lua)?;

    #[allow(clippy::disallowed_types)]
    let local = tokio::task::LocalSet::new();
    let res = local.run_until(lua.load(script).exec_async()).await;

    if let Err(e) = res {
        return Err(anyhow::anyhow!("Lua execution error: {}", e));
    }
    Ok(())
}

pub async fn run_lua_script_with_result(
    script: &str,
) -> anyhow::Result<Result<Option<String>, String>> {
    let lua = Lua::new();

    setup_lua_environment(&lua)?;

    #[allow(clippy::disallowed_types)]
    let local = tokio::task::LocalSet::new();
    let res = local
        .run_until(lua.load(script).eval_async::<mlua::MultiValue>())
        .await;

    match res {
        Ok(values) => {
            let mut iter = values.iter();
            let first = iter.next();
            let second = iter.next();

            match (first, second) {
                // (result, nil) - success with result
                (Some(Value::String(s)), Some(Value::Nil)) => match s.to_str() {
                    Ok(str_val) => Ok(Ok(Some(str_val.to_string()))),
                    Err(_) => Ok(Err("String conversion error".to_string())),
                },
                // (nil, nil) - success with no result
                (Some(Value::Nil), Some(Value::Nil)) => Ok(Ok(None)),
                // (nil, error) - error case
                (Some(Value::Nil), Some(Value::String(err))) => match err.to_str() {
                    Ok(err_str) => Ok(Err(err_str.to_string())),
                    Err(_) => Ok(Err("Error string conversion failed".to_string())),
                },
                // Single return value - treat as result
                (Some(Value::Nil), None) => Ok(Ok(None)),
                (Some(Value::String(s)), None) => match s.to_str() {
                    Ok(str_val) => Ok(Ok(Some(str_val.to_string()))),
                    Err(_) => Ok(Err("String conversion error".to_string())),
                },
                // Other combinations - treat first value as error if second is missing
                (Some(other), None) => Ok(Err(format!("{:?}", other))),
                _ => Ok(Ok(None)),
            }
        }
        Err(e) => Err(anyhow::anyhow!("Lua execution error: {}", e)),
    }
}
