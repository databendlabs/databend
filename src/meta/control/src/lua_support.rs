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

use std::sync::Arc;
use std::time::Duration;

use databend_common_meta_client::errors::CreationError;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_types::UpsertKV;
use mlua::Lua;
use mlua::LuaSerdeExt;
use mlua::UserData;
use mlua::UserDataMethods;
use mlua::Value;

pub struct LuaGrpcClient {
    client: Arc<ClientHandle>,
}

impl LuaGrpcClient {
    pub fn new(client: Arc<ClientHandle>) -> Self {
        Self { client }
    }
}

impl UserData for LuaGrpcClient {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_async_method("get", |lua, this, key: String| async move {
            match this.client.get_kv(&key).await {
                Ok(result) => match lua.to_value(&result) {
                    Ok(lua_value) => Ok((Some(lua_value), None::<String>)),
                    Err(e) => Ok((None::<Value>, Some(format!("Lua conversion error: {}", e)))),
                },
                Err(e) => Ok((None::<Value>, Some(format!("gRPC error: {}", e)))),
            }
        });

        methods.add_async_method(
            "upsert",
            |lua, this, (key, value): (String, String)| async move {
                let upsert = UpsertKV::update(key, value.as_bytes());
                match this.client.request(upsert).await {
                    Ok(result) => match lua.to_value(&result) {
                        Ok(lua_value) => Ok((Some(lua_value), None::<String>)),
                        Err(e) => Ok((None::<Value>, Some(format!("Lua conversion error: {}", e)))),
                    },
                    Err(e) => Ok((None::<Value>, Some(format!("gRPC error: {}", e)))),
                }
            },
        );
    }
}

pub fn setup_lua_environment(lua: &Lua) -> anyhow::Result<()> {
    // Register new_grpc_client function
    let new_grpc_client = lua
        .create_function(|_lua, address: String| {
            let client = MetaGrpcClient::try_create(
                vec![address],
                "root",
                "xxx",
                Some(Duration::from_secs(2)),
                Some(Duration::from_secs(1)),
                None,
            )
            .map_err(|e| mlua::Error::external(format!("Failed to create gRPC client: {}", e)))?;

            Ok(LuaGrpcClient::new(client))
        })
        .map_err(|e| anyhow::anyhow!("Failed to create new_grpc_client function: {}", e))?;

    lua.globals()
        .set("new_grpc_client", new_grpc_client)
        .map_err(|e| anyhow::anyhow!("Failed to register new_grpc_client: {}", e))?;

    // Export NULL constant to Lua environment
    lua.globals()
        .set("NULL", Value::NULL)
        .map_err(|e| anyhow::anyhow!("Failed to register NULL constant: {}", e))?;

    Ok(())
}

pub fn new_grpc_client(addresses: Vec<String>) -> Result<Arc<ClientHandle>, CreationError> {
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
    )
}
