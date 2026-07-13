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
use databend_meta_client::types::ConditionResult;
use databend_meta_client::types::TxnCondition;
use databend_meta_client::types::TxnOp;
use databend_meta_client::types::TxnRequest;
use databend_meta_client::types::UpsertKV;
use databend_meta_runtime::DatabendRuntime;
use mlua::Error;
use mlua::Lua;
use mlua::LuaSerdeExt;
use mlua::Table;
use mlua::UserData;
use mlua::UserDataMethods;
use mlua::Value;
use serde::Serialize;
use tokio::time;

use crate::admin::MetaAdminClient;

const LUA_UTIL: &str = include_str!("../lua/lua_util.lua");
const ZIPF_LOAD_GENERATOR: &str = include_str!("../lua/zipf_load_generator.lua");

fn invalid_input(path: &str, message: impl std::fmt::Display) -> Error {
    Error::RuntimeError(format!("{path}: {message}"))
}

fn transaction_error(error: Error) -> String {
    match error {
        Error::RuntimeError(message) => format!("Invalid transaction: {message}"),
        error => format!("Invalid transaction: {error}"),
    }
}

fn validate_fields(table: &Table, path: &str, allowed: &[&str]) -> mlua::Result<()> {
    for pair in table.clone().pairs::<Value, Value>() {
        let (key, _) = pair?;
        let Value::String(key) = key else {
            return Err(invalid_input(path, "field names must be strings"));
        };
        let key = key
            .to_str()
            .map_err(|_| invalid_input(path, "field names must be UTF-8"))?;
        if !allowed.contains(&key.as_ref()) {
            return Err(invalid_input(path, format!("unknown field `{key}`")));
        }
    }
    Ok(())
}

fn required_string(table: &Table, path: &str, field: &str) -> mlua::Result<mlua::String> {
    let value = table.raw_get::<Value>(field)?;
    match value {
        Value::String(value) => Ok(value),
        Value::Nil => Err(invalid_input(path, format!("missing field `{field}`"))),
        value => Err(invalid_input(
            path,
            format!(
                "field `{field}` must be a string, got {}",
                value.type_name()
            ),
        )),
    }
}

fn required_utf8(table: &Table, path: &str, field: &str) -> mlua::Result<String> {
    let value = required_string(table, path, field)?;
    value
        .to_str()
        .map(|value| value.to_owned())
        .map_err(|_| invalid_input(path, format!("field `{field}` must be UTF-8")))
}

fn required_u64(table: &Table, path: &str, field: &str) -> mlua::Result<u64> {
    let value = table.raw_get::<Value>(field)?;
    match value {
        Value::Integer(value) if value >= 0 => Ok(value as u64),
        Value::Nil => Err(invalid_input(path, format!("missing field `{field}`"))),
        value => Err(invalid_input(
            path,
            format!(
                "field `{field}` must be a non-negative integer, got {}",
                value.type_name()
            ),
        )),
    }
}

fn duration_from_seconds(seconds: f64, path: &str) -> mlua::Result<Duration> {
    let duration = Duration::try_from_secs_f64(seconds).map_err(|_| {
        invalid_input(
            path,
            "duration must be finite, non-negative, and representable",
        )
    })?;
    if duration.as_millis() > u64::MAX as u128 {
        return Err(invalid_input(
            path,
            "duration exceeds the meta service limit",
        ));
    }
    Ok(duration)
}

fn optional_ttl(table: &Table, path: &str) -> mlua::Result<Option<Duration>> {
    let value = table.raw_get::<Value>("ttl")?;
    let seconds = match value {
        Value::Nil => return Ok(None),
        Value::Integer(value) => value as f64,
        Value::Number(value) => value,
        value => {
            return Err(invalid_input(
                path,
                format!("field `ttl` must be a number, got {}", value.type_name()),
            ));
        }
    };
    duration_from_seconds(seconds, &format!("{path}.ttl")).map(Some)
}

fn dense_table_array(table: Table, path: &str) -> mlua::Result<Vec<Table>> {
    let len = table.raw_len();
    let mut count = 0;
    for pair in table.clone().pairs::<Value, Value>() {
        let (key, _) = pair?;
        count += 1;
        if !matches!(key, Value::Integer(index) if index >= 1 && index as usize <= len) {
            return Err(invalid_input(path, "must be a dense array"));
        }
    }
    if count != len {
        return Err(invalid_input(path, "must be a dense array"));
    }
    (1..=len)
        .map(|index| {
            table
                .raw_get::<Table>(index)
                .map_err(|_| invalid_input(path, format!("item {index} must be a table")))
        })
        .collect()
}

fn optional_array(parent: &Table, field: &str, path: &str) -> mlua::Result<Vec<Table>> {
    match parent.raw_get::<Value>(field)? {
        Value::Nil => Ok(Vec::new()),
        Value::Table(table) => dense_table_array(table, path),
        value => Err(invalid_input(
            path,
            format!("must be an array, got {}", value.type_name()),
        )),
    }
}

fn parse_condition(table: &Table, path: &str) -> mlua::Result<TxnCondition> {
    validate_fields(table, path, &["key", "op", "value"])?;
    let key = required_utf8(table, path, "key")?;
    let op = required_utf8(table, path, "op")?;
    match op.as_str() {
        "eq_seq" => Ok(TxnCondition::eq_seq(
            key,
            required_u64(table, path, "value")?,
        )),
        "ge_seq" => Ok(TxnCondition::match_seq(
            key,
            ConditionResult::Ge,
            required_u64(table, path, "value")?,
        )),
        "eq_value" => Ok(TxnCondition::eq_value(
            key,
            required_string(table, path, "value")?.as_bytes().to_vec(),
        )),
        _ => Err(invalid_input(path, format!("unknown condition `{op}`"))),
    }
}

fn parse_operation(table: &Table, path: &str) -> mlua::Result<TxnOp> {
    let op = required_utf8(table, path, "op")?;
    let key = required_utf8(table, path, "key")?;
    match op.as_str() {
        "put" => {
            validate_fields(table, path, &["op", "key", "value", "ttl"])?;
            let value = required_string(table, path, "value")?.as_bytes().to_vec();
            Ok(TxnOp::put_with_ttl(key, value, optional_ttl(table, path)?))
        }
        "get" => {
            validate_fields(table, path, &["op", "key"])?;
            Ok(TxnOp::get(key))
        }
        "delete" => {
            validate_fields(table, path, &["op", "key"])?;
            Ok(TxnOp::delete(key))
        }
        _ => Err(invalid_input(path, format!("unknown operation `{op}`"))),
    }
}

fn parse_conditions(table: &Table) -> mlua::Result<Vec<TxnCondition>> {
    optional_array(table, "conditions", "transaction.conditions")?
        .iter()
        .enumerate()
        .map(|(index, condition)| {
            parse_condition(condition, &format!("transaction.conditions[{}]", index + 1))
        })
        .collect()
}

fn parse_operations(table: &Table, field: &str) -> mlua::Result<Vec<TxnOp>> {
    let path = format!("transaction[\"{field}\"]");
    optional_array(table, field, &path)?
        .iter()
        .enumerate()
        .map(|(index, operation)| parse_operation(operation, &format!("{path}[{}]", index + 1)))
        .collect()
}

fn parse_transaction(table: Table) -> mlua::Result<TxnRequest> {
    validate_fields(&table, "transaction", &["conditions", "then", "else"])?;
    let conditions = parse_conditions(&table)?;
    let then_ops = parse_operations(&table, "then")?;
    let else_ops = parse_operations(&table, "else")?;
    Ok(TxnRequest::new(conditions, then_ops).with_else(else_ops))
}

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
            |lua, this, (key, value): (String, mlua::String)| async move {
                let upsert = UpsertKV::update(key, value.as_bytes().as_ref());
                lua_call(&lua, "gRPC error", || this.client.upsert_kv(upsert)).await
            },
        );

        methods.add_async_method("transaction", |lua, this, table: Table| async move {
            let txn = match parse_transaction(table) {
                Ok(txn) => txn,
                Err(error) => {
                    return Ok((None::<Value>, Some(transaction_error(error))));
                }
            };
            lua_call(&lua, "gRPC error", || this.client.transaction(txn)).await
        });
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
            let duration = duration_from_seconds(seconds, "sleep")?;
            time::sleep(duration).await;
            Ok(())
        })
        .map_err(|e| anyhow::anyhow!("Failed to create sleep function: {}", e))?;

    metactl_table
        .set("sleep", sleep_fn)
        .map_err(|e| anyhow::anyhow!("Failed to register sleep function: {}", e))?;

    // Register a monotonic millisecond clock for timing and benchmarks. The
    // epoch is this environment's setup time; only differences between calls are
    // meaningful. `Instant` guarantees the value never goes backwards.
    let start = std::time::Instant::now();
    let now_ms_fn = lua
        .create_function(move |_lua, ()| Ok(start.elapsed().as_secs_f64() * 1000.0))
        .map_err(|e| anyhow::anyhow!("Failed to create now_ms function: {}", e))?;

    metactl_table
        .set("now_ms", now_ms_fn)
        .map_err(|e| anyhow::anyhow!("Failed to register now_ms function: {}", e))?;

    // Register the Zipf load generator. The module returns the `ZipfGenerator`
    // table, exposed as `metactl.ZipfGenerator` so any script can call
    // `metactl.ZipfGenerator:new(num_keys, alpha)` directly.
    let zipf_generator = lua
        .load(ZIPF_LOAD_GENERATOR)
        .eval::<Table>()
        .map_err(|e| anyhow::anyhow!("Failed to load zipf_load_generator: {}", e))?;

    metactl_table
        .set("ZipfGenerator", zipf_generator)
        .map_err(|e| anyhow::anyhow!("Failed to register ZipfGenerator: {}", e))?;

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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(lua: &Lua, script: &str) -> TxnRequest {
        let table = lua.load(script).eval::<Table>().unwrap();
        parse_transaction(table).unwrap()
    }

    fn parse_error(lua: &Lua, script: &str) -> String {
        let table = lua.load(script).eval::<Table>().unwrap();
        parse_transaction(table).unwrap_err().to_string()
    }

    #[test]
    fn test_parse_transaction() {
        let lua = Lua::new();
        let actual = parse(
            &lua,
            r#"
            return {
                conditions = {
                    {key = "missing", op = "eq_seq", value = 0},
                    {key = "existing", op = "ge_seq", value = 2},
                    {key = "blob", op = "eq_value", value = "\000\255"},
                },
                ["then"] = {
                    {op = "put", key = "blob", value = "\000\255", ttl = 1.25},
                    {op = "get", key = "blob"},
                    {op = "delete", key = "obsolete"},
                },
                ["else"] = {
                    {op = "put", key = "fallback", value = ""},
                },
            }
            "#,
        );

        let expected = TxnRequest::new(
            vec![
                TxnCondition::eq_seq("missing", 0),
                TxnCondition::match_seq("existing", ConditionResult::Ge, 2),
                TxnCondition::eq_value("blob", vec![0, 255]),
            ],
            vec![
                TxnOp::put_with_ttl("blob", vec![0, 255], Some(Duration::from_millis(1_250))),
                TxnOp::get("blob"),
                TxnOp::delete("obsolete"),
            ],
        )
        .with_else(vec![TxnOp::put("fallback", Vec::new())]);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_transaction_helpers() {
        let lua = Lua::new();
        setup_lua_environment(&lua).unwrap();
        let actual = parse(
            &lua,
            r#"
            return metactl.new_txn(
                {
                    metactl.eq_seq("new", 0),
                    metactl.ge_seq("old", 1),
                    metactl.eq_value("flag", "on"),
                },
                {
                    metactl.put_op("new", "value", 0.5),
                    metactl.get_op("new"),
                },
                {metactl.delete_op("old")}
            )
            "#,
        );

        let expected = TxnRequest::new(
            vec![
                TxnCondition::eq_seq("new", 0),
                TxnCondition::match_seq("old", ConditionResult::Ge, 1),
                TxnCondition::eq_value("flag", b"on".to_vec()),
            ],
            vec![
                TxnOp::put_with_ttl("new", b"value".to_vec(), Some(Duration::from_millis(500))),
                TxnOp::get("new"),
            ],
        )
        .with_else(vec![TxnOp::delete("old")]);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_parse_empty_transaction() {
        let lua = Lua::new();
        assert_eq!(TxnRequest::default(), parse(&lua, "return {}"));
    }

    #[test]
    fn test_reject_invalid_transactions() {
        let lua = Lua::new();
        let cases = [
            (
                "return {unknown = {}}",
                "transaction: unknown field `unknown`",
            ),
            (
                "return {conditions = 1}",
                "transaction.conditions: must be an array, got integer",
            ),
            (
                "return {conditions = {[1] = {}, [3] = {}}}",
                "transaction.conditions: must be a dense array",
            ),
            (
                "return {conditions = {{op = 'eq_seq', value = 0}}}",
                "transaction.conditions[1]: missing field `key`",
            ),
            (
                "return {conditions = {{key = 'k', op = 'eq_seq', value = -1}}}",
                "field `value` must be a non-negative integer",
            ),
            (
                "return {conditions = {{key = 'k', op = 'eq_seq', value = 1.0}}}",
                "field `value` must be a non-negative integer",
            ),
            (
                "return {conditions = {{key = 'k', op = 'bad', value = 0}}}",
                "unknown condition `bad`",
            ),
            (
                "return {[\"then\"] = {'get'}}",
                "transaction[\"then\"]: item 1 must be a table",
            ),
            (
                "return {[\"then\"] = {{op = 'put', key = 'k'}}}",
                "transaction[\"then\"][1]: missing field `value`",
            ),
            (
                "return {[\"then\"] = {{op = 'bad', key = 'k'}}}",
                "unknown operation `bad`",
            ),
            (
                "return {[\"then\"] = {{op = 'get', key = 'k', ttl = 1}}}",
                "unknown field `ttl`",
            ),
            (
                "return {[\"then\"] = {{op = 'put', key = 'k', value = 1}}}",
                "field `value` must be a string, got integer",
            ),
            (
                "return {[\"then\"] = {{op = 'put', key = 'k', value = 'v', ttl = -1}}}",
                "duration must be finite, non-negative, and representable",
            ),
            (
                "return {[\"then\"] = {{op = 'put', key = 'k', value = 'v', ttl = 0/0}}}",
                "duration must be finite, non-negative, and representable",
            ),
            (
                "return {[\"then\"] = {{op = 'put', key = 'k', value = 'v', ttl = 1e17}}}",
                "duration exceeds the meta service limit",
            ),
        ];

        for (script, expected) in cases {
            let error = parse_error(&lua, script);
            assert!(
                error.contains(expected),
                "expected {expected:?} in {error:?} for {script:?}"
            );
        }
    }

    #[test]
    fn test_duration_validation() {
        assert_eq!(
            Duration::from_millis(125),
            duration_from_seconds(0.125, "duration").unwrap()
        );
        for seconds in [-1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(duration_from_seconds(seconds, "duration").is_err());
        }
    }

    #[test]
    fn test_to_string_escapes_strings_and_bytes() {
        let lua = Lua::new();
        setup_lua_environment(&lua).unwrap();
        let string = lua
            .load(r#"return metactl.to_string("a\n\"\\\000\127\128\255")"#)
            .eval::<String>()
            .unwrap();
        let bytes = lua
            .load("return metactl.to_string({97, 10, 34, 92, 0, 127, 128, 255})")
            .eval::<String>()
            .unwrap();
        let key = lua
            .load(r#"return metactl.to_string({["a\n"] = 1})"#)
            .eval::<String>()
            .unwrap();

        assert_eq!(r#""a\n\"\\\x00\x7F\x80\xFF""#, string);
        assert_eq!(r#""a\n\"\\\x00\x7F\x80\xFF""#, bytes);
        assert_eq!(r#"{"a\n"=1}"#, key);
    }

    #[test]
    fn test_zipf_generator_registered() {
        let lua = Lua::new();
        setup_lua_environment(&lua).unwrap();
        let index = lua
            .load(
                r#"
                local zipf = metactl.ZipfGenerator:new(1000000, 1.2)
                return zipf:generate_key_index(0.5)
                "#,
            )
            .eval::<i64>()
            .unwrap();
        assert_eq!(24, index);
    }

    #[test]
    fn test_now_ms_is_monotonic() {
        let lua = Lua::new();
        setup_lua_environment(&lua).unwrap();
        let (a, b) = lua
            .load(
                r#"
                local a = metactl.now_ms()
                local sum = 0
                for i = 1, 1000000 do sum = sum + i end
                local b = metactl.now_ms()
                return a, b
                "#,
            )
            .eval::<(f64, f64)>()
            .unwrap();
        assert!(a >= 0.0, "now_ms must be non-negative, got {a}");
        assert!(b >= a, "now_ms must not go backwards: {a} then {b}");
    }
}
