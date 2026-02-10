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

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use databend_common_base::runtime::CatchUnwindFuture;
use databend_common_base::runtime::catch_unwind;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ResultExt;
use futures_util::future::BoxFuture;
use serde::Deserialize;
use serde::Serialize;

use crate::servers::flight::v1::actions::GET_RUNNING_QUERY_DUMP;
use crate::servers::flight::v1::actions::INIT_QUERY_FRAGMENTS;
use crate::servers::flight::v1::actions::KILL_QUERY;
use crate::servers::flight::v1::actions::START_PREPARED_QUERY;
use crate::servers::flight::v1::actions::SYSTEM_ACTION;
use crate::servers::flight::v1::actions::get_running_query_dump::get_running_query_dump;
use crate::servers::flight::v1::actions::init_query_env::INIT_QUERY_ENV;
use crate::servers::flight::v1::actions::init_query_env::init_query_env;
use crate::servers::flight::v1::actions::init_query_fragments::init_query_fragments;
use crate::servers::flight::v1::actions::kill_query::kill_query;
use crate::servers::flight::v1::actions::set_priority::SET_PRIORITY;
use crate::servers::flight::v1::actions::set_priority::set_priority;
use crate::servers::flight::v1::actions::start_prepared_query::start_prepared_query;
use crate::servers::flight::v1::actions::system_action::system_action;
use crate::servers::flight::v1::actions::truncate_table::TRUNCATE_TABLE;
use crate::servers::flight::v1::actions::truncate_table::truncate_table;

pub struct FlightActions {
    #[allow(clippy::type_complexity)]
    actions: HashMap<
        String,
        Box<dyn Fn(&[u8]) -> BoxFuture<'static, Result<Vec<u8>>> + Send + Sync + 'static>,
    >,
}

impl FlightActions {
    pub fn create() -> FlightActions {
        FlightActions {
            actions: HashMap::new(),
        }
    }

    pub fn action<Req, Res, Fut, F>(mut self, path: impl Into<String>, t: F) -> Self
    where
        Req: Serialize + for<'de> Deserialize<'de> + Send + 'static,
        Res: Serialize + for<'de> Deserialize<'de>,
        Fut: Future<Output = Result<Res>> + Send + 'static,
        F: Fn(Req) -> Fut + Send + Sync + 'static,
    {
        let path = path.into();
        let t = Arc::new(t);
        self.actions.insert(
            path.clone(),
            Box::new(move |request| {
                let request =
                    match catch_unwind(|| -> std::result::Result<Req, serde_json::Error> {
                        let mut deserializer = serde_json::Deserializer::from_slice(request);
                        deserializer.disable_recursion_limit();

                        let deserializer = serde_stacker::Deserializer {
                            de: &mut deserializer,
                            red_zone: recursive::get_minimum_stack_size(),
                            stack_size: recursive::get_stack_allocation_size(),
                        };

                        Req::deserialize(deserializer)
                    }) {
                        Ok(Ok(request)) => Ok(request),
                        Ok(Err(cause)) => Err(ErrorCode::BadArguments(format!(
                            "Cannot parse request for {}, len: {}, cause: {:?}",
                            path,
                            request.len(),
                            cause
                        ))),
                        Err(cause) => Err(cause.add_message_back(format!(
                            "(while deserializing flight action request: action={}, len={})",
                            path,
                            request.len()
                        ))),
                    };

                let path = path.clone();
                let t = t.clone();
                Box::pin(async move {
                    let request = request?;

                    let future = catch_unwind(move || t(request)).map_err(|cause| {
                        cause.add_message_back(format!(
                            "(while creating flight action future: action={})",
                            path
                        ))
                    })?;

                    let future = CatchUnwindFuture::create(future);
                    let response = future
                        .await
                        .with_context(|| format!("failed to do flight action, action: {}", path))
                        .flatten()?;

                    match catch_unwind(|| -> std::result::Result<Vec<u8>, serde_json::Error> {
                        let mut out = Vec::with_capacity(512);
                        let mut serializer = serde_json::Serializer::new(&mut out);
                        let serializer = serde_stacker::Serializer {
                            ser: &mut serializer,
                            red_zone: recursive::get_minimum_stack_size(),
                            stack_size: recursive::get_stack_allocation_size(),
                        };

                        response.serialize(serializer)?;
                        Ok(out)
                    }) {
                        Ok(Ok(out)) => Ok(out),
                        Ok(Err(cause)) => Err(ErrorCode::BadBytes(format!(
                            "Cannot serialize response for {}, cause: {:?}",
                            path, cause
                        ))),
                        Err(cause) => Err(cause.add_message_back(format!(
                            "(while serializing flight action response: action={})",
                            path
                        ))),
                    }
                })
            }),
        );

        self
    }

    pub async fn do_action(&self, path: &str, data: &[u8]) -> Result<Vec<u8>> {
        match self.actions.get(path) {
            Some(fun) => fun(data).await,
            None => Err(ErrorCode::Unimplemented(format!(
                "{} action is unimplemented in flight service",
                path
            ))),
        }
    }
}

pub fn flight_actions() -> FlightActions {
    FlightActions::create()
        .action(INIT_QUERY_ENV, init_query_env)
        .action(INIT_QUERY_FRAGMENTS, init_query_fragments)
        .action(START_PREPARED_QUERY, start_prepared_query)
        .action(TRUNCATE_TABLE, truncate_table)
        .action(KILL_QUERY, kill_query)
        .action(SET_PRIORITY, set_priority)
        .action(SYSTEM_ACTION, system_action)
        .action(GET_RUNNING_QUERY_DUMP, get_running_query_dump)
}
