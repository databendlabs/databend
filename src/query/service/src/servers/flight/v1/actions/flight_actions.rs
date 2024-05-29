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

use databend_common_base::runtime::catch_unwind;
use databend_common_base::runtime::CatchUnwindFuture;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use futures_util::future::BoxFuture;
use serde::Deserialize;
use serde::Serialize;

use crate::servers::flight::v1::actions::init_query_env::init_query_env;
use crate::servers::flight::v1::actions::init_query_env::INIT_QUERY_ENV;
use crate::servers::flight::v1::actions::init_query_fragments::init_query_fragments;
use crate::servers::flight::v1::actions::kill_query::kill_query;
use crate::servers::flight::v1::actions::set_priority::set_priority;
use crate::servers::flight::v1::actions::set_priority::SET_PRIORITY;
use crate::servers::flight::v1::actions::start_prepared_query::start_prepared_query;
use crate::servers::flight::v1::actions::truncate_table::truncate_table;
use crate::servers::flight::v1::actions::truncate_table::TRUNCATE_TABLE;
use crate::servers::flight::v1::actions::INIT_QUERY_FRAGMENTS;
use crate::servers::flight::v1::actions::KILL_QUERY;
use crate::servers::flight::v1::actions::START_PREPARED_QUERY;

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
                let mut deserializer = serde_json::Deserializer::from_slice(request);
                deserializer.disable_recursion_limit();
                let deserializer = serde_stacker::Deserializer::new(&mut deserializer);
                let request = Req::deserialize(deserializer).map_err(|cause| {
                    ErrorCode::BadArguments(format!(
                        "Cannot parse request for {}, cause: {:?}",
                        path, cause
                    ))
                });

                let path = path.clone();
                let t = t.clone();
                Box::pin(async move {
                    let request = request?;

                    let future = catch_unwind(move || t(request))?;

                    let future = CatchUnwindFuture::create(future);
                    match future.await.flatten() {
                        Ok(v) => {
                            let mut out = Vec::with_capacity(512);
                            let mut serializer = serde_json::Serializer::new(&mut out);
                            let serializer = serde_stacker::Serializer::new(&mut serializer);
                            v.serialize(serializer).map_err(|cause| {
                                ErrorCode::BadBytes(format!(
                                    "Cannot serialize response for {}, cause: {:?}",
                                    path, cause
                                ))
                            })?;

                            Ok(out)
                        }
                        Err(err) => Err(err),
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
}
