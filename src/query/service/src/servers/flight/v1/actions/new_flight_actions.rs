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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use futures_util::future::BoxFuture;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::servers::flight::v1::actions::create_data_channel::create_data_channel;
use crate::servers::flight::v1::actions::create_query_fragments::create_query_fragments;
use crate::servers::flight::v1::actions::execute_query_fragments::execute_query_fragments;
use crate::servers::flight::v1::actions::kill_query::kill_query;
use crate::servers::flight::v1::actions::truncate_table::truncate_table;

pub struct FlightActions {
    #[allow(clippy::type_complexity)]
    actions: HashMap<
        String,
        Box<dyn Fn(Value) -> BoxFuture<'static, Result<Value>> + Send + Sync + 'static>,
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
        Req: Serialize + for<'de> Deserialize<'de>,
        Res: Serialize + for<'de> Deserialize<'de>,
        Fut: Future<Output = Result<Res>> + Send + 'static,
        F: Fn(Req) -> Fut + Send + Sync + 'static,
    {
        self.actions.insert(
            path.into(),
            Box::new(move |request| {
                let request = serde_json::from_value(request).unwrap();
                let future = t(request);
                Box::pin(async move {
                    match future.await {
                        Ok(v) => Ok(serde_json::to_value(v).expect("")),
                        Err(err) => Err(err),
                    }
                })
            }),
        );

        self
    }

    pub async fn do_action(&self, path: &str, data: &[u8]) -> Result<Value> {
        match self.actions.get(path) {
            None => Err(ErrorCode::Unimplemented(format!(
                "{} action is unimplemented in flight service",
                path
            ))),
            Some(fun) => match serde_json::from_slice(data) {
                Err(_) => Err(ErrorCode::BadArguments("")),
                Ok(value) => fun(value).await,
            },
        }
    }
}

pub fn flight_actions() -> FlightActions {
    FlightActions::create()
        .action("InitQueryFragmentsPlan", create_query_fragments)
        .action("InitNodesChannel", create_data_channel)
        .action("ExecutePartialQuery", execute_query_fragments)
        .action("TruncateTable", truncate_table)
        .action("KillQuery", kill_query)
}
