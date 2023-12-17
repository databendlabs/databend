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

// The servers module used for external communication with user, such as MySQL wired protocol, etc.

mod catalog;
mod query;
mod service;
mod session;
mod sql_info;

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::FlightData;
use catalog::CatalogInfoProvider;
use dashmap::DashMap;
use databend_common_sql::plans::Plan;
use databend_common_sql::PlanExtras;
use futures::Stream;
use parking_lot::Mutex;
use sql_info::SqlInfoProvider;
use tonic::Status;
use uuid::Uuid;

use crate::servers::http::v1::ExpiringMap;
use crate::sessions::Session;

#[macro_export]
macro_rules! status {
    ($desc:expr, $err:expr) => {{
        let msg = format!("{}: {} at {}:{}", $desc, $err, file!(), line!());
        log::error!("{}", &msg);
        Status::internal(msg)
    }};
}
pub(crate) use status;

type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

pub struct FlightSqlServiceImpl {
    pub sessions: Mutex<ExpiringMap<String, Arc<Session>>>,
    statements: Arc<DashMap<Uuid, (Plan, PlanExtras)>>,
}

/// in current official JDBC driver, Statement is based on PreparedStatement too, so we impl it first.
impl FlightSqlServiceImpl {
    pub fn create() -> Self {
        FlightSqlServiceImpl {
            sessions: Mutex::new(Default::default()),
            statements: Arc::new(Default::default()),
        }
    }
}
