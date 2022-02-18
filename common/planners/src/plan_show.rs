// Copyright 2022 Datafuse Labs.
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

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;

use crate::ShowDatabasesPlan;
use crate::ShowEnginesPlan;
use crate::ShowFunctionsPlan;
use crate::ShowGrantsPlan;
use crate::ShowMetricsPlan;
use crate::ShowProcessListsPlan;
use crate::ShowSettingsPlan;
use crate::ShowTablesPlan;
use crate::ShowUsersPlan;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum PlanShowKind {
    All,

    // show databases like '%xx%'
    Like(String),

    // show tables where name like '%xx%'
    Where(String),

    // show tables from db1 [or in db1]
    FromOrIn(String),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum ShowPlan {
    ShowDatabases(ShowDatabasesPlan),
    ShowTables(ShowTablesPlan),
    ShowEngines(ShowEnginesPlan),
    ShowFunctions(ShowFunctionsPlan),
    ShowMetrics(ShowMetricsPlan),
    ShowProcessList(ShowProcessListsPlan),
    ShowSettings(ShowSettingsPlan),
    ShowUsers(ShowUsersPlan),
    ShowGrants(ShowGrantsPlan),
}

impl ShowPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}
