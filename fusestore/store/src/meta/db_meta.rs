// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use common_planners::CreateDatabasePlan;
use common_planners::DatabaseEngineType;

// yes, this is weird, should not be the same structure
// let's figure it out later
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct DatabaseMeta {
    pub name: String,
    pub engine: DatabaseEngineType,
    pub options: HashMap<String, String>,
}

impl From<&CreateDatabasePlan> for DatabaseMeta {
    fn from(plan: &CreateDatabasePlan) -> DatabaseMeta {
        DatabaseMeta {
            name: plan.db.clone(),
            engine: plan.engine.clone(),
            options: plan.options.clone(),
        }
    }
}
