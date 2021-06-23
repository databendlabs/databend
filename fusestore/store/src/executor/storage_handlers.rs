// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_flights::ReadPlanAction;
use common_flights::ReadPlanResult;

use crate::executor::action_handler::RequestHandler;
use crate::executor::ActionHandler;

#[async_trait::async_trait]
impl RequestHandler<ReadPlanAction> for ActionHandler {
    async fn handle(&self, act: ReadPlanAction) -> common_exception::Result<ReadPlanResult> {
        let schema = &act.scan_plan.schema_name;
        let splits: Vec<&str> = schema.split('/').collect();
        // TODO error handling
        println!("schema {}, splits {:?}", schema, splits);
        let db_name = splits[0];
        let tbl_name = splits[1];

        let meta = self.meta.lock().unwrap();
        Ok(meta.get_data_parts(db_name, tbl_name))
    }
}
