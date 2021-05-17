// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
use common_exception::{Result, ErrorCodes};
use crate::api::FlightClient;
use common_infallible::RwLock;
use std::sync::Arc;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct Node {
    pub name: String,
    // Node priority is in [0,10]
    // larger value means higher priority
    pub priority: u8,
    pub address: String,
    pub local: bool,
    // pub client: RwLock<Option<Arc<FlightClient>>>,
}

impl Node {
    pub fn is_local(&self) -> bool {
        self.local
    }

    pub fn try_get_client(&self) -> Result<FlightClient> {
        // let client = self.client.write();
        // if let None = client {
        //     *client = Some(Arc::new(FlightClient::connect("".to_string())))
        // }
        Err(ErrorCodes::UnImplement(""))
    }
}
