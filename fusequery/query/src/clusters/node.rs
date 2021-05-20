// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
use common_exception::{Result, ErrorCodes};
use crate::api::FlightClient;
use common_infallible::RwLock;
use std::sync::Arc;
use futures::Future;
use super::address::Address;
use common_flights::ExecutePlanWithShuffleAction;
use common_streams::SendableDataBlockStream;
use common_datavalues::DataSchemaRef;
use serde::{Serializer, Deserializer};
use serde::ser::SerializeStruct;
use serde::de::{Visitor, Unexpected, Error, MapAccess};
use std::fmt::Formatter;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct Node {
    pub name: String,
    // Node priority is in [0,10]
    // larger value means higher priority
    pub priority: u8,
    pub address: Address,
    pub local: bool,
    pub sequence: usize,
}

impl Node {
    pub fn is_local(&self) -> bool {
        self.local
    }

    pub async fn prepare_query_stage(&self, action: ExecutePlanWithShuffleAction, timeout: u64) -> Result<()> {
        Ok(())
    }

    pub async fn fetch_stream(&self, stream_name: String, schema: DataSchemaRef, timeout: u64) -> Result<SendableDataBlockStream> {
        Err(ErrorCodes::UnImplement(""))
    }
}