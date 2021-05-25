// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use crate::api::rpc::flight_scatter::FlightScatter;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_scatter_data_block() -> Result<()> {
    FlightScatter::try_create();

    Ok(())
}