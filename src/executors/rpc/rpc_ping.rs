// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use tonic::{Request, Response, Status};

use crate::proto::{PingRequest, PingResponse};

pub fn ping(request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
    println!("Got a request from {:?}", request.remote_addr());

    let reply = PingResponse {
        message: format!("Hello {}!", request.into_inner().name),
    };
    Ok(Response::new(reply))
}
