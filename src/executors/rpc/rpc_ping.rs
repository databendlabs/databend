// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use tonic::{Request, Response, Status};

use ping::greeter_server::Greeter;
use ping::{PingRequest, PingResponse};

pub mod ping {
    tonic::include_proto!("ping");
}

#[derive(Default)]
pub struct MyGreeter {}

#[tonic::async_trait]
impl Greeter for MyGreeter {
    async fn say_hello(
        &self,
        request: Request<PingRequest>,
    ) -> Result<Response<PingResponse>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let reply = ping::PingResponse {
            message: format!("Hello {}!", request.into_inner().name),
        };
        Ok(Response::new(reply))
    }
}
