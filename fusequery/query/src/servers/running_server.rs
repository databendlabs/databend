// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use tokio::sync::mpsc::Sender;
use common_exception::Result;
use common_exception::ErrorCodes;
use tokio::task::JoinHandle;
use std::net::SocketAddr;
use std::sync::Arc;
use common_infallible::Mutex;
use futures::future::AbortHandle;

pub struct RunningServer {
    listener_address: SocketAddr,
    abort_handle: AbortHandle,
    join_handler: Option<JoinHandle<()>>,
}

impl RunningServer {
    pub fn create(listener_address: SocketAddr, abort_handle: AbortHandle, join_handler: JoinHandle<()>) -> RunningServer {
        RunningServer {
            listener_address,
            abort_handle,
            join_handler: Some(join_handler),
        }
    }

    pub fn listener_address(&self) -> SocketAddr {
        self.listener_address.clone()
    }

    pub async fn shutdown(&self) {
        self.abort_handle.abort();
    }

    pub async fn wait_server_terminal(&mut self) -> Result<()> {
        let join_handler = self.join_handler.take();

        if join_handler.is_none() {
            // The server already shutdown
            return Ok(());
        }

        if let Err(error) = join_handler.unwrap().await {
            return Err(ErrorCodes::TokioError(format!(
                "Cannot shutdown, because cannot to join runnable server: {}", error
            )));
        }

        Ok(())
    }
}
