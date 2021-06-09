// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use tokio::sync::mpsc::Sender;
use common_exception::{Result, ToErrorCode};
use common_exception::ErrorCode;
use tokio::task::JoinHandle;
use std::net::SocketAddr;
use std::sync::Arc;
use common_infallible::Mutex;
use futures::future::AbortHandle;
use crate::servers::abortable::Abortable;

pub struct RunningServer {
    listener_address: SocketAddr,
    abort_handle: AbortHandle,
    join_handler: Mutex<Option<JoinHandle<()>>>,
    // join_handler: Option<JoinHandle<()>>,
}

impl RunningServer {
    pub fn create(
        listener_address: SocketAddr,
        abort_handle: AbortHandle,
        join_handler: JoinHandle<()>,
    ) -> RunningServer {
        RunningServer {
            listener_address,
            abort_handle,
            join_handler: Mutex::new(Some(join_handler)),
        }
    }

    pub fn listener_address(&self) -> SocketAddr {
        self.listener_address.clone()
    }
}

#[async_trait::async_trait]
impl Abortable for RunningServer {
    fn abort(&self) {
        self.abort_handle.abort();
        // TODO: abort session manager
    }

    async fn wait_server_terminal(&self) -> Result<()> {
        let join_handler = self.join_handler.lock();
        // match self.join_handler.take() {
        //     None => Ok(()),
        //     Some(handler) => {
        //         handler.await.map_err_to_code(
        //             ErrorCode::TokioError,
        //             || "Cannot shutdown server.",
        //         )
        //     },
        // }
    }
}

