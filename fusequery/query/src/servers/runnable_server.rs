// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use tokio::sync::mpsc::Sender;
use common_exception::Result;
use common_exception::ErrorCodes;
use tokio::task::JoinHandle;

pub struct RunnableServer {
    shutdown_sender: Sender<()>,
    join_handler: Option<JoinHandle<()>>,
}

impl RunnableServer {
    pub fn create(shutdown_sender: Sender<()>, join_handler: JoinHandle<()>) -> RunnableServer {
        RunnableServer {
            shutdown_sender,
            join_handler: Some(join_handler),
        }
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        let join_handler = self.join_handler.take();

        if join_handler.is_none() {
            // The server already shutdown
            return Ok(());
        }

        if let Err(error) = self.shutdown_sender.send(()).await {
            return Err(ErrorCodes::TokioError(format!(
                "Cannot shutdown, because cannot to send shutdown signal: {}", error
            )));
        }

        if let Err(error) = join_handler.unwrap().await {
            return Err(ErrorCodes::TokioError(format!(
                "Cannot shutdown, because cannot to join runnable server: {}", error
            )));
        }

        Ok(())
    }
}
