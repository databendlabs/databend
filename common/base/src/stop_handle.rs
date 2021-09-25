// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_tracing::tracing;
use futures::Future;
use tokio::sync::broadcast;

use crate::Stoppable;

/// Handle a group of `Stoppable` tasks.
/// When a user press ctrl-c, it calls the `stop()` method on every task to close them.
/// If a second ctrl-c is pressed, it sends a `()` through the `force` channel to notify tasks to shutdown at once.
///
/// Once `StopHandle` is dropped, it triggers a force stop on every tasks in it.
pub struct StopHandle {
    stopping: Arc<AtomicBool>,
    pub(crate) stoppable_tasks: Vec<Box<dyn Stoppable + Send>>,
}

impl StopHandle {
    pub fn create() -> StopHandle {
        StopHandle {
            stopping: Arc::new(AtomicBool::new(false)),
            stoppable_tasks: vec![],
        }
    }

    pub fn stop_all(
        &mut self,
        force_tx: Option<broadcast::Sender<()>>,
    ) -> Result<impl Future<Output = ()> + Send + '_, ErrorCode> {
        if self
            .stopping
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return Err(ErrorCode::AlreadyStopped("StopHandle is shutting down"));
        }

        let mut handles = vec![];
        for s in &mut self.stoppable_tasks {
            let rx = force_tx.as_ref().map(|x| x.subscribe());
            handles.push(s.stop(rx));
        }

        let join_all = futures::future::join_all(handles);
        Ok(async move {
            let _ = join_all.await;
        })
    }

    pub fn wait_to_terminate(
        mut self,
        signal: broadcast::Sender<()>,
    ) -> impl Future<Output = ()> + 'static {
        let mut rx = signal.subscribe();

        async move {
            // The first termination signal triggers graceful shutdown
            // Ignore the result
            let _ = rx.recv().await;

            tracing::info!("Received termination signal.");
            tracing::info!("Press Ctrl + C again to force shutdown.");

            // A second signal indicates a force shutdown.
            // It is the task's responsibility to decide whether to deal with it.
            let fut = self.stop_all(Some(signal));
            if let Ok(f) = fut {
                f.await;
            }
        }
    }

    pub fn install_termination_handle() -> broadcast::Sender<()> {
        let (tx, _rx) = broadcast::channel(16);

        let t = tx.clone();
        ctrlc::set_handler(move || {
            if let Err(error) = t.send(()) {
                tracing::error!("Could not send signal on channel {}", error);
                std::process::exit(1);
            }
        })
        .expect("Error setting Ctrl-C handler");

        tx
    }

    pub fn push(&mut self, s: Box<dyn Stoppable + Send>) {
        self.stoppable_tasks.push(s);
    }
}

impl Drop for StopHandle {
    fn drop(&mut self) {
        let (tx, _rx) = broadcast::channel::<()>(16);

        // let every task subscribe the channel, then send a force stop signal `()`
        let fut = self.stop_all(Some(tx.clone()));

        if let Ok(fut) = fut {
            let _ = tx.send(());
            futures::executor::block_on(fut);
        }
    }
}
