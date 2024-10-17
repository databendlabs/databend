// Copyright 2021 Datafuse Labs
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

use std::error::Error;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use futures::Future;
use log::error;
use log::info;
use tokio::sync::broadcast;

use super::Stoppable;
use crate::runtime::drop_guard;

/// Handle a group of `Stoppable` tasks.
/// When a user press ctrl-c, it calls the `stop()` method on every task to close them.
/// If a second ctrl-c is pressed, it sends a `()` through the `force` channel to notify tasks to shutdown at once.
///
/// Once `StopHandle` is dropped, it triggers a force stop on every tasks in it.
pub struct StopHandle<E: Error + Send + 'static> {
    stopping: Arc<AtomicBool>,
    pub(crate) stoppable_tasks: Vec<Box<dyn Stoppable<Error = E> + Send>>,
}

impl<E: Error + Send + 'static> StopHandle<E> {
    pub fn create() -> Self {
        StopHandle {
            stopping: Arc::new(AtomicBool::new(false)),
            stoppable_tasks: vec![],
        }
    }

    /// Call `Stoppable::stop` on every task, with an arg of **force shutdown signal receiver**.
    ///
    /// It blocks until all `Stoppable::stop()` return.
    pub fn stop_all(
        &mut self,
        force_tx: Option<broadcast::Sender<()>>,
    ) -> Result<impl Future<Output = ()> + Send + '_, ErrorCode> {
        if self
            .stopping
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return Err(ErrorCode::AlreadyStopped(
                "StopHandle is already shutting down",
            ));
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

    /// Impl a two phase shutting down procedure(graceful then force):
    ///
    /// - The first signal initiates a **graceful** shutdown by calling
    ///   `Stoppable::stop(Option<rx>)` on every managed tasks.
    ///
    /// - The second signal will be passed through `rx`, and it's the impl's duty to decide
    ///   whether to forcefully shutdown or just ignore the second signal.
    pub fn wait_to_terminate(
        mut self,
        signal: broadcast::Sender<()>,
    ) -> impl Future<Output = ()> + 'static {
        let mut rx = signal.subscribe();

        async move {
            // The first termination signal triggers graceful shutdown
            // Ignore the result
            let _ = rx.recv().await;

            info!("Received termination signal.");
            info!("Press Ctrl + C again to force shutdown.");

            // A second signal indicates a force shutdown.
            // It is the task's responsibility to decide whether to deal with it.
            let fut = self.stop_all(Some(signal));
            if let Ok(f) = fut {
                f.await;
            }
        }
    }

    /// Build a Sender `tx` for user to send **stop** signal to all tasks managed by this StopHandle.
    /// It also installs a `ctrl-c` monitor to let user send a signal to the `tx` by press `ctrl-c`.
    /// Thus there are two ways to stop tasks: `ctrl-c` or `tx.send()`.
    ///
    /// How to deal with the signal is not defined in this method.
    pub fn install_termination_handle() -> broadcast::Sender<()> {
        let (tx, _rx) = broadcast::channel(16);

        let t = tx.clone();
        ctrlc::set_handler(move || {
            if let Err(error) = t.send(()) {
                error!("Could not send signal on channel {}", error);
                std::process::exit(1);
            }
        })
        .expect("Error setting Ctrl-C handler");

        tx
    }

    pub fn push(&mut self, s: Box<dyn Stoppable<Error = E> + Send>) {
        self.stoppable_tasks.push(s);
    }
}

impl<E: Error + Send + 'static> Drop for StopHandle<E> {
    fn drop(&mut self) {
        drop_guard(move || {
            let (tx, _rx) = broadcast::channel::<()>(16);

            // let every task subscribe the channel, then send a force stop signal `()`
            let fut = self.stop_all(Some(tx.clone()));

            if let Ok(fut) = fut {
                let _ = tx.send(());
                futures::executor::block_on(fut);
            }
        })
    }
}
