use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use futures::future::Either;
use tokio::sync::broadcast::error::SendError;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::Receiver as BroadcastReceiver;
use tokio::sync::broadcast::Sender as BroadcastSender;
use tokio::sync::mpsc::Sender as MPSCSender;
use tokio::sync::mpsc::Sender;

use crate::base::GlobalInstance;
use crate::runtime::GlobalIORuntime;

pub struct RuntimeTasks {}

impl RuntimeTasks {
    pub fn init() {
        GlobalInstance::set(Arc::new(RuntimeTasks {}));
    }

    pub fn instance() -> Arc<RuntimeTasks> {
        GlobalInstance::get()
    }

    pub async fn dump_tasks(&self, wait_for_running_tasks: bool) -> String {
        async_backtrace::taskdump_tree(wait_for_running_tasks)
    }
}

// pub async fn dumping_future<O, F: Future<Output=O>>(f: F) -> impl Future<Output=O> {
//     let rx = RuntimeTasks::instance().subscribe();
//     let finished = Arc::new(AtomicBool::new(false));
//     match async_backtrace::frame!(futures::future::select(Box::pin(f), recv(rx))).await {
//         Either::Left((res, _)) => res,
//         Either::Right(_) => unimplemented!(),
//     }
// }
//
// #[async_backtrace::framed]
// async fn recv(mut receiver: Receiver<(bool, MPSCSender<String>)>) {
//     // TODO: notify
//     while let Ok((wait_for_running_tasks, tx)) = receiver.recv().await {
//         let _ = tx.send(async_backtrace::taskdump_tree(wait_for_running_tasks)).await;
//         drop(tx);
//     }
// }
