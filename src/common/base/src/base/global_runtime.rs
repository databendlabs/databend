use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use once_cell::sync::OnceCell;

use crate::base::Runtime;
use crate::base::singleton_instance::{SingletonInstance, SingletonInstanceImpl};

pub struct GlobalIORuntime;

static GLOBAL_RUNTIME: OnceCell<SingletonInstance<Arc<Runtime>>> = OnceCell::new();

impl GlobalIORuntime {
    pub fn init(num_cpus: usize, v: SingletonInstance<Arc<Runtime>>) -> Result<()> {
        let thread_num = std::cmp::max(num_cpus, num_cpus::get() / 2);
        let thread_num = std::cmp::max(2, thread_num);

        if let Some(thread_name) = std::thread::current().name() {
            println!("thread name {:?}", thread_name);
            v.init(Arc::new(Runtime::with_worker_threads(
                thread_num,
                Some(thread_name.to_string()),
            )?))?;
        } else {
            v.init(Arc::new(Runtime::with_worker_threads(
                thread_num,
                Some("IO-worker".to_owned()),
            )?))?;
        }

        GLOBAL_RUNTIME.set(v.clone()).ok();
        Ok(())
        // match GLOBAL_RUNTIME.set(v.clone()) {
        //     Ok(_) => Ok(()),
        //     Err(_) => Err(ErrorCode::LogicalError("Cannot init GlobalRuntime twice")),
        // }
    }

    pub fn instance() -> Arc<Runtime> {
        match GLOBAL_RUNTIME.get() {
            None => panic!("GlobalRuntime is not init"),
            Some(global_runtime) => global_runtime.get(),
        }
    }
}
