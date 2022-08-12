use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use once_cell::sync::OnceCell;

use crate::base::Runtime;

pub struct GlobalIORuntime;

static GLOBAL_RUNTIME: OnceCell<Arc<Runtime>> = OnceCell::new();

impl GlobalIORuntime {
    pub fn init(num_cpus: usize) -> Result<()> {
        let thread_num = std::cmp::max(num_cpus, num_cpus::get() / 2);
        let thread_num = std::cmp::max(2, thread_num);

        let runtime = Arc::new(Runtime::with_worker_threads(
            thread_num,
            Some("IO-worker".to_owned()),
        )?);

        match GLOBAL_RUNTIME.set(runtime) {
            Ok(_) => Ok(()),
            Err(_) => Err(ErrorCode::LogicalError("Cannot init GlobalRuntime twice")),
        }
    }

    pub fn instance() -> Arc<Runtime> {
        match GLOBAL_RUNTIME.get() {
            None => panic!("GlobalRuntime is not init"),
            Some(global_runtime) => global_runtime.clone(),
        }
    }
}
