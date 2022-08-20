use std::sync::Arc;

use common_exception::Result;

pub trait SingletonImpl<T>: Send + Sync {
    fn get(&self) -> T;

    fn init(&self, value: T) -> Result<()>;
}

pub type Singleton<T> = Arc<dyn SingletonImpl<T>>;
