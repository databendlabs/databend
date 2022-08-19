use std::sync::Arc;
use common_exception::Result;

pub trait SingletonInstanceImpl<T>: Send + Sync {
    fn get(&self) -> T;

    fn init(&self, value: T) -> Result<()>;
}

pub type SingletonInstance<T> = Arc<dyn SingletonInstanceImpl<T>>;

